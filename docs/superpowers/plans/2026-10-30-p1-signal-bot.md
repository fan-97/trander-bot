# P1 信号机器人 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一个 7x24 运行的 Go 进程：订阅币安 U 本位永续自选池行情，按 YAML 规则计算多周期指标和合约数据信号，经过防刷屏处理后推送到多个通知渠道，并把行情和信号写入 TimescaleDB。

**Architecture:** 单进程。Binance 接入层把行情事件按 symbol 路由给每个币一个的 `SymbolWorker` goroutine（独占状态、无锁）；worker 在 K 线收盘时评估规则，把命中结果发到 `signal.Gate`（边沿触发+冷却）→ `signal.Batcher`（合并/限速/免打扰）→ `notify.Dispatcher`（worker 池、重试、降级）。配置以不可变快照经 `atomic.Pointer` 分发，`fsnotify` 热加载。

**Tech Stack:** Go 1.25+, `github.com/adshao/go-binance/v2/futures`, `github.com/jackc/pgx/v5`, `github.com/golang-migrate/migrate/v4`, `gopkg.in/yaml.v3`, `github.com/fsnotify/fsnotify`, `github.com/go-telegram/bot`, `golang.org/x/sync/errgroup`, `golang.org/x/time/rate`, `github.com/prometheus/client_golang`, `github.com/stretchr/testify`, `log/slog`。

**Spec:** `docs/superpowers/specs/2026-10-30-trader-bot-design.md`

## Global Constraints

- 只接币安 **U 本位永续**；单用户；P1 不下单、不做 Web 后台、不做 Telegram 命令（P2）。
- Go 模块名：`trader-bot`；所有业务代码在 `internal/` 下。
- 规则默认 `trigger: on_close`（K 线收盘确认）；`realtime` 为可选。
- 防刷屏六条全部实现：边沿触发、收盘确认、规则+币种冷却、10 秒窗口合并、渠道限速+免打扰、被压下的信号也落库。
- 通知失败重试 3 次（指数退避），仍失败则降级到配置的备用渠道。
- WS 断线超过 1 分钟或任一币种数据停更，发系统告警通知。
- 启动预热：每币每周期用 REST 拉 500 根历史 K 线。
- RSI 使用 Wilder 平滑；指标与 TradingView 对照误差 < 1e-6。
- REST 调用统一走令牌桶限流客户端。
- 所有 `time.Now()` 通过注入的 `clock.Clock` 获取，便于测试。
- 每个任务结束 `go test -race ./...` 必须通过。

## Review Focus

1. **配置文件改坏了（YAML 语法错/规则表达式错/未知渠道）** → 热加载拒绝新配置、继续用旧快照、发系统告警；不能崩溃也不能清空规则。（Task 2 `TestWatcher_InvalidKeepsOld`）
2. **指标预热不足或合约数据还没到（刚启动、新加币、资金费率首条推送前）** → 叶子条件求值为「未知」，整条规则不触发，绝不当成 0 去比较产生假信号。（Task 6 `TestEval_UnknownNeverFires`）
3. **WS 重连后 K 线缺口或重复** → 用 REST 补齐缺口、按 open time 去重，同一根收盘 K 线只评估一次。（Task 12 `TestWorker_DuplicateCloseEvaluatedOnce`、Task 11 `TestStream_GapFill`）
4. **自选池里有币安不存在/已下架的币** → 启动校验时剔除并告警，其他币正常运行。（Task 10 `TestValidateSymbols_DropsUnknown`）
5. **某个通知渠道持续不可用（Telegram 被墙/Webhook 500）** → 不阻塞其他渠道和行情处理，走降级渠道，队列满时丢弃最旧消息并计数。（Task 9 `TestDispatcher_SlowChannelDoesNotBlock`）

---

## File Structure

```
cmd/traderbot/main.go               # 组装、errgroup、优雅停机
internal/clock/clock.go             # Clock 接口 + Real + Fake
internal/config/{types,load,validate,watch}.go
internal/market/{types,aggregate}.go # Kline/Interval/事件；1m→高周期合成
internal/indicator/{sma,ema,rsi,macd,boll,atr,window}.go
internal/rule/{ast,parse,eval}.go   # 表达式解析、条件树、求值
internal/signal/{gate,batcher}.go   # 边沿+冷却；合并+限速+免打扰
internal/notify/{notify,dispatcher,telegram,discord,webhook,email,wecom,dingtalk,feishu}.go
internal/exchange/binance/{rest,stream}.go
internal/engine/{worker,router,metrics_ctx}.go
internal/storage/{store,batch}.go
internal/health/health.go           # 数据停更/断线监测、/healthz、/metrics
migrations/0001_init.{up,down}.sql
configs/config.example.yaml
deploy/{Dockerfile,docker-compose.yml}
```

---

### Task 0: 环境与骨架

本机当前**没有安装 Go 和 Docker**。

- [ ] **Step 1:** `brew install go` 并安装 Docker Desktop（或 OrbStack）；`go version` 输出 ≥ go1.25。
- [ ] **Step 2:** `go mod init trader-bot`；创建 `internal/clock/clock.go`：
  - `type Clock interface { Now() time.Time; After(d time.Duration) <-chan time.Time }`
  - `Real{}`；`Fake`：`NewFake(t time.Time) *Fake`、`Advance(d)`（触发到期的 After 通道）。
- [ ] **Step 3:** 测试 `TestFake_AdvanceFiresAfter`：`After(5s)` 在 `Advance(4s)` 后未触发、`Advance(1s)` 后触发。
- [ ] **Step 4:** `go test -race ./...` PASS；加 `.gitignore`（`/bin`, `config.yaml`, `.env`）；提交 `chore: scaffold module and clock`。

### Task 1: 配置类型、加载与校验

**Files:** Create `internal/config/{types,load,validate}.go`, `configs/config.example.yaml`; Test `internal/config/load_test.go`

**Interfaces:**
- Produces:
  - `type Config struct { Symbols []string; Intervals []market.Interval; Rules []Rule; Channels map[string]ChannelConfig; Notify NotifyConfig; Database DatabaseConfig; Binance BinanceConfig }`
  - `type Rule struct { ID string; Symbols []string /* ["all"] 表示全部 */; Interval market.Interval; Trigger Trigger /* "on_close"|"realtime" */; When rule.Node; Cooldown time.Duration; Channels []string; Level Level /* info|warn|critical */ }`
  - `type ChannelConfig struct { Type string /* telegram|discord|webhook|email|wecom|dingtalk|feishu */; Fallback string; Params map[string]string }`
  - `type NotifyConfig struct { MergeWindow time.Duration /* 默认 10s */; RatePerMinute int /* 默认 20 */; QuietHours *QuietHours; QuietMinLevel Level }`；`QuietHours{Start, End string /* "23:00" */; TZ string}`
  - `func Load(path string) (*Config, error)` — 解析+默认值+`Validate`；`func (c *Config) Validate() error` 返回汇总所有错误（`errors.Join`）。
  - `func (c *Config) RulesFor(symbol string) []Rule`
- Consumes: `market.Interval`（Task 3 定义；本任务先在 `internal/market/types.go` 建 `type Interval string` 和常量 `I1m,I5m,I15m,I1h,I4h,I1d` 及 `Duration()`）；`rule.Node` 与 `rule.Parse`（Task 5；本任务 `When` 先以 `yaml.Node` 读入，Task 5 接上解析，校验中调用）。

- [ ] **Step 1:** 写测试：
  - `TestLoad_Example`：加载 `configs/config.example.yaml`（含设计文档里的两条规则），`len(Rules)==2`，`Rules[0].Cooldown==30*time.Minute`，`Rules[1].Trigger=="on_close"`（默认值），`Notify.MergeWindow==10*time.Second`。
  - `TestValidate_CollectsErrors`：重复规则 ID、未知渠道名、未知 interval、symbol 不在自选池 —— 一次返回 4 条错误，`errors.Is` 不要求，用 `strings.Contains` 检查每条。
  - `TestRulesFor_All`：`symbols: [all]` 的规则对每个自选币都返回。
- [ ] **Step 2:** `go test ./internal/config/...` → FAIL（未定义）。
- [ ] **Step 3:** 实现。密钥类参数支持 `${ENV_VAR}` 展开（`os.ExpandEnv` 作用于 `Params` 值）。
- [ ] **Step 4:** PASS；提交 `feat(config): load and validate yaml config`。

### Task 2: 配置热加载

**Files:** Create `internal/config/watch.go`; Test `watch_test.go`

**Interfaces:**
- Produces: `type Store struct{...}`；`func NewStore(c *Config) *Store`；`func (s *Store) Current() *Config`（`atomic.Pointer`）；`func (s *Store) Subscribe() <-chan *Config`；`func Watch(ctx context.Context, path string, s *Store, onErr func(error)) error` — fsnotify 监听目录（应对编辑器原子替换），300ms 防抖后 `Load`，成功则替换并广播，失败调用 `onErr` 并保留旧快照。

- [ ] **Step 1:** 测试 `TestWatcher_ReloadsOnChange`（写新文件后 1s 内 `Subscribe` 收到 `len(Symbols)==3`）；`TestWatcher_InvalidKeepsOld`（写入坏 YAML，`onErr` 被调用一次，`Current()` 仍是旧指针）。
- [ ] **Step 2:** FAIL → **Step 3:** 实现 → **Step 4:** PASS（带 `-race`）→ **Step 5:** 提交 `feat(config): hot reload with fsnotify`。

### Task 3: 行情模型与多周期合成

**Files:** Create `internal/market/{types,aggregate}.go`; Test `aggregate_test.go`

**Interfaces:**
- Produces:
  - `type Kline struct { Symbol string; Interval Interval; OpenTime time.Time; Open, High, Low, Close, Volume, QuoteVolume float64; Closed bool }`
  - 事件：`type Event interface{ EventSymbol() string }`，实现：`KlineEvent{Kline}`、`MarkPriceEvent{Symbol; Time; MarkPrice, FundingRate float64; NextFunding time.Time}`、`LiquidationEvent{Symbol; Time; Side string; Price, Qty float64}`、`OpenInterestEvent{Symbol; Time; OI float64}`、`LongShortEvent{Symbol; Time; Kind string /* global|top */; Ratio float64}`
  - `type Aggregator struct{...}`；`func NewAggregator(symbol string, targets []Interval) *Aggregator`；`func (a *Aggregator) Add(k1m Kline) []Kline` — 输入 1m K 线（含未收盘更新），返回受影响的各目标周期 K 线（当前状态）；目标周期在其最后一根 1m 收盘时 `Closed=true`。周期按 UTC 对齐（`OpenTime.Truncate(d)`，1d 同样按 UTC 0 点，与币安一致）。

- [ ] **Step 1:** 测试：
  - `TestAggregator_5mFrom1m`：喂 5 根已收盘 1m（O=1,2,3,4,5 / H=最大 / V 各 1），第 5 根后返回的 5m `Closed==true, Open==1, Close==5, Volume==5`；前 4 根返回 `Closed==false`。
  - `TestAggregator_UnclosedUpdateReplaces`：同一根 1m 的两次未收盘更新不会重复累计 Volume。
  - `TestAggregator_GapStartsNewBar`：跳过若干分钟后新 1m 落在下一个 5m，旧 5m 以收盘状态返回一次。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。
- [ ] **Step 5:** 提交 `feat(market): kline model and interval aggregation`。

### Task 4: 增量指标

**Files:** Create `internal/indicator/*.go`; Test `indicator_test.go`, `testdata/btcusdt_1h.csv`（OHLCV 300 行）+ `testdata/tv_expected.csv`（TradingView 导出的 RSI14/EMA12/EMA26/MACD/Signal/BB20/ATR14）

**Interfaces:**
- Produces（全部操作已收盘 K 线；`Value()` 在预热不足时返回 `ok=false`）：
  - `type Indicator interface { Update(k market.Kline); Value() (float64, bool); Prev() (float64, bool) }`
  - 构造函数：`NewSMA(n)`, `NewEMA(n)`（种子 = 前 n 根 SMA，与 TV 一致）, `NewRSI(n)`（Wilder，种子 = 前 n 根平均涨跌）, `NewMACD(fast, slow, sig int, out MACDOut /* Line|Signal|Hist */)`, `NewBoll(n int, k float64, out BollOut /* Upper|Mid|Lower|Width */)`（总体标准差）, `NewATR(n)`（Wilder）, `NewVolumeRatio(n)`（当前量 / 前 n 根均量，不含当前）, `NewChangePct(n)`（`(C_t / C_{t-n} - 1)*100`）, `NewHighest(n)`, `NewLowest(n)`（均不含当前根，用于突破判断）
  - `func New(name string, args []float64) (Indicator, error)` — 名字注册表：`sma, ema, rsi, macd, macd_signal, macd_hist, boll_upper, boll_mid, boll_lower, atr, volume_ratio, change_pct, highest, lowest, close, volume`
  - 辅助：`window.go` 里的定长环形缓冲 `Ring`。

- [ ] **Step 1:** 测试 `TestIndicators_MatchTradingView`：表驱动，逐行喂 CSV，在 TV 有值的每一行断言 `math.Abs(got-want) < 1e-6`；`TestRSI_NotReadyBeforeN`：喂 14 根前 `ok==false`；`TestNew_UnknownName` 返回错误。
- [ ] **Step 2:** 获取测试数据：用币安 REST `GET /fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=300` 导出 CSV；在 TradingView 同一时段导出图表数据（含上述指标）得到 `tv_expected.csv`。**这一步需要用户手动在 TradingView 导出**，执行者应停下来请求文件。
- [ ] **Step 3–4:** FAIL → 实现 → PASS。
- [ ] **Step 5:** 提交 `feat(indicator): incremental indicators matched to TradingView`。

### Task 5: 规则表达式解析

**Files:** Create `internal/rule/{ast,parse}.go`; Test `parse_test.go`

**Interfaces:**
- Produces:
  - `type Node interface{ isNode() }`；`All{Children []Node}`, `Any{Children []Node}`, `Not{Child Node}`, `Leaf{Left Operand; Op Op; Right Operand}`
  - `type Operand struct { Func string; Args []float64; Const *float64 }`（`Func` 为空表示常量）
  - `type Op string`：`> >= < <= == cross_up cross_down`
  - `func Parse(n *yaml.Node) (Node, error)` — 映射键 `all/any/not`；标量字符串为叶子。
  - `func ParseLeaf(s string) (Leaf, error)`；叶子语法：`<operand> <cmp> <operand>` 或 `cross_up(<operand>, <operand>)`；operand = `name` | `name(num,...)` | 数字。
  - 合约数据名（非 K 线指标，由 worker 提供）：`funding_rate`, `mark_price`, `oi`, `oi_change_pct(n)`（n 根 5m OI 采样）, `long_short_ratio`, `top_long_short_ratio`, `liq_volume(minutes)`（窗口内爆仓名义价值 USDT）。
  - `func Funcs(n Node) []Operand` — 列出规则用到的所有函数（worker 据此创建指标）。
  - 校验：未知函数名、参数个数不对，错误信息包含原始表达式和位置。

- [ ] **Step 1:** 测试：`TestParseLeaf`（表驱动：`"rsi(14) < 30"`、`"change_pct(1) > 3"`、`"funding_rate < -0.0005"`、`"cross_up(ema(12), ema(26))"`、`"close > highest(20)"`）；`TestParseLeaf_Errors`（`"rsi(14) <"`、`"foo(1) > 2"`、`"rsi(1,2) > 3"` 均报错且 msg 含原串）；`TestParse_Tree`（设计文档第二条规则解析成 `All{Leaf, Any{Leaf, Leaf}}`）。
- [ ] **Step 2–4:** FAIL → 手写递归下降解析（不引入表达式库）→ PASS。
- [ ] **Step 5:** 在 `config.Validate` 中接入 `rule.Parse`（`Rule.When` 改为 `rule.Node`），补 `TestValidate_BadExpression`。提交 `feat(rule): expression parser and condition tree`。

### Task 6: 规则求值（三值逻辑）

**Files:** Create `internal/rule/eval.go`; Test `eval_test.go`

**Interfaces:**
- Produces:
  - `type Values interface { Get(o Operand) (cur, prev float64, ok bool) }`
  - `type Result int`：`False, True, Unknown`
  - `func Eval(n Node, v Values) Result` — 叶子任一操作数 `ok=false` → `Unknown`；`All`：有 False 则 False，否则有 Unknown 则 Unknown；`Any`：有 True 则 True，否则有 Unknown 则 Unknown；`Not(Unknown)=Unknown`。`cross_up`：`prevA <= prevB && curA > curB`。
  - `func Describe(n Node, v Values) string` — 触发时的数值说明，如 `rsi(14)=27.31 < 30; funding_rate=-0.0007 < -0.0005`（用于消息模板）。

- [ ] **Step 1:** 测试：`TestEval_Comparisons`、`TestEval_CrossUp`（prev 3<4，cur 5>4 → True；prev 已在上方 → False）、`TestEval_UnknownNeverFires`（`All{rsi<30(unknown), close>0(true)}` → Unknown；`Any{unknown, false}` → Unknown）、`TestDescribe`。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(rule): three-valued evaluation`。

### Task 7: 信号闸门（边沿触发 + 冷却）

**Files:** Create `internal/signal/gate.go`; Test `gate_test.go`

**Interfaces:**
- Produces:
  - `type Signal struct { RuleID, Symbol string; Interval market.Interval; Level config.Level; Time time.Time; Price float64; Detail string; Channels []string; Suppressed string /* ""|"cooldown"|"quiet"|"rate" */ }`
  - `type Gate struct{...}`；`func NewGate(clk clock.Clock) *Gate`
  - `func (g *Gate) Observe(ruleID, symbol string, r rule.Result, cooldown time.Duration) (fire bool, suppressed string)` — 键 `ruleID|symbol`；只有上一次结果不是 True 且本次 True 才算「边沿」；边沿在冷却内 → `(false, "cooldown")`；`Unknown` 不改变上一次状态。
  - `func (g *Gate) Forget(ruleID string)` — 配置删除规则时清理。
- [ ] **Step 1:** 测试 `TestGate_EdgeOnly`（T,T,T → 只第一次 fire）、`TestGate_RearmAfterFalse`（T,F,T 且超出冷却 → 两次 fire）、`TestGate_Cooldown`（T,F,T 在 30m 冷却内 → 第二次 `suppressed=="cooldown"`；`fake.Advance(30m)` 后 F,T → fire）、`TestGate_UnknownKeepsState`（T,Unknown,T → 只一次）。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(signal): edge trigger and cooldown gate`。

### Task 8: 合并、限速、免打扰

**Files:** Create `internal/signal/batcher.go`; Test `batcher_test.go`

**Interfaces:**
- Produces:
  - `type Batch struct { Channel string; Signals []Signal }`
  - `type Batcher struct{...}`；`func NewBatcher(clk clock.Clock, cfg func() config.NotifyConfig, out chan<- Batch, record func(Signal)) *Batcher`
  - `func (b *Batcher) Run(ctx context.Context, in <-chan Signal) error` — 按渠道分组；某渠道收到第一条后开 `MergeWindow` 计时，到期一次性输出该渠道全部信号；每渠道令牌桶 `RatePerMinute`（超出的标 `Suppressed="rate"`）；免打扰时段内低于 `QuietMinLevel` 的标 `"quiet"`；`critical` 永远不受免打扰和限速影响。所有信号（包括被压下的）都调用 `record`。
- [ ] **Step 1:** 测试（Fake 时钟）：`TestBatcher_MergesWithinWindow`（3 条信号在 10s 内 → 1 个 Batch，3 条）、`TestBatcher_PerChannel`、`TestBatcher_RateLimit`（RatePerMinute=2，发 5 条不同窗口 → 2 条送出、3 条 `record` 为 rate）、`TestBatcher_QuietHours`（Asia/Shanghai 23:00–07:00，warn 被压、critical 送出）。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(signal): merge window, rate limit, quiet hours`。

### Task 9: 通知接口、分发器与 Telegram

**Files:** Create `internal/notify/{notify,dispatcher,telegram}.go`; Test `dispatcher_test.go`, `telegram_test.go`

**Interfaces:**
- Produces:
  - `type Message struct { Title string; Lines []string; Level config.Level; Links []Link }`；`type Link struct{ Text, URL string }`
  - `type Notifier interface { Name() string; Send(ctx context.Context, m Message) error }`
  - `func Format(b signal.Batch) Message` — 每条信号一行：`[5m] BTCUSDT pump_with_volume @ 67321.5 | change_pct(1)=3.4 > 3; ...`；Links 为币安合约页 `https://www.binance.com/zh-CN/futures/<SYMBOL>` 与 TradingView `https://www.tradingview.com/chart/?symbol=BINANCE:<SYMBOL>.P`。
  - `func NewFromConfig(name string, c config.ChannelConfig, hc *http.Client) (Notifier, error)` — 工厂（其余渠道在 Task 10 注册）。
  - `type Dispatcher struct{...}`；`func NewDispatcher(ns map[string]Notifier, fallback map[string]string, queueSize int /* 默认 100 */, clk clock.Clock) *Dispatcher`；`func (d *Dispatcher) Run(ctx context.Context, in <-chan signal.Batch) error`；`func (d *Dispatcher) SendSystem(m Message)` — 系统告警，发往全部渠道。
  - 每渠道一个 goroutine + 有界队列；满时丢最旧并 `dropped_total{channel}` 计数；重试 3 次（1s/2s/4s，Fake 时钟可测）；失败后投递到 `fallback[name]`（仅一层，不级联）。
  - Telegram：`Params{token, chat_id}`，HTTP 直调 `https://api.telegram.org/bot<token>/sendMessage`，`parse_mode=HTML`，内容 HTML 转义；`base_url` 参数可覆盖（测试用 httptest）。P1 不引入 go-telegram/bot（P2 做命令时再引入）。
- [ ] **Step 1:** 测试：`TestDispatcher_RetryThenSuccess`（前 2 次 error、第 3 次成功 → 共调用 3 次）、`TestDispatcher_FallbackAfter3Failures`、`TestDispatcher_SlowChannelDoesNotBlock`（A 渠道 Send 阻塞，B 渠道 100ms 内收到）、`TestTelegram_SendsHTML`（httptest 校验 path、chat_id、`&lt;` 转义）、`TestFormat`。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(notify): dispatcher with retry/fallback and telegram`。

### Task 10: 其余通知渠道

**Files:** Create `internal/notify/{discord,webhook,email,wecom,dingtalk,feishu}.go`; Test `channels_test.go`

**Interfaces:**
- Consumes: `Notifier`, `Message`, `NewFromConfig`（在工厂中注册）。
- Params：discord `{url}`（`{"content": ...}`，≤2000 字符截断）；webhook `{url}`（POST JSON：`{"title","level","lines","links"}`）；email `{host, port, username, password, from, to}`（`net/smtp` + STARTTLS，纯文本）；wecom `{url}`（markdown 消息）；dingtalk `{url, secret}`（`timestamp`+`sign` = Base64(HmacSHA256(`timestamp+"\n"+secret`)) 并 URL 编码）；feishu `{url, secret?}`（text 消息；有 secret 时 `sign` = Base64(HmacSHA256(key=`timestamp+"\n"+secret`, msg=空))）。
- [ ] **Step 1:** 测试：每个 HTTP 渠道一个 httptest 用例校验请求体；`TestDingtalkSign` 和 `TestFeishuSign` 用固定 timestamp 与官方文档示例比对；`TestEmail_BuildsMessage` 只测报文构造（不真连 SMTP）；`TestNewFromConfig_UnknownType`。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(notify): discord, webhook, email, wecom, dingtalk, feishu`。

### Task 11: 币安 REST 与 WebSocket 接入

**Files:** Create `internal/exchange/binance/{rest,stream}.go`; Test `rest_test.go`, `stream_test.go`, `testdata/ws_*.json`（录制的真实消息样本）

**Interfaces:**
- Produces:
  - `type REST struct{...}`；`func NewREST(baseURL string, weightPerMin int /* 默认 1200，留余量 */) *REST`
  - `func (r *REST) Symbols(ctx) (map[string]bool, error)` — `exchangeInfo` 中 `contractType==PERPETUAL && quoteAsset==USDT && status==TRADING`
  - `func ValidateSymbols(want []string, have map[string]bool) (ok []string, dropped []string)`
  - `func (r *REST) Klines(ctx, symbol string, iv market.Interval, start time.Time, limit int) ([]market.Kline, error)`
  - `func (r *REST) OpenInterest(ctx, symbol) (market.OpenInterestEvent, error)`；`func (r *REST) LongShort(ctx, symbol, kind string) (market.LongShortEvent, error)`（`period=5m, limit=1`）
  - `func PollContract(ctx, r *REST, symbols func() []string, every time.Duration /* 5m */, out chan<- market.Event, clk clock.Clock) error`
  - `type Stream struct{...}`；`func NewStream(wsURL string, rest *REST, clk clock.Clock, onState func(connected bool)) *Stream`
  - `func (s *Stream) Run(ctx, symbols func() []string, changed <-chan struct{}, out chan<- market.Event) error` — 组合流 `<s>@kline_1m`、`<s>@markPrice@1s`、`<s>@forceOrder`；断线指数退避重连（1s→60s 封顶）；连接满 23h 主动重建；自选池变化时重建订阅；重连后对每个币用 REST 补齐从最后已收盘 1m 到现在的缺口（按 OpenTime 去重后再发出）。
  - 使用 `adshao/go-binance/v2/futures` 处理 REST 与 WS 消息结构；若其 WS 封装不能满足重连/轮换控制，则直接用 `github.com/coder/websocket` 连接 `wss://fstream.binance.com/stream?streams=...` 并复用其事件结构体解析。（执行者二选一，记录在提交信息里。）
- [ ] **Step 1:** 测试：`TestValidateSymbols_DropsUnknown`；`TestParseKlineMsg` / `TestParseMarkPriceMsg` / `TestParseForceOrderMsg`（录制样本 → 事件字段）；`TestStream_ReconnectsAfterDrop`（本地 httptest WS 服务器主动断开，`onState(false)` 后再 `onState(true)`）；`TestStream_GapFill`（断开期间缺 3 根 1m，REST mock 返回 3 根，out 中按序出现且无重复）；`TestREST_RateLimited`（权重耗尽时调用阻塞而不是报错）。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。
- [ ] **Step 5:** 手动冒烟（需在可访问币安的网络）：`go run ./internal/exchange/binance/cmd/smoke BTCUSDT` 打印 30 秒事件后退出。提交 `feat(binance): rest client and resilient websocket stream`。

### Task 12: SymbolWorker 与路由

**Files:** Create `internal/engine/{worker,router,metrics_ctx}.go`; Test `worker_test.go`, `router_test.go`

**Interfaces:**
- Consumes: `market.Event/Aggregator`、`indicator.New`、`rule.Funcs/Eval/Describe`、`signal.Gate/Signal`、`config.Store`。
- Produces:
  - `type Worker struct{...}`；`func NewWorker(symbol string, cfg *config.Config, gate *signal.Gate, out chan<- signal.Signal, clk clock.Clock) *Worker`
  - `func (w *Worker) Warmup(history map[market.Interval][]market.Kline)`
  - `func (w *Worker) Run(ctx context.Context, in <-chan market.Event, cfgs <-chan *config.Config) error` — 单 goroutine 独占状态：1m K 线 → Aggregator → 对应周期指标 `Update`（仅收盘）→ 评估 `trigger==on_close` 的规则；`realtime` 规则在每次未收盘更新时用「已收盘指标 + 当前价」评估（P1 仅 `close/change_pct/volume` 支持 realtime，其余在 Validate 拒绝）；合约事件更新内部值后评估引用它们的规则。收到新配置时按 `rule.Funcs` 重建指标（用保留的最近 500 根 K 线重放），删除的规则调用 `gate.Forget`。
  - `metrics_ctx.go`：`type values struct{...}` 实现 `rule.Values`，把 Operand 映射到对应周期的 Indicator 或合约数据；`oi_change_pct(n)` 用 OI 采样环形缓冲；`liq_volume(m)` 用时间窗口累加。
  - `type Router struct{...}`；`func NewRouter(...) *Router`；`func (r *Router) Run(ctx, in <-chan market.Event) error` — 按 symbol 分发到 worker 的 channel（缓冲 1024）；自选池变化时启动/停止 worker；某 worker 的 channel 满时记录 `worker_backpressure_total` 并阻塞等待（不丢行情）。
- [ ] **Step 1:** 测试：`TestWorker_FiresOnClose`（预热 30 根平稳 5m，喂一根涨 4% 且放量的收盘 K 线 → out 收到 `pump_with_volume`，Detail 含 `change_pct(1)=`）；`TestWorker_NoFireOnUnclosed`；`TestWorker_DuplicateCloseEvaluatedOnce`（同一根收盘 K 线送两次 → 只评估/触发一次）；`TestWorker_ConfigReloadAddsRule`；`TestWorker_FundingRule`（markPrice 事件 funding=-0.0007 + rsi 条件满足 → 触发）；`TestRouter_StartsAndStopsWorkers`。
- [ ] **Step 2–4:** FAIL → 实现 → PASS（`-race`）。**Step 5:** 提交 `feat(engine): per-symbol workers and router`。

### Task 13: 存储与批量写入

**Files:** Create `migrations/0001_init.{up,down}.sql`, `internal/storage/{store,batch}.go`; Test `store_test.go`（`//go:build integration`，依赖 `TEST_DATABASE_URL`）

**Schema（up）：**
- `CREATE EXTENSION IF NOT EXISTS timescaledb;`
- `klines(symbol text, interval text, open_time timestamptz, open, high, low, close, volume, quote_volume double precision, PRIMARY KEY(symbol, interval, open_time))` → hypertable on `open_time`，7 天后压缩。只存**已收盘**的 1m 及配置的周期。
- `mark_prices(symbol, time, mark_price, funding_rate, PK(symbol,time))`，每币每分钟采样一条（不存每秒）。
- `open_interest(symbol, time, oi)`、`long_short(symbol, time, kind, ratio)`、`liquidations(symbol, time, side, price, qty)` —— 均为 hypertable。
- `signals(id bigserial, time, rule_id, symbol, interval, level, price, detail, channels text[], suppressed text)`。

**Interfaces:**
- Produces: `func Open(ctx, dsn string) (*Store, error)`（启动时自动跑 migrate）；`type Writer struct{...}`；`func NewWriter(s *Store, batchSize int /* 500 */, flushEvery time.Duration /* 2s */) *Writer`；`func (w *Writer) Run(ctx, in <-chan any) error` — 按类型分组用 `pgx.CopyFrom`（klines 用 `INSERT ... ON CONFLICT DO UPDATE` 批量）写入；DB 不可用时缓冲最多 10000 条后丢最旧并告警，不阻塞上游；`ctx` 结束时 flush。
- [ ] **Step 1:** 集成测试：`TestWriter_FlushOnSize`、`TestWriter_FlushOnInterval`、`TestWriter_KlineUpsert`（同 PK 写两次只一行，值为后者）、`TestWriter_SignalsIncludingSuppressed`。
- [ ] **Step 2:** `docker run -d -p 5433:5432 -e POSTGRES_PASSWORD=pg timescale/timescaledb:latest-pg16`；`TEST_DATABASE_URL=postgres://postgres:pg@localhost:5433/postgres go test -tags integration ./internal/storage/...` → FAIL。
- [ ] **Step 3–4:** 实现 → PASS。**Step 5:** 提交 `feat(storage): timescaledb schema and batch writer`。

### Task 14: 健康监测与系统告警

**Files:** Create `internal/health/health.go`; Test `health_test.go`

**Interfaces:**
- Produces: `type Monitor struct{...}`；`func NewMonitor(clk clock.Clock, alert func(notify.Message), staleAfter time.Duration /* 1m */) *Monitor`；`func (m *Monitor) OnEvent(symbol string)`；`func (m *Monitor) OnConn(connected bool)`；`func (m *Monitor) Run(ctx) error`（每 10s 检查）；`func (m *Monitor) Handler() http.Handler` — `/healthz`（断线或任一币停更 → 503 + JSON 详情）、`/metrics`（Prometheus）。
- 告警规则：断线持续 ≥1m 发一次 critical「WS 断开」，恢复时发一次 info「已恢复」；某币超过 1m 无事件发一次 critical，恢复发 info —— 每种状态变化只发一次（同样不刷屏）。
- [ ] **Step 1:** 测试（Fake 时钟）：`TestMonitor_DisconnectAlertOnceAfter1m`、`TestMonitor_RecoveryAlert`、`TestMonitor_StaleSymbol`、`TestHealthz_503WhenStale`。
- [ ] **Step 2–4:** FAIL → 实现 → PASS。**Step 5:** 提交 `feat(health): stale/disconnect alerts and healthz`。

### Task 15: 组装、部署与端到端

**Files:** Create `cmd/traderbot/main.go`, `deploy/Dockerfile`, `deploy/docker-compose.yml`, `README.md`（覆盖现有内容）; Test `cmd/traderbot/e2e_test.go`

- [ ] **Step 1:** `main.go`：flags `-config configs/config.yaml -http :8080`；流程：`config.Load` → `storage.Open` → `REST.Symbols` + `ValidateSymbols`（dropped 发系统告警）→ 每币每周期 `Klines(..., 500)` 预热 → 用 `errgroup` 启动 `config.Watch`、`Stream.Run`、`PollContract`、`Router.Run`、`Batcher.Run`、`Dispatcher.Run`、`Writer.Run`、`Monitor.Run`、HTTP server；行情事件 fan-out 到 Router / Writer / Monitor；`SIGINT/SIGTERM` 取消 ctx，10s 内优雅退出。
- [ ] **Step 2:** e2e 测试 `TestE2E_ReplayProducesOneNotification`：httptest 假币安（REST + WS 回放 `testdata/ws_*.json` 加一段构造的拉升行情）、假 Telegram 服务器、Storage 用接口 mock；断言 Telegram 恰好收到 1 条消息且包含 `pump_with_volume`，连续拉升不产生第二条。
- [ ] **Step 3:** FAIL → 实现 → PASS。
- [ ] **Step 4:** Dockerfile（多阶段，`golang:1.25` 构建、`gcr.io/distroless/static` 运行）；compose：`timescaledb` + `traderbot`（挂载 `configs/config.yaml`，`.env` 提供密钥，`restart: unless-stopped`，healthcheck 调 `/healthz`）。`docker compose -f deploy/docker-compose.yml up -d` 后 `curl localhost:8080/healthz` 返回 200。
- [ ] **Step 5:** README：配置说明、规则语法、可用函数表、部署步骤。提交 `feat: wire up main, docker deploy, e2e test`。
- [ ] **Step 6（用户参与）:** 在海外 VPS 用 3–5 个币、宽松规则跑 24h：检查无重复信号、信号延迟（K 线收盘到 Telegram 收到）< 5s、手动 `docker network disconnect` 1 分钟后收到断线告警和恢复通知。

---

## Spec 覆盖对照

| 设计文档要求 | 任务 |
|---|---|
| 币安 WS/REST、24h 轮换、重连补数、限流、预热 | 11, 15 |
| 多周期 K 线 | 3 |
| 指标（MA/EMA/MACD/RSI/BOLL/ATR/量比/涨跌幅/新高新低） | 4 |
| 合约数据（资金费率/OI/多空比/爆仓） | 3, 11, 12 |
| 与/或/非组合、金叉死叉 | 5, 6 |
| YAML 热加载 | 1, 2 |
| 防刷屏六条 | 7, 8, 13 |
| 7 种通知渠道、重试、降级 | 9, 10 |
| 系统异常告警 | 14 |
| TimescaleDB 落库 | 13 |
| Docker Compose 部署 | 15 |
| Telegram 命令 | **P2，不在本计划** |
| Strategy 接口预留 | **P3，不在本计划**（设计中的 `internal/strategy/` 目录推迟到 P3 创建） |
