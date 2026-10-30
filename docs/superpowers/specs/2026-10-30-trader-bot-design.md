# trader-bot 项目大纲（v0.1 设计稿）

## Context

从零开始的新项目（`/home/dukpay/codes/trader-bot` 目前为空）。用户想做一个 Go 写的加密货币合约系统，**最终目标是 AI 量化自动交易**，但 AI 的具体形态还没定。所以第一阶段先把「行情数据 → 指标 → 信号规则 → 通知」这条链路做扎实，同时把数据存好，给以后的回测和 AI 打基础。

已和用户确认的事实：
- 交易所：**只做币安**（U 本位永续 —— 币本位不做，这是我的判断）
- 监控范围：**手动维护的自选池**，几十个币种
- 信号类型：价格/成交量异动、技术指标、合约特有数据（资金费率/持仓量/多空比/爆仓）、**以上条件的与/或组合**
- 通知渠道：Telegram、Discord/通用 Webhook、邮件、企微/钉钉/飞书，全部都要
- 配置方式：**YAML 文件（热加载）+ Telegram 命令**，两种都要
- 用户：**只有用户本人**（单用户，不做账号、权限、订阅）
- 部署：**海外云服务器**，7x24 运行
- 技术选型：除 Go 外**交给我定**
- 用户最受不了的是：**信号刷屏**（宁可少而准）

## 目标 / 非目标 / 反目标

**目标（第一期）**
- 自选池内的币种稳定实时监控，断线能自动恢复，用户无需值守
- 规则可配置、可组合、改了不用重启
- 信号少而准，同一件事不重复轰炸
- 所有行情和信号落库，数据结构以后能直接拿去回测、训练、喂给 AI

**非目标（第一期不做）**
- 自动下单 / 持仓管理（只预留接口）
- 后台管理 Web 界面
- 多交易所、多用户、现货

**反目标（功能能跑也算失败的情况）**
- 信号刷屏、假信号多到用户不想看
- 指标和 TradingView/币安 对不上
- 断线后静默失效，用户不知道

## 路线图

| 阶段 | 内容 | 产出 |
|---|---|---|
| **P1 数据+监控+通知** | 币安 WS/REST 接入、K 线多周期聚合、指标计算、规则引擎、防刷屏、4 类通知、YAML 热加载、数据落库 | 能长期跑的信号机器人 |
| **P2 交互与运维** | Telegram 命令管理（增删币、暂停规则、查状态）、健康自检和断线告警、信号复盘统计（命中率） | 不登服务器也能管理 |
| **P3 回测与模拟盘** | 用落库数据回放规则；策略接口 `Strategy`；模拟盘（纸面交易）和风控模块 | 能评估信号质量 |
| **P4 AI 量化** | 在 Strategy 接口上接入 AI 决策（大模型/机器学习/AI 辅助规则，到时再定）；实盘下单 + 严格风控 | 最终目标 |
| **P5 后台管理** | Web 后台，复用 P2 的 API | 优先级最低 |

下面的细节只覆盖 **P1**，后面的阶段到时再单独细化。

## 系统架构（P1）

```
Binance WS/REST ─► Ingest(行情接入) ─► MarketBus ─► SymbolWorker × N (每个币一个 goroutine)
                                          │            ├─ 多周期 K 线序列
                                          │            ├─ 指标增量计算
                                          │            └─ 规则评估 ─► SignalBus
                                          ▼                              │
                                     Storage(落库)        Dedup/冷却/合并 ◄┘
                                                                  │
                                                     Notifier 分发 ─► Telegram / Webhook / Email / IM
ConfigManager(YAML 热加载 + TG 命令) ──► 下发规则和自选池变更给各模块
```

**并发模型（我的设计决定）**
- **每个币种一个 goroutine（Actor）**：它独占自己的 K 线和指标状态，不需要加锁；行情按 symbol 路由到对应的 channel。
- 模块之间用带缓冲的 channel 串联，`context` 统一控制退出，`errgroup` 管理生命周期，支持优雅停机。
- 通知用**独立的 worker 池**，按渠道限速、失败重试（指数退避），慢的渠道不会拖慢行情处理。
- 落库走**批量异步写入**，不阻塞主链路。
- 配置变更以「新快照」的形式推给 worker（原子替换），不在运行中改共享状态。

## 数据接入（币安 U 本位）

- **WebSocket**：`<symbol>@kline_1m`（本地合成 5m/15m/1h/4h 等，也可以直接订阅多周期）、`<symbol>@markPrice@1s`（标记价+资金费率）、`<symbol>@forceOrder`（爆仓）。组合流 + 每 24h 主动换连接（币安 24h 会断开）+ 断线自动重连并补数据。
- **REST 定时轮询**：持仓量 `openInterest` / `openInterestHist`、多空比 `globalLongShortAccountRatio` / `topLongShortPositionRatio`（5 分钟粒度）、`exchangeInfo`（检查币种上下架）。
- **启动预热**：每个币用 REST 拉取足够的历史 K 线（比如 500 根），让指标一启动就是准的。
- 遵守币安权重限流：REST 统一走一个带令牌桶的客户端。

## 指标与规则

**指标（P1）**：MA/EMA、MACD、RSI（Wilder 平滑，和 TradingView 对齐）、布林带、ATR、成交量均值/量比、N 周期涨跌幅、新高新低。按 K 线**增量计算**。

**规则 YAML 示例**（格式是我定的，可以改）：
```yaml
symbols: [BTCUSDT, ETHUSDT, SOLUSDT]
rules:
  - id: pump_with_volume
    symbols: all            # 或指定列表
    interval: 5m
    trigger: on_close       # on_close=K线收盘确认（默认，防假信号）| realtime
    when:
      all:
        - change_pct(1) > 3
        - volume_ratio(20) > 2
    cooldown: 30m
    channels: [telegram, discord]
    level: warn
  - id: rsi_oversold_funding_negative
    interval: 1h
    when:
      all:
        - rsi(14) < 30
        - any:
            - funding_rate < -0.0005
            - oi_change_pct(4) > 10
```
- 条件是一棵 `all/any/not` 树，叶子是「指标 比较符 阈值」，也支持 `cross_up/cross_down`（金叉死叉）。
- 规则引擎先用自己写的小型表达式解析器（叶子语法很简单），不引入通用脚本语言 —— 这样表达式能提前校验，出错时提示清楚。

## 防刷屏（重点，用户最在意）

1. **边沿触发**：条件从「不满足」变成「满足」才触发一次，持续满足不重复发。
2. **默认收盘确认**：K 线收盘后才评估，避免盘中来回抖动。
3. **冷却时间**：按「规则 + 币种」设冷却，冷却期内静默。
4. **合并推送**：同一时间窗口（比如 10 秒）内多条信号合并成一条消息。
5. **全局限速 + 免打扰时段**：每个渠道每分钟最多 N 条；可设置夜间只推高等级信号。
6. 所有被压下的信号也落库，方便复盘。

## 通知

- 统一的 `Notifier` 接口：`Send(ctx, Message) error`；每个渠道一个实现：Telegram Bot、Discord Webhook、通用 HTTP Webhook、SMTP 邮件、企业微信/钉钉（加签）/飞书机器人。
- 消息模板：币种、周期、规则名、触发值、当前价格、币安/TradingView 链接。
- 失败重试 3 次，还是失败就降级到备用渠道，并记日志。
- **系统自身异常**（WS 断线超过 1 分钟、数据停更）也走通知 —— 避免「静默失效」。

## 存储（我的选择和理由）

- **PostgreSQL + TimescaleDB**：K 线、资金费率、持仓量、多空比、爆仓都是时间序列，Timescale 有自动分区和压缩，以后回测和 AI 训练直接用 SQL 取数；配置和信号记录也放在同一个 PG 里，一个数据库就够了，运维简单。
- **暂时不用 Redis / 消息队列**：单进程、单用户、几十个币，进程内 channel 完全够用；以后真要拆服务再加 NATS。
- YAML 是配置的唯一来源；Telegram 命令修改后**写回 YAML 文件**（保证只有一个来源），然后触发热加载。

## 技术栈（我的选择）

| 用途 | 选型 |
|---|---|
| Go 版本 | Go 1.25+ |
| 币安 SDK | `github.com/adshao/go-binance/v2`（futures 包，成熟；必要时自己封装 WS） |
| 数据库 | `jackc/pgx/v5`，迁移用 `golang-migrate` |
| 配置 | `gopkg.in/yaml.v3` + `fsnotify` 热加载 |
| Telegram | `github.com/go-telegram/bot` |
| 日志 / 监控 | 标准库 `log/slog`；Prometheus 指标 + `/healthz` |
| 测试 | 标准库 `testing` + `testify`；指标用已知数据集做对照测试 |
| 部署 | Docker Compose（app + timescaledb），部署在海外 VPS（东京/新加坡） |

## 目录结构（建议）

```
trader-bot/
├── cmd/traderbot/main.go        # 入口，组装各模块
├── internal/
│   ├── config/                  # YAML 加载、校验、热更新
│   ├── exchange/binance/        # WS/REST 客户端、重连、限流
│   ├── market/                  # K线模型、多周期聚合、MarketBus
│   ├── indicator/               # 增量指标
│   ├── rule/                    # 表达式解析、条件树、评估
│   ├── engine/                  # SymbolWorker、调度
│   ├── signal/                  # 边沿触发、冷却、合并
│   ├── notify/                  # Notifier 接口及各渠道实现
│   ├── storage/                 # pgx 仓储、批量写入
│   ├── telegram/                # P2：命令处理
│   └── strategy/                # 预留：Strategy/Executor 接口（P3/P4）
├── migrations/
├── configs/config.example.yaml
├── deploy/docker-compose.yml
└── docs/superpowers/specs/      # 本设计文档的正式存放位置
```

## 留给实现者决定的
- 具体的批量写入大小和刷新间隔、channel 缓冲大小
- 多周期是本地合成还是直接订阅（先选本地合成，有问题再换）
- 消息模板的具体排版

## 第一步执行顺序（批准后）
1. 把这份设计保存到 `docs/superpowers/specs/2026-10-30-trader-bot-design.md`，执行 `git init`
2. 用 superpowers:writing-plans 把 P1 拆成可执行的任务计划（按 TDD 来）
3. 建议的实现顺序：config → binance 接入+预热 → K 线/指标（先和 TradingView 对数）→ 规则引擎 → 防刷屏 → 通知（先做 Telegram）→ 落库 → 其余渠道 → Docker 部署

## 验证方式
- 单元测试：指标用固定 K 线数据集和 TradingView 导出的值对比（误差 < 1e-6）；规则解析/评估；边沿触发与冷却的时间推进测试（注入假时钟）
- 集成测试：用录制的币安 WS 消息回放，检查端到端产生的信号和通知（通知用 mock）
- 故障演练：手动断网/kill WS，确认自动重连、补数据，并且收到系统告警
- 实跑：在 VPS 上用 3–5 个币、宽松规则跑 24h，检查 Telegram 收到的信号、是否有重复、延迟是否在秒级
- `go test -race ./...` 检查并发安全
