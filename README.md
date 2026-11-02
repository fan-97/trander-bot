# trader-bot

币安 U 本位永续合约的**信号机器人**（P1）：实时接收行情，按 YAML 里写好的规则计算指标、判断条件，
满足时推送到 Telegram / Discord / 企业微信 / 钉钉 / 飞书 / 邮件 / 通用 Webhook，并把行情和信号落库到 TimescaleDB。

P1 **不下单**、没有 Web 后台、没有 Telegram 命令（这些在 P2 及以后）。

## 它做什么

- 通过 WebSocket 订阅每个币的 `kline_1m`、`markPrice@1s`、`forceOrder`（爆仓），每 5 分钟用 REST 轮询持仓量和多空比。
- 1 分钟 K 线在进程内聚合成 5m / 15m / 1h / 4h / 1d，增量计算指标（RSI 用 Wilder 平滑）。
- 启动时每个币每个周期用 REST 拉 500 根历史 K 线预热，另外再拉够最大周期当前这一根所需的 1m K 线（最多 1500 根），
  所以启动后第一根高周期 K 线的成交量也是完整的。
- 断线自动重连（指数退避），重连后用 REST 补齐缺失的 1m K 线；每条连接 23 小时主动轮换（币安 24 小时会断开）。
- 规则是 `all / any / not` 条件树，默认 K 线收盘确认后才评估；防刷屏六条全部实现（见下文）。
- 通知失败重试 3 次（1s、2s 退避），仍失败则转发到该渠道配置的备用渠道。
- 系统异常也会推送：WS 断开超过 1 分钟、某个币数据停更 1 分钟、启动 2 分钟仍未连上 WS、配置热加载失败、落库丢数据、
  配置里有不存在的币种、某个币预热失败、启动失败或运行中组件出错导致退出（退出前最多等 5 秒把告警发出去）。
- 配置文件修改后自动热加载（无需重启），非法配置会被拒绝并告警，继续使用旧配置。
- `/healthz` 和 `/metrics`（Prometheus）。

## 配置

复制示例并修改：

```sh
cp configs/config.example.yaml configs/config.yaml
```

配置里可以用 `${VAR}` 引用环境变量（只在 `channels.*.params` 和 `database.dsn` 里展开）。
**注意：未设置的变量会被展开成空字符串，不会报错**，请确认 `.env` 里都填了。

### 所有配置项

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `symbols` | 是 | – | 监控的币种池，如 `[BTCUSDT, ETHUSDT]`。启动时会和交易所的 U 本位永续列表核对，不存在或不在交易的币会被忽略并发一条系统告警。 |
| `intervals` | 否 | 规则用到的周期 | 需要聚合/预热的周期：`1m` `5m` `15m` `1h` `4h` `1d`。写了的话，每条规则的 `interval` 必须在列表里。 |
| `rules[].id` | 是 | – | 规则名，唯一，会出现在通知里。 |
| `rules[].symbols` | 否 | `all` | `all`（整个币种池）或币种列表（必须在 `symbols` 里）。 |
| `rules[].interval` | 是 | – | 规则在哪个周期上评估。 |
| `rules[].trigger` | 否 | `on_close` | `on_close`：K 线收盘后评估；`realtime`：每次 1m 推送都用未收盘的 K 线评估（见下面的限制）。 |
| `rules[].when` | 是 | – | 条件树，见“规则语法”。 |
| `rules[].cooldown` | 否 | max(周期, 15m) | 同一规则 + 同一币种两次推送的最小间隔，如 `30m`、`1h`；`0s` 表示不冷却。 |
| `rules[].channels` | 是 | – | 推送到哪些渠道（`channels` 里的名字）。 |
| `rules[].level` | 否 | `warn` | `info` / `warn` / `critical`。`critical` 不受限速和免打扰限制。 |
| `channels.<名字>.type` | 是 | – | `telegram` `discord` `webhook` `email` `wecom` `dingtalk` `feishu`。 |
| `channels.<名字>.fallback` | 否 | – | 重试 3 次仍失败时转发到的备用渠道名（只转发一次；系统告警不转发）。 |
| `channels.<名字>.params` | 视类型 | – | 见“通知渠道参数”。 |
| `notify.merge_window` | 否 | `10s` | 合并窗口：同一渠道在窗口内的信号合并成一条消息。 |
| `notify.rate_per_minute` | 否 | `20` | 每个渠道每分钟最多推送多少条**信号**（令牌桶）。 |
| `notify.quiet_hours.start` / `end` | 否 | – | 免打扰时段，`"23:00"` 格式，可跨午夜。 |
| `notify.quiet_hours.tz` | quiet_hours 时必填 | – | 时区，如 `Asia/Shanghai`。 |
| `notify.quiet_min_level` | 否 | `critical` | 免打扰时段内只推送这个等级及以上的信号。 |
| `database.dsn` | 是（生产） | – | PostgreSQL 连接串，通常写 `${DATABASE_URL}`。 |
| `binance.rest_url` | 否 | `https://fapi.binance.com` | |
| `binance.ws_url` | 否 | `wss://fstream.binance.com/market` | 必须带 `/market` 路径：币安在根路径上能连上但不推数据。 |

### 规则语法

`when` 是一棵条件树。节点可以是：

- `all: [条件, ...]`：全部满足
- `any: [条件, ...]`：任一满足
- `not: 条件`：取反（一个条件，可以是字符串或另一个 `all/any/not`）
- 叶子字符串：`<操作数> <比较符> <操作数>`，比较符为 `>` `>=` `<` `<=` `==`；
  或者交叉：`cross_up(a, b)`（a 上穿 b，金叉）、`cross_down(a, b)`（a 下穿 b，死叉）。

操作数是函数调用或数字常量（可为负数、小数）。例子：

```yaml
rules:
  - id: pump_with_volume
    symbols: all
    interval: 5m
    trigger: on_close
    when:
      all:
        - change_pct(1) > 3          # 1 根内涨 3% 以上
        - volume_ratio(20) > 2       # 成交量是前 20 根均量的 2 倍以上
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
        - not: close < boll_lower(20, 2)
    cooldown: 1h
    channels: [telegram]

  - id: golden_cross
    interval: 4h
    when: cross_up(ema(12), ema(26))
    channels: [telegram]
```

求值是三值逻辑：数据不足（例如预热不够、合约数据还没到）时叶子为“未知”，未知不会触发，也不会重置边沿状态。

### 可用函数

K 线指标（在规则的 `interval` 上按收盘 K 线计算）：

| 函数 | 参数个数 | 含义 |
|---|---|---|
| `close` | 0 | 收盘价 |
| `volume` | 0 | 成交量 |
| `sma(n)` | 1 | 简单移动平均（MA） |
| `ema(n)` | 1 | 指数移动平均（用前 n 根 SMA 作种子） |
| `rsi(n)` | 1 | RSI，Wilder 平滑 |
| `macd(fast, slow, signal)` | 3 | MACD 线 |
| `macd_signal(fast, slow, signal)` | 3 | MACD 信号线 |
| `macd_hist(fast, slow, signal)` | 3 | MACD 柱 |
| `boll_upper(n, k)` / `boll_mid(n, k)` / `boll_lower(n, k)` | 2 | 布林带上 / 中 / 下轨，k 为标准差倍数 |
| `atr(n)` | 1 | ATR，Wilder 平滑 |
| `volume_ratio(n)` | 1 | 量比：当前成交量 / 前 n 根均量（不含当前） |
| `change_pct(n)` | 1 | n 根涨跌幅（%）：`(C / C[n] - 1) * 100` |
| `highest(n)` | 1 | 前 n 根最高价（不含当前，用于突破判断） |
| `lowest(n)` | 1 | 前 n 根最低价（不含当前） |

合约数据（与周期无关，按币种）：

| 函数 | 参数个数 | 含义 |
|---|---|---|
| `funding_rate` | 0 | 当前资金费率（markPrice 推送，小数，如 0.0001 = 0.01%） |
| `mark_price` | 0 | 标记价格 |
| `oi` | 0 | 持仓量（每 5 分钟采样） |
| `oi_change_pct(n)` | 1 | 持仓量相对 n 个采样前的变化（%）；n=4 约等于 20 分钟 |
| `long_short_ratio` | 0 | 全市场多空账户比（5m） |
| `top_long_short_ratio` | 0 | 大户多空持仓比（5m） |
| `liq_volume(m)` | 1 | 最近 m 分钟爆仓名义金额（价格 × 数量，USDT） |

只引用合约数据的规则：`trigger: on_close`（默认）时同样只在该规则周期的 K 线收盘时评估；
只有写成 `trigger: realtime` 才会在每次对应数据到达时评估（资金费率每秒一次）。

### trigger：on_close 与 realtime

- `on_close`（默认）：只在该周期 K 线**收盘**后评估一次，避免盘中来回抖动产生假信号。
- `realtime`：每收到一次 1m 推送，就用该周期**未收盘**的 K 线评估。
  **限制（R3）：`realtime` 规则只能使用 `close`、`change_pct(n)`、`volume` 和合约数据函数**，
  其它指标（RSI、MACD 等）在未收盘的 K 线上没有意义，配置校验会直接拒绝。

## 防刷屏

1. **边沿触发**：条件从“不满足”变成“满足”才触发一次，持续满足不会重复推送。
2. **收盘确认**：默认 `on_close`。
3. **冷却**：按“规则 + 币种”计算，冷却期内再次出现的边沿不推送，但会落库（`suppressed = cooldown`）。
   规则不写 `cooldown` 时默认为 **max(规则周期, 15m)**；确实想要无冷却请显式写 `cooldown: 0s`。
4. **合并推送**：每个渠道一个合并窗口（`notify.merge_window`），窗口内的信号合成一条消息；
   同一窗口里“同一规则 + 同一币种”的多次信号合并成一行（取最新值，结尾标 `(×N)`）。
5. **限速 + 免打扰**：每个渠道每分钟最多 `rate_per_minute` 条信号，超出的落库为 `suppressed = rate`；
   免打扰时段内低于 `quiet_min_level` 的信号落库为 `suppressed = quiet`。`critical` 级别不受这两条限制。
6. **过期 K 线不推送**：断线恢复后补回来的 K 线照常更新指标，但如果一根 K 线收盘已超过 max(2 个周期, 2 分钟)，
   它触发的信号只落库（`suppressed = stale`），不推送——那时行情已经过去了。
7. **被压下的信号也落库**：所有信号（推送的和被压下的）都写入 `signals` 表，方便复盘。

系统告警（断线、停更等）不经过上面的合并/限速，直接发到**所有**渠道；同一标题 10 分钟内最多一条。

## 通知渠道参数

| type | params | 说明 |
|---|---|---|
| `telegram` | `token`、`chat_id`，可选 `base_url` | Bot API `sendMessage`，HTML 格式；超过 4096 字符会截断行并注明。 |
| `discord` | `url` | Webhook 地址；2000 字符限制。 |
| `webhook` | `url` | POST JSON：`{"title","level","lines","links"}`，任何 2xx 视为成功。 |
| `wecom` | `url` | 企业微信群机器人 Webhook（markdown，4096 字节）。 |
| `dingtalk` | `url`、`secret` | 钉钉机器人，**必须**开启“加签”并填 `secret`。 |
| `feishu` | `url`，可选 `secret` | 飞书机器人；开启签名校验时填 `secret`。 |
| `email` | `host`、`port`、`username`、`password`、`from`、`to`，可选 `insecure` | SMTP 纯文本邮件，`to` 用逗号分隔多个地址。 |

邮件的 TLS 与认证策略：

- 端口 `465` 使用隐式 TLS；其它端口在服务器支持时使用 STARTTLS（TLS 1.2+，校验证书）。
- 服务器**不支持 STARTTLS 时默认拒绝发送**（避免明文发送密码），除非主机是 `localhost` / `127.0.0.1` / `::1`，
  或显式设置 `insecure: "true"`（只认精确的字符串 `true`）。
- **SMTP 认证是强制的**：`username`、`password` 必填，服务器不支持 AUTH 时发送失败。
  这意味着不需要认证的本地中继（例如本机 postfix 开放中继）目前不能直接用，需要给它配一个账号。

## 本地运行

需要 Go 1.26+、一个 PostgreSQL（最好带 TimescaleDB 扩展）。

```sh
export TELEGRAM_TOKEN=... TELEGRAM_CHAT_ID=... DATABASE_URL='postgres://user:pass@localhost:5432/traderbot?sslmode=disable'
go run ./cmd/traderbot -config configs/config.yaml -http :8080
curl -i localhost:8080/healthz
```

参数：

- `-config`：配置文件路径（默认 `configs/config.yaml`），文件所在目录会被监听用于热加载。
- `-http`：`/healthz`、`/metrics` 的监听地址（默认 `:8080`）。
- `-healthcheck`：请求 `-http` 地址上的 `/healthz`，返回 200 时退出码 0，否则 1（给容器健康检查用）。
- `-no-db`（隐藏参数，仅用于本地冒烟测试）：不连数据库，所有落库数据直接丢弃。

数据库迁移在启动时自动执行。`SIGINT` / `SIGTERM` 触发优雅退出，10 秒内没退完会强制退出（退出码 1）。

`/healthz`：启动中（WS 还没连上过）返回 503 `starting`；断线或有币种停更返回 503 `degraded`；正常 200 `ok`。

`/metrics` 主要指标：`traderbot_ws_connected`、`traderbot_symbols_stale`、`traderbot_symbol_last_event_seconds{symbol}`、
`traderbot_router_backpressure_total`、`traderbot_writer_dropped_total`、`traderbot_dispatcher_dropped_total{channel}`。

测试：

```sh
go test -race ./...
```

`cmd/traderbot` 里的端到端测试用本地假币安（REST + WS 回放录制数据和一段构造的拉升）和假 Telegram 跑完整链路，不访问外网。

## 用 Docker Compose 部署

```sh
cp deploy/.env.example deploy/.env                  # 填密钥和数据库密码（密码只用字母数字和 - _ . ~）
cp configs/config.example.yaml configs/config.yaml  # 写规则
docker compose -f deploy/docker-compose.yml up -d --build
curl -i localhost:8080/healthz
docker compose -f deploy/docker-compose.yml logs -f traderbot
```

- 两个服务：`timescaledb`（`timescale/timescaledb:latest-pg16`，数据在 `tsdb-data` 卷里，`pg_isready` 健康检查）
  和 `traderbot`（等数据库健康后再启动，`restart: unless-stopped`）。
- `deploy/.env` 既用于 compose 变量替换（数据库账号），也作为 `traderbot` 容器的环境变量（配置里的 `${VAR}`）。
  `DATABASE_URL` 由 compose 根据 `POSTGRES_*` 自动拼好。
- 挂载的是整个 `configs/` **目录**（只读），而不是单个文件：编辑器保存时通常是“写新文件再改名”，
  单文件挂载会一直指向旧文件，热加载就永远不会触发。直接在宿主机上改 `configs/config.yaml` 即可生效。
- 镜像基于 `gcr.io/distroless/static:nonroot`，里面没有 shell 和 curl，容器健康检查用 `traderbot -healthcheck`。
- 容器以 uid **65532**（nonroot）运行：`configs/` 目录和 `config.yaml` 必须对它可读，
  例如 `chmod 755 configs && chmod 644 configs/config.yaml`（`600` 且属主不是 65532 时会启动失败）。
- 端口只绑定在 `127.0.0.1:8080`；需要外部访问 `/metrics` 请自行加反向代理和鉴权。

### 首次部署前：验证 TimescaleDB 迁移

开发环境里没有 Docker，Timescale 相关的 DDL（扩展、hypertable、压缩策略）**还没在真实 TimescaleDB 上跑过**。
上生产前请执行一次存储层的集成测试（见 `internal/storage/README.md`）：

```sh
docker run -d --name tb-tsdb -p 5433:5432 -e POSTGRES_PASSWORD=pg timescale/timescaledb:latest-pg16
TEST_DATABASE_URL='postgres://postgres:pg@localhost:5433/postgres?sslmode=disable' \
  go test -race -count=1 -tags integration ./internal/storage/...
docker rm -f tb-tsdb
```

这次运行**不要**设置 `TB_SKIP_TIMESCALE`。如果迁移有问题，机器人启动时会直接报错退出，不会静默失败。

## 已知限制

P2 及以后才做：

- Telegram 命令（增删币、暂停规则、查状态）、信号复盘统计；回测、模拟盘、`Strategy` 接口（P3）；下单（P4）；Web 后台（P5）。

当前版本需要知道的：

- **热加载范围**：规则、币种池、`intervals`、`notify.*` 热加载即时生效。**通知渠道（`channels`）、`database`、`binance` 的修改需要重启**，
  热加载时只会打一条 Warn 日志。新加的规则如果引用了新渠道，在重启前发往该渠道的消息会被丢弃（有日志）。
- **重启**：边沿和冷却状态只在内存里。为了不在重启后把所有“正在满足”的条件再推一遍，预热结束时会用历史 K 线
  先评估一次规则、把结果记为“已见过”（不推送）；这些条件要先变成不满足、再次满足才会推送。
  代价是：停机期间新出现的信号，重启后不会补发。合约数据类规则（预热时没有数据）不受此影响，重启后可能会推送一次。
  启动时会向所有渠道发一条 `traderbot 已启动`，同时用来确认各渠道可用。
  启动时连不上数据库或币安 exchangeInfo 会在进程内重试约 1 分钟（只发一条 `启动重试中`），仍失败才发 `启动失败` 并退出。
- **预热超时**：每个币的历史拉取限时 90 秒。超时时使用已拉到的周期，其余周期冷启动；出错时整个币冷启动。
  两种情况都会发 `预热失败: <币种>` 告警——冷启动的周期要等实时 K 线积累够（1h/4h/1d 可能要很多天），期间相关规则不会触发。
  REST 统一限流在每分钟 1200 权重，币种很多（例如上百个、且配置了 1h/4h/1d）时，后面的币可能等不到令牌而冷启动。
- **重连时错过的收盘 K 线**：重连后会在下一根 K 线的首个推送时（收盘约 2–3 秒后）用 REST 补回，而不是等一分钟。
- **长时间断线后补数据**：重连后补回的 K 线按正常收盘 K 线处理，规则可能对“旧”K 线触发信号。
- **下游阻塞会拖慢行情**：行情链路不丢数据，规则引擎跟不上时会反压到 WebSocket（`traderbot_router_backpressure_total` 增长，日志有 Warn）。
  持续反压时健康监控收不到新数据，表现为 `数据停更` 告警和 `/healthz` 503，即使 WS 本身是连着的。
  落库永远不会阻塞行情，数据库跟不上时丢最旧的数据并告警（`落库丢弃`）。
- **爆仓窗口**：`liq_volume` 用交易所时间和本机时间比较，本机时钟偏差大时窗口会不准，请开启 NTP。
- **配置里的 `${VAR}` 未设置时是空字符串**，不会报错；Telegram 等渠道会在启动时因为缺参数而报错退出，但 `database.dsn` 为空时会尝试连默认地址。
- **邮件**：强制 SMTP 认证；`insecure` 只认 `"true"`；超长主题不折行。
- **EMA 种子**：EMA/MACD 用 SMA 作种子，和 TradingView 在最初几十根上可能略有差异；预热 500 根后实时值不受影响。
  指标期望值来自币安真实数据 + 独立 Python 参考实现，尚未逐根对照 TradingView 导出数据（计划单独做一次优化）。
- **关闭时**：退出前最后不到一个合并窗口内的信号可能已记为“推送”但实际没发出去；
  最后几秒还没写入数据库的行情和信号也可能丢失（最终写入限时 5 秒，失败计入 `traderbot_writer_dropped_total`）。
- **同一分钟的标记价格**只存一条；数据库写入失败重试时，爆仓和信号在极少数情况下可能重复一行。
