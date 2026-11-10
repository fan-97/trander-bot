# trader-bot

币安 U 本位永续合约的**信号机器人**（P1）：实时接收行情，按 YAML 里写好的规则计算指标、判断条件，
满足时推送到 Telegram / Discord / 企业微信 / 钉钉 / 飞书 / 邮件 / 通用 Webhook，并把行情和信号落库到 TimescaleDB。

P1 **不下单**、没有 Web 后台；Telegram 上目前只有监控币种管理面板（见“Telegram 币种管理”），其它命令在 P2 及以后。

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
- `/healthz`、`/metrics`（Prometheus）和 `/status`（当前监控的币种、规则开关和状态）。
- 可选的 Telegram 币种管理面板：在 Telegram 里点按钮增删监控币种，改动写回配置文件并热加载。

## 配置

复制示例并修改：

```sh
cp configs/config.example.yaml configs/config.yaml
```

配置里可以用 `${VAR}` 引用环境变量（只在 `channels.*.params` 和 `database.dsn` 里展开）。
**引用了未设置的变量会被当成配置错误**（报错里只写变量名，不写值）；设置成空字符串是允许的。

### 所有配置项

| 键 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `symbols` | 是* | – | 监控的币种池，如 `[BTCUSDT, ETHUSDT]`。启动时会和交易所的 U 本位永续列表核对，不存在或不在交易的币会被忽略并发一条系统告警。 |
| `intervals` | 否 | 规则用到的周期 | 需要聚合/预热的周期：`1m` `5m` `15m` `1h` `4h` `1d`。写了的话，每条规则的 `interval`、`@` 引用的周期（含 `setup` 里的）和 `targets.interval` 都必须在列表里。不写时自动包含这些周期，启动时都会通过 REST 预热 500 根。 |
| `rules[].id` | 是 | – | 规则名，唯一，会出现在通知里。 |
| `rules[].enabled` | 否 | `true` | 规则开关。`false` 时规则仍会做配置校验，但不评估、不推送；改完保存即热加载生效。 |
| `rules[].symbols` | 否 | `all` | `all`（整个币种池，含自动币种）、`manual`（只对手动币种）、`auto`（只对自动币种）或币种列表（必须在 `symbols` 里），可以混写，如 `[auto, BTCUSDT]`。 |
| `auto_symbols` | 否 | 不开启 | 自动选币，见“自动选币 `auto_symbols`”。 |
| `telegram_admin.channel` | 开启时必填 | – | 在 Telegram 里管理币种用哪个 telegram 渠道的 bot，见“Telegram 币种管理”。 |
| `telegram_admin.allow` | 否 | 该渠道的 `chat_id` | 允许管理的 chat id / 用户 id 列表（数字，可写 `${VAR}`）。 |
| `telegram_admin.enabled` | 否 | `true` | 写了这一节默认开启。 |
| `rules[].interval` | 是 | – | 规则在哪个周期上评估。 |
| `rules[].trigger` | 否 | `on_close` | `on_close`：K 线收盘后评估；`realtime`：每次 1m 推送都用未收盘的 K 线评估（见下面的限制）。 |
| `rules[].when` | 是 | – | 条件树，见“规则语法”。 |
| `rules[].cooldown` | 否 | max(周期, 15m) | 同一规则 + 同一币种两次推送的最小间隔，如 `30m`、`1h`；`0s` 表示不冷却。 |
| `rules[].channels` | 是 | – | 推送到哪些渠道（`channels` 里的名字）。 |
| `rules[].level` | 否 | `warn` | `info` / `warn` / `critical`。`critical` 不受限速和免打扰限制。 |
| `rules[].setup` | 否 | – | 两阶段规则的前置条件，见下面“两阶段规则 `setup`”。 |
| `rules[].targets` | 否 | – | 基于 ATR 的止损 / 止盈参考价，只显示在通知里，不影响是否触发。见下面“ATR 止损止盈参考”。 |
| `channels.<名字>.type` | 是 | – | `telegram` `discord` `webhook` `email` `wecom` `dingtalk` `feishu`。 |
| `channels.<名字>.fallback` | 否 | – | 重试 3 次仍失败时转发到的备用渠道名（只转发一次；系统告警不转发）。 |
| `channels.<名字>.params` | 视类型 | – | 见“通知渠道参数”。 |
| `notify.merge_window` | 否 | `10s` | 合并窗口：同一渠道在窗口内的信号合并成一条消息。 |
| `notify.rate_per_minute` | 否 | `20` | 每个渠道每分钟最多推送多少条**信号**（令牌桶）。 |
| `notify.quiet_hours.start` / `end` | 否 | – | 免打扰时段，`"23:00"` 格式，可跨午夜。 |
| `notify.quiet_hours.tz` | quiet_hours 时必填 | – | 时区，如 `Asia/Shanghai`。 |
| `notify.tz` | 否 | `Asia/Shanghai` | 消息里 K 线时间用的时区，如 `UTC`。可热加载。 |
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

操作数是函数调用或数字常量（可为负数、小数）。K 线函数后面可以加 `@周期` 读取另一个周期，见“跨周期引用”。例子：

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

  - id: fast_move_5m             # 盘中 5 分钟快速波动
    interval: 5m
    trigger: realtime
    when:
      all:
        - amplitude_pct > 3          # 这根 5m 振幅超过 3%
        - volume_ratio(20) > 1.5     # 成交量已达前 20 根均量的 1.5 倍
    channels: [telegram]

  - id: wick_down                  # 下插针：急跌后被拉回，收盘确认
    interval: 5m
    when:
      all:
        - lower_wick_atr(14) > 2     # 下影线超过 2 倍 ATR
        - wick_body_ratio > 3        # 影线是实体的 3 倍以上
        - volume_ratio(20) > 2
    channels: [telegram]

  - id: golden_cross
    interval: 4h
    when: cross_up(ema(12), ema(26))
    channels: [telegram]
```

### 跨周期引用 `@周期`

在 K 线函数后面加 `@1m` `@5m` `@15m` `@1h` `@4h` `@1d`，就读取那个周期的值，例如 `ema(20)@4h`、`close@1d`。
不加后缀时用规则自己的 `interval`。

- 读的是该周期**最近一根已收盘** K 线上的值，不会用未收盘的高周期 K 线（不会重绘、不会偷看未来）。
  1h 和 4h 在同一分钟收盘时（如 08:00），先更新 4h，再评估 1h 规则，所以 1h 规则读到的是刚收盘的那根 4h。
- `realtime` 规则里的 `@` 操作数同样读已收盘的值，因此可以用任何指标（如 `ema(20)@1h`），不受 R3 限制。
- 合约数据函数与周期无关，不能加 `@`。

例子：4H 趋势 + 1H 回踩 4H EMA20：

```yaml
  - id: trend_pullback_ema_long
    interval: 1h
    when:
      all:
        - ema(20)@4h > ema(50)@4h    # 4H 多头排列
        - close@4h > ema(50)@4h      # 4H 收盘在 EMA50 上方
        - low <= ema(20)@4h          # 1H 最低价回踩到 4H EMA20
        - close > ema(20)@4h         # 1H 收回 EMA20 上方
    cooldown: 4h
    targets: {side: long, atr: 14, stop: 1.5, take: [2, 3]}
    channels: [telegram]
```

### 两阶段规则 `setup`：先满足前置条件，再等触发

有些信号要求严格的先后顺序，比如“4H 上涨中 1H 先回踩确认，之后 15m 再放量突破”。给规则加 `setup` 即可：

```yaml
  - id: trend_pullback_breakout_long
    interval: 15m
    trigger: realtime                        # 盘中突破就发，不等 15m 收盘
    setup:                                   # 第一阶段：4H 上涨 + 1H 回踩确认
      when:
        all:
          - ema(20)@4h > ema(50)@4h
          - close@4h > ema(50)@4h
          - low@1h <= ema(20)@4h             # 1H 回踩到 4H EMA20
          - close@1h > ema(20)@4h            # 并收回 EMA20 上方
      invalidate: close@1h < ema(50)@4h      # 可选：1H 跌破 4H EMA50 就作废
      within: 8h                             # 回踩确认后 8 小时内有效
    when:                                    # 第二阶段：15m 放量突破
      all:
        - ema(20)@4h > ema(50)@4h            # 突破时趋势仍在
        - close > highest(20)                # 盘中突破前 20 根 15m 高点
        - volume_ratio(20) > 2               # 这根 15m 到目前的量已达均量 2 倍
    cooldown: 4h
    targets: {side: long, atr: 14, stop: 1.5, take: [2, 3]}
    channels: [telegram]
```

规则是这样走的：

1. `setup.when` 从不成立变成成立时，规则进入“待触发”。之后只要 `setup.when` 还成立，有效期就一直往后延；
   它不再成立后再过 `within`，待触发就失效。
2. 待触发期间，`when` **从不成立变成成立**才推送。突破如果在回踩确认之前就已经发生（价格一直在高点上方），不算。
3. 一次待触发最多推一次。推送后，要等 `setup.when` 先不成立、再成立一次，才会重新进入待触发。
4. `invalidate` 成立时，待触发立即作废。
5. 推送内容会附上前置条件进入待触发时的取值，例如
   `close=64120 > highest(20)=64000; volume_ratio(20)=2.4 > 2 | 前置: ema(20)@4h=63500 > ema(50)@4h=62000; ...`。
6. 启动或重启时，如果 `setup.when` 已经成立，从启动那一刻起进入待触发；已经成立的 `when` 不算新的突破。
   待触发状态只保存在内存里，修改这条规则的 `when`/`setup` 并热加载后会清空。

`setup` 的配置项：`when`（必填，条件树）、`within`（必填，时长，如 `8h`）、`invalidate`（可选，条件树）。
`realtime` 规则的操作数限制同样适用于 `setup` 里的条件（`@周期` 操作数不受限）。

### ATR 止损止盈参考 `targets`

```yaml
    targets:
      side: long      # long | short（必填）
      atr: 14         # ATR 周期数，默认 14
      interval: 1h    # 在哪个周期上算 ATR，默认规则自己的 interval
      stop: 1.5       # 止损 = 价格 ∓ 1.5×ATR；不写或 0 表示不显示止损
      take: [2, 3]    # 止盈 = 价格 ± k×ATR，可写多档
```

价格是信号价格（触发 K 线的收盘价，`realtime` 为当前价），ATR 是该周期最近一根已收盘 K 线的值。
通知末尾会附加类似 `ATR(14)@1h=412.35 止损 59381.48 止盈 60824.69 / 61237.04`。ATR 还没预热好时不显示这一段，信号照常推送。
这些价格只是参考，不会下单，也不会写入数据库的 `signals.detail`。

求值是三值逻辑：数据不足（例如预热不够、合约数据还没到）时叶子为“未知”，未知不会触发，也不会重置边沿状态。

### 可用函数

K 线指标（在规则的 `interval` 上按收盘 K 线计算；加 `@周期` 则在那个周期上计算）：

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
| `open` / `high` / `low` | 0 | 当根开盘价 / 最高价 / 最低价 |
| `amplitude_pct` | 0 | 振幅（%）：`(最高 - 最低) / 前一根收盘 * 100` |
| `upper_wick_pct` / `lower_wick_pct` | 0 | 上 / 下影线长度占收盘价的 %（上影线 = 最高 - max(开, 收)，下影线 = min(开, 收) - 最低） |
| `upper_wick_atr(n)` / `lower_wick_atr(n)` | 1 | 上 / 下影线长度是 ATR(n) 的几倍；ATR 只用之前的 K 线，不含当根，插针本身不会把比值拉低 |
| `wick_body_ratio` | 0 | 较长那根影线 ÷ 实体；实体最小按收盘价的 0.01% 计，十字星不会除以 0 |

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
  **限制（R3）：`realtime` 规则只能使用 `close`、`open`、`high`、`low`、`change_pct(n)`、`amplitude_pct`、
  `volume`、`volume_ratio(n)`、`highest(n)`、`lowest(n)`、`@周期` 操作数和合约数据函数**，其它指标（RSI、MACD、影线等）在未收盘的 K 线上没有意义，配置校验会直接拒绝。
  - 盘中 `high` / `low` / `amplitude_pct` 用的是这根 K 线开盘以来的最高 / 最低价。
  - 盘中 `highest(n)` / `lowest(n)` = 前 n 根**已收盘** K 线的最高 / 最低价，不含正在走的这根，整根 K 线期间不变；
    所以 `close > highest(20)` 就是“当前价突破前 20 根高点”的盘中突破。
  - 盘中 `volume_ratio(n)` = 这根 K 线**到目前为止**的成交量 ÷ 前 n 根已收盘 K 线的均量。刚开盘时累计量还小，所以这个条件偏严，只有真正放量才成立。

## 自动选币 `auto_symbols`

定期从币安 U 本位永续里挑出最近几天“排名靠前”的币，加入监控；**手动写在 `symbols` 里的币不受影响**，永远不会被自动移除。
自动币种只在内存里，不会改写 `config.yaml`；`symbols` 可以留空，只靠自动选币（上表 `symbols` 的 * 即指此）。

```yaml
auto_symbols:
  enabled: true               # 写了这一节默认就是 true
  top: 20                     # 自动加入多少个（不含手动币种）
  rank_by: volume_growth      # volume_growth | quote_volume | abs_change | gain
  days: 7                     # 按最近几天排名（已收盘的日线）
  base_days: 30               # volume_growth 的对比基准：再往前的天数
  min_quote_volume: 20000000  # 最近 days 天日均成交额下限（USDT）
  min_listing_days: 30        # 上线至少多少天
  keep_rank: 30               # 已选中的币排名掉到这个名次以外才移除，默认 top × 1.5
  refresh: 6h                 # 多久重新选一次（最少 10m）
  exclude: [XXXUSDT]          # 永远不选的币
```

筛选步骤：

1. **硬条件**：U 本位永续、交易中、标的是币（排除 BTCDOM 等指数合约）、不是稳定币或黄金代币（USDC、FDUSD、PAXG、XAUT 等）、
   不在 `exclude` 里、不是手动币种、上线满 `min_listing_days` 天。
2. **24h 成交额粗筛**：一次请求拿全部币的 24h 成交额，低于 `min_quote_volume` 的直接跳过，省 REST 权重。
3. **日线打分**：剩下的每个币取 `days + base_days` 根日线，并要求最近 `days` 天日均成交额 ≥ `min_quote_volume`。打分方式：
   - `volume_growth`（默认）：最近 `days` 天日均成交额 ÷ 之前 `base_days` 天日均，找“突然变热”的币。
     基准期数据不足一半的币（刚上线不久）不参与这种排名。小币基准很低时倍数可能上百，嫌太激进就调高 `min_quote_volume`。
   - `quote_volume`：最近 `days` 天日均成交额，最稳定，基本是大币。
   - `abs_change`：最近 `days` 天涨跌幅的绝对值，涨跌都算。
   - `gain`：最近 `days` 天涨幅，只看涨。
4. **取前 `top` 个，防抖**：上次已选中的币只要还在前 `keep_rank` 名就保留，避免在边界上反复加入、移除
   （每次加入都要预热，每次移除都会清掉该币的冷却和 `setup` 状态）。

运行时：

- 启动时先选一次。选币失败时：有手动币种就先只监控手动币种，10 分钟后重试；没有手动币种就按启动重试流程重试，仍失败则退出。
- 之后每 `refresh` 重选一次；热加载时 `auto_symbols` 这一节变了会立即重选，改成 `enabled: false` 会立即移除全部自动币种。
- 自动币种有变化时推送一条“自动币种更新”系统消息，列出新增（名次、量增倍数、日均成交额、涨跌幅）和移除（原因）的币。
- 新加入的币会像热加载新增币种一样自动订阅 WebSocket 并预热历史 K 线；移除的币停止监控。

调参时可以先看排名，不启动机器人（只调用币安公开行情接口）：

```sh
go run ./cmd/traderbot -config configs/config.yaml -rank
# 或在容器里：
docker compose -f deploy/docker-compose.yml run --rm --no-deps traderbot -config /app/configs/config.yaml -rank
```

没写 `auto_symbols` 时 `-rank` 按默认参数排名。输出包括名次、得分、量增倍数、日均成交额、涨跌幅和是否会被选中。

## Telegram 币种管理

在配置里加上这一节，就能直接在 Telegram 里增删**手动**监控币种（`symbols`），带按钮界面：

```yaml
telegram_admin:
  channel: telegram          # 用哪个 type: telegram 的渠道的 bot（同一个 token）
  allow: [123456789]         # 可选：允许操作的 chat id / 用户 id；不写就是该渠道的 chat_id
```

在允许的聊天里给 bot 发：

| 命令 | 作用 |
|---|---|
| `/symbols` | 打开管理面板：列出手动币种和自动币种，每个手动币种一个按钮，点一下→确认即删除；「➕ 添加」后直接发币种；超过 24 个自动分页 |
| `/add BTC ETHUSDT` | 添加，多个用空格或逗号分隔；没写计价币时自动补 `USDT`（`BTC` → `BTCUSDT`） |
| `/del SOLUSDT` | 删除 |
| `/cancel` | 取消待输入的添加 |
| `/help` | 帮助 |

- **热加载**：修改直接写回 `config.yaml` 的 `symbols` 那一行（改成 `symbols: [A, B, C]` 单行写法，文件其它内容和注释原样保留），
  然后走正常的配置热加载：校验 → 订阅 WebSocket → 预热历史 K 线，无需重启。面板会等热加载完成后显示「已热加载生效」。
- 写入前先完整校验新配置，不合法就不写（例如删掉的币还被某条规则的 `symbols` 点名引用，会提示先改规则）；
  添加时会先查币安，不是可交易的 U 本位永续合约的会被跳过。
- 自动选币选出的币只读，面板里单独列出；想排除请用 `auto_symbols.exclude`。
- 只有 `allow` 里的 chat / 用户能操作；其他人发命令会收到“无权限”并附上自己的 chat id，方便配置。群里使用时，
  把群的 chat id（负数）或你自己的用户 id 加进 `allow`。
- bot 用 `getUpdates` 长轮询收消息：同一个 bot token 不能设置 webhook，也不能同时被别的程序 `getUpdates`，否则会告警“Telegram 管理面板不可用”。
  bot 停机期间发的超过 5 分钟的消息会被忽略。
- `telegram_admin` 本身的修改需要重启才生效。
- Docker 部署时 `configs/` 需要以可写方式挂载，并且 `config.yaml`（最好连同 `configs/` 目录）对 uid 65532 可写，见下面的部署说明。

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

**信号消息格式**：每条信号一行，例如

```
[5m 11-02 14:35 盘中] IDOLUSDT fast_up_5m @ 0.025466 本根 +2.31% | amplitude_pct=3.726401 > 3; volume_ratio(20)=1.541964 > 1.5
```

方括号里依次是：周期、这根 K 线的开盘时间（`notify.tz` 时区）、`盘中`（realtime 规则，K 线还没收盘）或 `收盘`（on_close 规则）。
`本根 ±x%` 是这根 K 线收盘价（盘中为当前价）相对其开盘价的涨跌幅；`@` 后面是触发时的价格；`|` 后面是各条件的实际值。
只由合约数据（资金费率等）触发的 realtime 信号没有对应的 K 线，不显示时间和涨跌幅。

系统告警（断线、停更等）不经过上面的合并/限速，直接发到**所有**渠道；同一标题 10 分钟内最多一条。

**渠道自检**：某个渠道重试 3 次仍发不出去时，会向**其它**渠道发 `渠道失败: <名字>`（每个渠道 10 分钟最多一条）；
连续 3 次失败且 10 分钟内没有成功过的渠道会让 `/healthz` 返回 503（`channels_failing`），
`/metrics` 里有 `traderbot_notify_failures_total{channel}`。所以建议至少配置两个渠道，否则唯一的渠道坏了只能从日志和 `/healthz` 看出来。

**配置热加载反馈**：保存配置后成功生效会发 `配置已生效`；失败会发 `配置无效 #<短哈希>`（不同的错误哈希不同，不会被 10 分钟限流吞掉）。
规则引用了启动时没有的通知渠道时，整份新配置会被拒绝（新增渠道需要重启）；修改已有渠道的参数会提示 `通知渠道变更需重启`。

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
- `-http`：`/healthz`、`/metrics`、`/status` 的监听地址（默认 `:8080`）。
- `-rank`：打印 `auto_symbols` 的当前排名后退出，不启动机器人（见“自动选币”）。
- `-healthcheck`：请求 `-http` 地址上的 `/healthz`，返回 200 时退出码 0，否则 1（给容器健康检查用）。
- `-no-db`（隐藏参数，仅用于本地冒烟测试）：不连数据库，所有落库数据直接丢弃。

数据库迁移在启动时自动执行。`SIGINT` / `SIGTERM` 触发优雅退出，10 秒内没退完会强制退出（退出码 1）。

`/healthz`：启动中（WS 还没连上过）返回 503 `starting`；断线或有币种停更返回 503 `degraded`；正常 200 `ok`。

`/status`：返回当前运行状态的 JSON（只读，仅 GET）：

```sh
curl -s localhost:8080/status
curl -s localhost:8080/status | jq '.symbols'                               # 手动 / 自动币种
curl -s localhost:8080/status | jq '.rules[] | {id, enabled, applies_to}'   # 规则开关和作用的币
```

- `symbols`：`total`、`manual`（手动）、`auto`（自动选币加入的）。
- `auto_symbols`：未开启时为 `null`；否则包括排名方式、上次 / 下次选币时间、上次失败原因，以及每个已选币的名次、得分、量增倍数、日均成交额、涨跌幅；`kept` 表示靠 `keep_rank` 保留的币。
- `rules`：配置里的每条规则（包括 `enabled: false` 的），含开关、周期、触发方式、冷却、渠道、配置的 `symbols`、
  当前实际作用的币（`applies_to`）、条件（`when`、`setup`）、`targets`，以及每个币最近一次触发的时间（`last_fired`）。
  冷却中被抑制的不算触发；状态只在内存里，重启后清空。
- `feeds`：每个币最近一次收到行情的时间和距今秒数。
- 时间按 `notify.tz` 时区显示。端口默认只绑定本机，`/status` 没有鉴权，不要直接暴露到公网。

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

- 两个服务：`timescaledb`（固定为 `timescale/timescaledb:2.17.2-pg16`，数据在 `tsdb-data` 卷里，`pg_isready -h 127.0.0.1` 健康检查）
  和 `traderbot`（等数据库健康后再启动，`restart: unless-stopped`）。
  升级 TimescaleDB 时请改 `deploy/docker-compose.yml` 里的镜像标签，并先用新标签跑一遍下面的集成测试。
- 两个服务的日志都做了轮转（单文件 10MB，保留 3 个）。
- 注意：Docker 的健康检查只会把容器标成 unhealthy，**不会自动重启**；需要自动重启请另配 autoheal 之类的工具。
- `deploy/.env` 既用于 compose 变量替换（数据库账号），也作为 `traderbot` 容器的环境变量（配置里的 `${VAR}`）。
  `DATABASE_URL` 由 compose 根据 `POSTGRES_*` 自动拼好。
- 挂载的是整个 `configs/` **目录**，而不是单个文件：编辑器保存时通常是“写新文件再改名”，
  单文件挂载会一直指向旧文件，热加载就永远不会触发。直接在宿主机上改 `configs/config.yaml` 即可生效。
- 挂载是可写的，供 Telegram 币种管理把修改写回 `config.yaml`。要用这个功能，需让 uid 65532 能写：
  `sudo chown 65532 configs configs/config.yaml`（目录可写时用“写临时文件再改名”原子替换；只有文件可写时原地改写）。
  不用这个功能可以在 compose 里把挂载改回 `:ro`。
- 镜像基于 `gcr.io/distroless/static:nonroot`，里面没有 shell 和 curl，容器健康检查用 `traderbot -healthcheck`。
- 容器以 uid **65532**（nonroot）运行：`configs/` 目录和 `config.yaml` 必须对它可读，
  例如 `chmod 755 configs && chmod 644 configs/config.yaml`（`600` 且属主不是 65532 时会启动失败）。
- 端口只绑定在 `127.0.0.1:8080`；需要外部访问 `/metrics` 请自行加反向代理和鉴权。

### 首次部署前：验证 TimescaleDB 迁移

开发环境里没有 Docker，Timescale 相关的 DDL（扩展、hypertable、压缩策略）**还没在真实 TimescaleDB 上跑过**。
上生产前请执行一次存储层的集成测试（见 `internal/storage/README.md`）：

```sh
docker run -d --name tb-tsdb -p 5433:5432 -e POSTGRES_PASSWORD=pg timescale/timescaledb:2.17.2-pg16
TEST_DATABASE_URL='postgres://postgres:pg@localhost:5433/postgres?sslmode=disable' \
  go test -race -count=1 -tags integration ./internal/storage/...
docker rm -f tb-tsdb
```

这次运行**不要**设置 `TB_SKIP_TIMESCALE`。如果迁移有问题，机器人启动时会直接报错退出，不会静默失败。

## 已知限制

P2 及以后才做：

- Telegram 命令（暂停规则、查状态）、信号复盘统计；回测、模拟盘、`Strategy` 接口（P3）；下单（P4）；Web 后台（P5）。

当前版本需要知道的：

- **热加载范围**：规则、币种池、`intervals`、`notify.*` 热加载即时生效。**通知渠道（`channels`）、`database`、`binance` 的修改需要重启**，
  热加载时只会打一条 Warn 日志。新加的规则如果引用了新渠道，在重启前发往该渠道的消息会被丢弃（有日志）。
- **重启**：边沿和冷却状态只在内存里。为了不在重启后把所有“正在满足”的条件再推一遍，预热结束时会用历史 K 线
  先评估一次规则、把结果记为“已见过”（不推送）；这些条件要先变成不满足、再次满足才会推送。
  代价是：停机期间新出现的信号，重启后不会补发。用到合约数据的规则（包括 K 线 + 合约数据混合的规则；预热时还没有合约数据）不受此影响，重启后可能会推送一次。
  启动时会向所有渠道发一条 `traderbot 已启动`，同时用来确认各渠道可用。
  启动时连不上数据库或币安 exchangeInfo 会在进程内重试约 1 分钟（只发一条 `启动重试中`），仍失败才发 `启动失败` 并退出。
- **预热超时**：每个币的历史拉取限时 90 秒。超时时使用已拉到的周期，其余周期冷启动；出错时整个币冷启动。
  两种情况都会发 `预热失败: <币种>` 告警（5 秒内的多个失败合并成一条 `预热失败: N 个币种`）——
  冷启动的周期要等实时 K 线积累够（1h/4h/1d 可能要很多天），期间相关规则不会触发。
  热加载新增的周期也会先拉历史再用；拉取前收盘的第一根（不完整的）K 线会被丢弃，不参与计算。
  预热拉到的 K 线中，比数据库里已有的更新的那部分会写入数据库（补上停机期间的空档）。
  REST 统一限流在每分钟 1200 权重，币种很多（例如上百个、且配置了 1h/4h/1d）时，后面的币可能等不到令牌而冷启动。
- **重连时错过的收盘 K 线**：重连后会在下一根 K 线的首个推送时（收盘约 2–3 秒后）用 REST 补回，而不是等一分钟。
- **长时间断线后补数据**：重连后补回的 K 线会更新指标，但过期 K 线触发的信号只落库不推送（见“防刷屏”第 6 条）。
- **下游阻塞会拖慢行情**：行情链路不丢数据，规则引擎跟不上时会反压到 WebSocket（`traderbot_router_backpressure_total` 增长，日志有 Warn）。
  持续反压时健康监控收不到新数据，表现为 `数据停更` 告警和 `/healthz` 503，即使 WS 本身是连着的。
  落库永远不会阻塞行情，数据库跟不上时丢最旧的数据并告警（`落库丢弃`）。
  数据库写入连续失败 2 分钟会发 `数据库写入失败`，恢复后发 `数据库写入恢复`（`/metrics` 有 `traderbot_writer_failing`）。
- **爆仓窗口**：`liq_volume` 用交易所时间和本机时间比较，本机时钟偏差大时窗口会不准，请开启 NTP。
- **邮件**：强制 SMTP 认证；`insecure` 只认 `"true"`；超长主题不折行。
- **EMA 种子**：EMA/MACD 用 SMA 作种子，和 TradingView 在最初几十根上可能略有差异；预热 500 根后实时值不受影响。
  指标期望值来自币安真实数据 + 独立 Python 参考实现，尚未逐根对照 TradingView 导出数据（计划单独做一次优化）。
- **关闭时**：退出前最后不到一个合并窗口内的信号可能已记为“推送”但实际没发出去；
  最后几秒还没写入数据库的行情和信号也可能丢失（最终写入限时 5 秒，失败计入 `traderbot_writer_dropped_total`）。
- **同一分钟的标记价格**只存一条；数据库写入失败重试时，爆仓和信号在极少数情况下可能重复一行。
