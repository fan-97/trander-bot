// Command traderbot watches Binance USDT-M perpetual futures, evaluates the
// rules in a YAML config and pushes signals to notification channels.
//
//	traderbot -config configs/config.yaml -http :8080
//	traderbot -healthcheck -http :8080   # container healthcheck: exit 0 if /healthz is 200
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	ossignal "os/signal"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/engine"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/health"
	"trader-bot/internal/market"
	"trader-bot/internal/notify"
	"trader-bot/internal/signal"
	"trader-bot/internal/storage"
	"trader-bot/internal/tgadmin"
)

const (
	// shutdownTimeout bounds the whole graceful shutdown after SIGINT/SIGTERM.
	shutdownTimeout = 10 * time.Second
	// warmupBars is how many bars per configured interval are fetched.
	warmupBars = 500
	// maxWarmup1m caps the extra 1m bars fetched to fill the current bucket
	// of the largest interval (R15).
	maxWarmup1m = 1500
	// staleAfter is the WS-down / per-symbol silence threshold for alerts.
	staleAfter = time.Minute
	// connectDeadline: a critical alert fires if the WS has never connected
	// this long after start.
	connectDeadline = 2 * time.Minute
	// pollEvery is the open interest / long-short sampling period.
	pollEvery = 5 * time.Minute
	// warmupAlertWindow collects 预热失败 alerts into one message (I8).
	warmupAlertWindow = 5 * time.Second
	// dbFailingAfter: continuous write failure this long raises 数据库写入失败 (I6).
	dbFailingAfter = 2 * time.Minute
	// dbCheckEvery is how often watchWriter looks at the writer.
	dbCheckEvery = 10 * time.Second
	// maxListedSymbols caps the symbols named in one aggregated alert.
	maxListedSymbols = 20

	eventBuffer  = 4096
	routerBuffer = 4096
	writerBuffer = 4096
	signalBuffer = 256
	batchBuffer  = 64

	writerBatch = 500
	writerFlush = 2 * time.Second

	statsEvery = time.Minute
)

// warmupTimeout bounds one symbol's history fetch (R16); on timeout the bars
// fetched so far are used. A var so tests can shorten it.
var warmupTimeout = 90 * time.Second

// fatalNotifyTimeout bounds how long a fatal error waits for its system
// message to be delivered before run returns.
const fatalNotifyTimeout = 5 * time.Second

// Store is the storage surface run needs: the Writer's sink plus Close.
type Store interface {
	storage.Sink
	Close()
}

// Deps are optional overrides for run; zero values mean production defaults.
type Deps struct {
	// HTTPClient is used for Binance REST and HTTP notifiers.
	HTTPClient *http.Client
	// Clock drives every component; defaults to clock.Real.
	Clock clock.Clock
	// OpenStore opens storage for the configured DSN; defaults to storage.Open.
	OpenStore func(ctx context.Context, dsn string) (Store, error)
	// StartupRetryWaits are the waits between startup retries of storage and
	// exchangeInfo; nil means defaultStartupRetryWaits.
	StartupRetryWaits []time.Duration
}

// defaultStartupRetryWaits rides out a short DB or Binance hiccup at boot
// (about a minute in total) instead of exiting into a restart loop.
var defaultStartupRetryWaits = []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}

// retryStartup runs fn, retrying after each of waits. The first failure sends
// one "启动重试中" warning (not one per attempt); the last error is returned.
// The waits are real time: this runs before the clock-driven components start.
func retryStartup(ctx context.Context, waits []time.Duration, disp *notify.Dispatcher, what string, log *slog.Logger, fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}
	disp.SendSystem(notify.Message{Title: "启动重试中", Level: config.LevelWarn,
		Lines: []string{what + " 失败，正在重试", err.Error()}})
	for _, w := range waits {
		log.Warn("startup step failed, retrying", "step", what, "in", w, "err", err)
		select {
		case <-time.After(w):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err = fn(); err == nil {
			return nil
		}
	}
	return err
}

func main() {
	fs := flag.CommandLine
	cfgPath := fs.String("config", "configs/config.yaml", "path to the YAML config (hot reloaded)")
	httpAddr := fs.String("http", ":8080", "listen address for /healthz and /metrics")
	healthcheck := fs.Bool("healthcheck", false, "GET /healthz on the -http address and exit 0 if it returns 200, else 1")
	rank := fs.Bool("rank", false, "print the auto_symbols ranking for -config (read-only Binance REST calls) and exit")
	noDB := fs.Bool("no-db", false, "") // hidden: discard all writes (local smoke tests only)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
		fs.VisitAll(func(f *flag.Flag) {
			if f.Name == "no-db" {
				return
			}
			fmt.Fprintf(fs.Output(), "  -%s\n    \t%s (default %q)\n", f.Name, f.Usage, f.DefValue)
		})
	}
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if *healthcheck {
		os.Exit(probe(*httpAddr))
	}
	if *rank {
		if err := printRank(context.Background(), *cfgPath, os.Stdout); err != nil {
			slog.Error("rank", "err", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := ossignal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, func() {
		slog.Info("shutting down")
		time.AfterFunc(shutdownTimeout, func() {
			slog.Error("shutdown did not finish in time, exiting", "timeout", shutdownTimeout)
			os.Exit(1)
		})
	})

	var deps Deps
	if *noDB {
		slog.Warn("-no-db: storage disabled, market data and signals are NOT persisted")
		deps.OpenStore = func(context.Context, string) (Store, error) { return discardStore{}, nil }
	}
	if err := run(ctx, *cfgPath, *httpAddr, deps); err != nil {
		slog.Error("traderbot exited", "err", err)
		os.Exit(1)
	}
	slog.Info("traderbot stopped")
}

// run wires every component and blocks until ctx is cancelled (returns nil)
// or a component fails (returns its error). Storage is closed last.
func run(ctx context.Context, cfgPath, httpAddr string, deps Deps) error {
	log := slog.Default()
	clk := deps.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	openStore := deps.OpenStore
	if openStore == nil {
		openStore = func(ctx context.Context, dsn string) (Store, error) {
			s, err := storage.Open(ctx, dsn)
			if err != nil {
				return nil, err
			}
			return s, nil
		}
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	disp, err := newDispatcher(cfg, deps.HTTPClient, clk)
	if err != nil {
		return err
	}
	// From here on a fatal error is also pushed to every channel.
	fatal := func(err error) error {
		if ctx.Err() == nil {
			notifyFatal(disp, "启动失败", err, log)
		}
		return err
	}

	waits := deps.StartupRetryWaits
	if waits == nil {
		waits = defaultStartupRetryWaits
	}
	var st Store
	if err := retryStartup(ctx, waits, disp, "数据库连接", log, func() (err error) {
		st, err = openStore(ctx, cfg.Database.DSN)
		return err
	}); err != nil {
		return fatal(fmt.Errorf("open storage: %w", err))
	}
	defer st.Close() // deferred first, so it runs after everything else has stopped

	mon := health.NewMonitor(clk, disp.SendSystem, staleAfter)

	rest := binance.NewREST(cfg.Binance.RESTURL, binance.DefaultWeightPerMin)
	rest.Clock = clk
	if deps.HTTPClient != nil {
		rest.HTTP = deps.HTTPClient
	}
	var have map[string]bool
	if err := retryStartup(ctx, waits, disp, "币安 exchangeInfo", log, func() (err error) {
		have, err = exchangeSymbols(ctx, rest)
		return err
	}); err != nil {
		return fatal(fmt.Errorf("load exchange symbols: %w", err))
	}
	var auto []string
	autoRan := false
	autoSt := &autoState{}
	if autoOn(cfg) {
		pick := func() error {
			sel, rows, err := selectAuto(ctx, rest, cfg, nil, clk.Now())
			if err != nil {
				autoSt.failed(err, clk.Now().Add(autoRetryAfter))
				return err
			}
			auto = sel
			autoSt.ran(clk.Now(), clk.Now().Add(cfg.AutoSymbols.Refresh), rows)
			return nil
		}
		if len(cfg.Symbols) == 0 {
			// Nothing to watch without it: retry like the other startup calls.
			if err := retryStartup(ctx, waits, disp, "自动选币", log, pick); err != nil {
				return fatal(fmt.Errorf("auto_symbols: %w", err))
			}
			autoRan = true
		} else if err := pick(); err != nil {
			log.Warn("auto_symbols: startup selection failed, starting with the manual symbols only", "err", err)
			disp.SendSystem(notify.Message{Title: "自动选币失败", Level: config.LevelWarn,
				Lines: []string{"先只监控手动币种，" + autoRetryAfter.String() + " 内重试", err.Error()}})
		} else {
			autoRan = true
		}
		log.Info("auto_symbols selected", "symbols", auto)
	}
	active, dropped := filterSymbols(cfg, have, auto)
	if len(active.Symbols) == 0 {
		return fatal(fmt.Errorf("none of the configured symbols %v is a tradable USDT perpetual", cfg.Symbols))
	}
	if len(dropped) > 0 {
		log.Warn("ignoring symbols that are not tradable USDT perpetuals", "dropped", dropped)
		disp.SendSystem(droppedMessage(dropped))
	}

	raw := config.NewStore(cfg)      // what is on disk; config.Watch publishes here
	store := config.NewStore(active) // what the bot runs on: symbols validated
	disp.Location = func() *time.Location { return store.Current().Notify.Location }

	g, gctx := errgroup.WithContext(ctx)
	// A signal-driven shutdown is bounded in main; a component failure
	// cancels gctx without a signal, so bound that shutdown here.
	var bound atomic.Pointer[time.Timer]
	stopBound := context.AfterFunc(gctx, func() {
		if ctx.Err() != nil {
			return
		}
		bound.Store(time.AfterFunc(shutdownTimeout, func() {
			slog.Error("shutdown after a component failure did not finish in time, exiting", "timeout", shutdownTimeout)
			os.Exit(1)
		}))
	})
	defer func() {
		stopBound()
		if t := bound.Load(); t != nil {
			t.Stop()
		}
	}()

	events := make(chan market.Event, eventBuffer)
	routerIn := make(chan market.Event, routerBuffer)
	writerIn := make(chan any, writerBuffer)
	signals := make(chan signal.Signal, signalBuffer)
	batches := make(chan signal.Batch, batchBuffer)
	changed := make(chan struct{}, 1)
	symbols := func() []string { return store.Current().Symbols }

	stream := binance.NewStream(cfg.Binance.WSURL, rest, clk, mon.OnConn)
	persistWarmup := func(ks []market.Kline) {
		for _, k := range unstored(gctx, st, ks, log) {
			if !k.Closed {
				continue
			}
			select {
			case writerIn <- market.KlineEvent{Kline: k}:
			case <-gctx.Done():
				return
			}
		}
	}
	warmAlert := batchWarmupAlerts(gctx, clk, warmupAlertWindow, mon.Alert)
	gate := signal.NewGate(clk)
	router := engine.NewRouter(store, gate, signals, clk,
		warmup(rest, stream, store, clk, warmAlert, persistWarmup, log))
	batcher := signal.NewBatcher(clk, func() config.NotifyConfig { return store.Current().Notify }, batches,
		func(s signal.Signal) { // every signal, delivered or suppressed, is persisted
			select {
			case writerIn <- s:
			case <-gctx.Done():
			}
		})
	writer := storage.NewWriterWithSink(st, writerBatch, writerFlush)
	writer.Clock = clk
	writer.OnDrop = func(n int) {
		mon.Alert(notify.Message{Title: "落库丢弃", Level: config.LevelWarn,
			Lines: []string{fmt.Sprintf("数据库写入跟不上，已丢弃 %d 条（累计 %d）", n, writer.Dropped())}})
	}

	mon.SetSymbols(active.Symbols)
	var fanBlocked atomic.Int64
	registerMetrics(mon, router, writer, disp, cfg)

	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fatal(fmt.Errorf("http listen: %w", err))
	}
	mux := http.NewServeMux()
	mux.Handle("/", mon.Handler()) // /healthz, /metrics
	mux.Handle("/status", statusHandler(store, autoSt, gate, mon, clk))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g.Go(func() error {
		return config.Watch(gctx, cfgPath, raw, func(err error) {
			log.Error("config reload failed, keeping the previous config", "err", err)
			mon.Alert(invalidConfigMessage("配置文件热加载失败，继续使用旧配置: " + err.Error()))
		})
	})
	// Subscribe before starting the loop so a reload published in between is not missed.
	rawSub, autoSub := raw.Subscribe(), raw.Subscribe()
	autoCh := make(chan []string)
	g.Go(func() error {
		refresh := func(ctx context.Context) (map[string]bool, error) { return exchangeSymbols(ctx, rest) }
		reloadLoop(gctx, rawSub, autoCh, cfg, store, refresh, have, mon, changed, log)
		return nil
	})
	g.Go(func() error {
		autoLoop(gctx, clk, rest, raw, autoSub, auto, autoRan, autoCh, autoSt, mon.Alert, log)
		return nil
	})
	if ta := cfg.TelegramAdmin; ta != nil && ta.Enabled {
		ch := cfg.Channels[ta.Channel]
		admin := newSymbolAdmin(cfgPath, store, have,
			func(ctx context.Context) (map[string]bool, error) { return exchangeSymbols(ctx, rest) }, log)
		bot := tgadmin.New(tgadmin.Options{
			Token: ch.Params["token"], BaseURL: ch.Params["base_url"], HTTP: deps.HTTPClient,
			Allow: ta.Allow, Backend: admin, Log: log,
			Alert: func(title, text string) {
				mon.Alert(notify.Message{Title: title, Level: config.LevelWarn, Lines: []string{text}})
			},
		})
		log.Info("telegram admin enabled", "channel", ta.Channel, "allow", ta.Allow)
		g.Go(func() error { return bot.Run(gctx) })
	}
	g.Go(func() error { return ignoreCancel(stream.Run(gctx, symbols, changed, events)) })
	g.Go(func() error { return ignoreCancel(binance.PollContract(gctx, rest, symbols, pollEvery, events, clk)) })
	g.Go(func() error {
		fanOut(gctx, events, routerIn, writerIn, mon, &fanBlocked)
		return nil
	})
	g.Go(func() error { return router.Run(gctx, routerIn) })
	g.Go(func() error { return batcher.Run(gctx, signals) })
	g.Go(func() error { return disp.Run(gctx, batches) })
	g.Go(func() error { return writer.Run(gctx, writerIn) })
	g.Go(func() error { return mon.Run(gctx) })
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return nil
		case <-clk.After(connectDeadline):
		}
		if !mon.Snapshot().Started {
			log.Error("binance websocket has not connected since start", "after", connectDeadline)
			mon.Alert(notify.Message{Title: "WS 未连接", Level: config.LevelCritical,
				Lines: []string{"启动 " + connectDeadline.String() + " 后仍未连上币安 WebSocket"}})
		}
		return nil
	})
	g.Go(func() error {
		logStats(gctx, clk, router, &fanBlocked, log)
		return nil
	})
	g.Go(func() error {
		watchWriter(gctx, clk, writer, mon.Alert, log)
		return nil
	})
	g.Go(func() error {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			log.Warn("http shutdown", "err", err)
		}
		return nil
	})

	log.Info("traderbot started", "symbols", active.Symbols, "auto", active.Auto, "intervals", active.Intervals,
		"rules", len(active.Rules), "channels", len(active.Channels), "http", ln.Addr().String())
	// Doubles as a channel self-test, and explains any signal right after a restart.
	disp.SendSystem(startedMessage(active))
	if err := g.Wait(); err != nil {
		if ctx.Err() == nil {
			notifyFatal(disp, "运行失败", err, log)
		}
		return err
	}
	return nil
}

// notifyFatal pushes a critical system message to every channel and runs the
// Dispatcher (whose own Run has not started or has already returned) until it
// is delivered or fatalNotifyTimeout passes.
func notifyFatal(disp *notify.Dispatcher, title string, err error, log *slog.Logger) {
	log.Error(title, "err", err)
	disp.SendSystem(notify.Message{Title: title, Level: config.LevelCritical,
		Lines: []string{"traderbot 即将退出", err.Error()}})
	in := make(chan signal.Batch)
	close(in) // closed input: Run drains the queues, then returns
	dctx, cancel := context.WithTimeout(context.Background(), fatalNotifyTimeout)
	defer cancel()
	_ = disp.Run(dctx, in)
}

func ignoreCancel(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func newDispatcher(cfg *config.Config, hc *http.Client, clk clock.Clock) (*notify.Dispatcher, error) {
	ns := make(map[string]notify.Notifier, len(cfg.Channels))
	fallback := map[string]string{}
	for name, ch := range cfg.Channels {
		n, err := notify.NewFromConfig(name, ch, hc)
		if err != nil {
			return nil, err
		}
		ns[name] = n
		if ch.Fallback != "" {
			fallback[name] = ch.Fallback
		}
	}
	return notify.NewDispatcher(ns, fallback, 0, clk), nil
}

func exchangeSymbols(ctx context.Context, rest *binance.REST) (map[string]bool, error) {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return rest.Symbols(sctx)
}

// filterSymbols returns a copy of c whose watch pool holds only symbols in
// have (upper-cased, de-duplicated) and whose rule symbol lists are
// upper-cased to match, plus the dropped manual symbols. The auto-selected
// symbols in have that are not manual are appended to the pool and listed in
// Auto; a symbol that is both stays manual. c is not modified.
func filterSymbols(c *config.Config, have map[string]bool, auto []string) (*config.Config, []string) {
	ok, dropped := binance.ValidateSymbols(c.Symbols, have)
	out := *c
	out.Symbols, out.Auto = ok, nil
	for _, s := range auto {
		if have[s] && !slices.Contains(out.Symbols, s) {
			out.Symbols = append(out.Symbols, s)
			out.Auto = append(out.Auto, s)
		}
	}
	out.Rules = make([]config.Rule, len(c.Rules))
	for i, r := range c.Rules {
		syms := make([]string, len(r.Symbols))
		for j, s := range r.Symbols {
			if config.IsSymbolGroup(s) {
				syms[j] = s
			} else {
				syms[j] = strings.ToUpper(strings.TrimSpace(s))
			}
		}
		r.Symbols = syms
		out.Rules[i] = r
	}
	return &out, dropped
}

func droppedMessage(dropped []string) notify.Message {
	return notify.Message{Title: "币种已忽略", Level: config.LevelWarn,
		Lines: []string{"以下币种不是可交易的 U 本位永续合约，已忽略: " + strings.Join(dropped, ", ")}}
}

// manualOf returns the manual members of the running pool c.
func manualOf(c *config.Config) []string {
	var out []string
	for _, s := range c.Symbols {
		if !slices.Contains(c.Auto, s) {
			out = append(out, s)
		}
	}
	return out
}

func autoList(c *config.Config) string {
	switch {
	case !autoOn(c):
		return "未开启"
	case len(c.Auto) == 0:
		return "暂无"
	}
	return strings.Join(c.Auto, ", ")
}

// startedMessage is sent once to every channel at startup.
func startedMessage(c *config.Config) notify.Message {
	chans := make([]string, 0, len(c.Channels))
	for name := range c.Channels {
		chans = append(chans, name)
	}
	sort.Strings(chans)
	return notify.Message{Title: "traderbot 已启动", Level: config.LevelInfo, Lines: []string{
		"币种: " + strings.Join(manualOf(c), ", "),
		"自动币种: " + autoList(c),
		fmt.Sprintf("规则: %d 条", len(c.Rules)),
		"通知渠道: " + strings.Join(chans, ", "),
		"收到这条说明该渠道可用；重启前已满足的条件不会重复推送。",
	}}
}

// reloadLoop applies each config published by config.Watch, and each auto
// selection from autoLoop merged into the last such config (raw is the one
// loaded at startup): it re-validates the symbols, publishes the result to
// store, resubscribes the stream and updates the health monitor when the
// pool changed. Manual symbols are never removed by an auto selection. Notification channels,
// the database DSN and the Binance URLs are fixed at startup: a change to
// them is logged and takes effect only after a restart.
func reloadLoop(ctx context.Context, sub <-chan *config.Config, autoIn <-chan []string, raw *config.Config, store *config.Store,
	refresh func(context.Context) (map[string]bool, error), have map[string]bool,
	mon *health.Monitor, changed chan<- struct{}, log *slog.Logger) {
	running := store.Current().Channels // the dispatcher was built from these and never changes
	auto := store.Current().Auto
	for {
		var c *config.Config
		fromAuto := false
		select {
		case <-ctx.Done():
			return
		case c = <-sub:
			raw = c
		case auto = <-autoIn:
			c, fromAuto = raw, true
		}
		if fresh, err := refresh(ctx); err == nil {
			have = fresh
		} else if ctx.Err() == nil {
			log.Warn("config reload: exchangeInfo failed, validating symbols against the cached list", "err", err)
		}
		next, dropped := filterSymbols(c, have, auto)
		if len(next.Symbols) == 0 {
			log.Error("config reload: no tradable symbol left, keeping the previous config", "symbols", c.Symbols)
			mon.Alert(invalidConfigMessage("热加载后的配置中没有可交易的币种，继续使用旧配置"))
			continue
		}
		// I5: the dispatcher cannot gain channels without a restart, so a rule
		// pointing at a channel it does not have would silently lose signals.
		if missing := missingChannels(next, running); len(missing) > 0 {
			log.Error("config reload: rules reference channels that are not running, keeping the previous config", "channels", missing)
			mon.Alert(invalidConfigMessage("规则引用了未启动的通知渠道（新增渠道需要重启）: " + strings.Join(missing, ", ") + "，继续使用旧配置"))
			continue
		}
		if len(dropped) > 0 && !fromAuto {
			log.Warn("config reload: ignoring symbols that are not tradable USDT perpetuals", "dropped", dropped)
			mon.Alert(droppedMessage(dropped)) // rate-limited: reloads can be frequent
		}
		prev := store.Current()
		if !reflect.DeepEqual(prev.Channels, next.Channels) {
			log.Warn("config reload: notification channel changes take effect only after a restart")
			if !reflect.DeepEqual(running, next.Channels) {
				mon.Alert(notify.Message{Title: "通知渠道变更需重启", Level: config.LevelWarn,
					Lines: []string{"配置里的通知渠道（channels）改了，重启后才生效；当前仍使用启动时的渠道"}})
			}
		}
		if prev.Database != next.Database || prev.Binance != next.Binance {
			log.Warn("config reload: database/binance changes take effect only after a restart")
		}
		store.Set(next)
		if !slices.Equal(prev.Symbols, next.Symbols) {
			mon.SetSymbols(next.Symbols)
			select {
			case changed <- struct{}{}:
			default:
			}
		}
		if fromAuto {
			log.Info("auto symbols applied", "symbols", next.Symbols, "auto", next.Auto)
			continue // autoLoop already announced the change
		}
		log.Info("config reloaded", "symbols", next.Symbols, "rules", len(next.Rules))
		mon.Alert(notify.Message{Title: "配置已生效", Level: config.LevelInfo,
			Lines: []string{fmt.Sprintf("币种 %d 个，规则 %d 条", len(next.Symbols), len(next.Rules))}})
	}
}

// missingChannels lists channels referenced by next's rules that are not in
// running (sorted, deduplicated).
func missingChannels(next *config.Config, running map[string]config.ChannelConfig) []string {
	var out []string
	for _, r := range next.Rules {
		for _, ch := range r.Channels {
			if _, ok := running[ch]; !ok && !slices.Contains(out, ch) {
				out = append(out, ch)
			}
		}
	}
	sort.Strings(out)
	return out
}

// invalidConfigMessage builds a 配置无效 alert. The title carries a short hash
// of the reason so a different bad edit is not swallowed by the per-title
// rate limit of the previous one.
func invalidConfigMessage(reason string) notify.Message {
	sum := sha256.Sum256([]byte(reason))
	return notify.Message{Title: "配置无效 #" + hex.EncodeToString(sum[:3]), Level: config.LevelCritical,
		Lines: []string{reason}}
}

// warmupPlan returns how many bars to fetch per interval: warmupBars for every
// configured interval, and enough 1m bars to rebuild the current bucket of
// the largest interval (largest/1m, capped at maxWarmup1m) (R15).
func warmupPlan(ivs []market.Interval) map[market.Interval]int {
	plan := map[market.Interval]int{}
	var largest time.Duration
	for _, iv := range ivs {
		if iv.Valid() {
			plan[iv] = warmupBars
			largest = max(largest, iv.Duration())
		}
	}
	if n := min(int(largest/time.Minute), maxWarmup1m); n > plan[market.I1m] {
		plan[market.I1m] = n
	}
	return plan
}

// lastKlineStore is implemented by *storage.Store.
type lastKlineStore interface {
	LastKlineTime(ctx context.Context, symbol string, iv market.Interval) (time.Time, bool, error)
}

// lastKlineTimeout bounds the lookup done before persisting warmup bars.
const lastKlineTimeout = 5 * time.Second

// unstored returns the bars of ks (one symbol and interval) newer than the
// newest one already stored. Warmup re-fetches hundreds of bars per interval
// on every restart; writing them all again floods the writer (dropping data)
// and rewrites history that TimescaleDB may already have compressed. If st
// cannot tell, or the lookup fails, every bar is returned.
func unstored(ctx context.Context, st Store, ks []market.Kline, log *slog.Logger) []market.Kline {
	ls, ok := st.(lastKlineStore)
	if !ok || len(ks) == 0 {
		return ks
	}
	lctx, cancel := context.WithTimeout(ctx, lastKlineTimeout)
	defer cancel()
	last, found, err := ls.LastKlineTime(lctx, ks[0].Symbol, ks[0].Interval)
	if err != nil {
		log.Warn("warmup: latest stored bar unknown, persisting all fetched bars", "symbol", ks[0].Symbol, "interval", ks[0].Interval, "err", err)
		return ks
	}
	if !found {
		return ks
	}
	i := sort.Search(len(ks), func(i int) bool { return ks[i].OpenTime.After(last) })
	return ks[i:]
}

// warmup returns the Router's history func: REST bars per warmupPlan, bounded
// by warmupTimeout. The 1m bars are fetched first and seed the stream's gap
// detection with the last closed bar. A failure alerts ("预热失败: SYMBOL");
// on timeout the bars fetched so far are returned, otherwise the error (the
// worker then starts cold). A shutdown neither alerts nor logs.
//
// persist, if set, receives the fetched bars so the database also holds them
// (I7; klines are upserted, so overlaps with live bars are harmless).
func warmup(rest *binance.REST, stream *binance.Stream, store *config.Store, clk clock.Clock,
	alert func(notify.Message), persist func([]market.Kline), log *slog.Logger) engine.HistoryFunc {
	return func(parent context.Context, symbol string) (out map[market.Interval][]market.Kline, err error) {
		defer func() {
			if persist != nil && err == nil {
				for _, ks := range out {
					persist(ks)
				}
			}
		}()
		ctx, cancel := context.WithTimeout(parent, warmupTimeout)
		defer cancel()
		plan := warmupPlan(store.Current().Intervals)
		ivs := make([]market.Interval, 0, len(plan))
		for iv := range plan {
			ivs = append(ivs, iv)
		}
		sort.Slice(ivs, func(i, j int) bool { return ivs[i].Duration() < ivs[j].Duration() })

		now := clk.Now()
		out = make(map[market.Interval][]market.Kline, len(plan))
		total := 0
		for _, iv := range ivs {
			n, d := plan[iv], iv.Duration()
			start := now.Truncate(d).Add(-time.Duration(n) * d)
			ks, err := rest.Klines(ctx, symbol, iv, start, n)
			if err != nil {
				if parent.Err() != nil {
					return nil, err
				}
				err = fmt.Errorf("warmup %s %s: %w", symbol, iv, err)
				timedOut := ctx.Err() != nil
				alert(notify.Message{Title: "预热失败: " + symbol, Level: config.LevelWarn,
					Lines: []string{warmupFailText(timedOut, out), err.Error()}})
				if timedOut {
					log.Warn("warmup timed out, using the bars fetched so far", "symbol", symbol, "bars", total, "err", err)
					return out, nil
				}
				return nil, err
			}
			out[iv] = ks
			total += len(ks)
			if iv == market.I1m {
				for i := len(ks) - 1; i >= 0; i-- {
					if ks[i].Closed {
						stream.Seed(symbol, ks[i].OpenTime)
						break
					}
				}
			}
		}
		log.Info("warmup done", "symbol", symbol, "bars", total)
		return out, nil
	}
}

func warmupFailText(timedOut bool, got map[market.Interval][]market.Kline) string {
	if !timedOut {
		return "历史 K 线拉取失败，该币种冷启动：指标要等实时 K 线积累后才可用，期间相关规则不会触发"
	}
	var ivs []string
	for iv := range got {
		ivs = append(ivs, string(iv))
	}
	sort.Strings(ivs)
	return "历史 K 线拉取超时，仅使用已拉到的周期 " + fmt.Sprint(ivs) + "，其余周期要等实时 K 线积累后才可用"
}

// fanOut forwards every market event to the Router (never dropped; a full
// router input blocks and is counted), the Writer and the health Monitor.
// It must keep draining in, or the Binance stream stalls.
func fanOut(ctx context.Context, in <-chan market.Event, router chan<- market.Event, writer chan<- any,
	mon *health.Monitor, blocked *atomic.Int64) {
	for {
		var ev market.Event
		select {
		case <-ctx.Done():
			return
		case ev = <-in:
		}
		mon.OnEvent(ev.EventSymbol())
		select {
		case router <- ev:
		default:
			blocked.Add(1)
			select {
			case router <- ev:
			case <-ctx.Done():
				return
			}
		}
		select {
		case writer <- ev:
		case <-ctx.Done():
			return
		}
	}
}

// logStats logs a warning whenever the event path had to wait since the last
// check (router input full, or a worker's channel full).
func logStats(ctx context.Context, clk clock.Clock, router *engine.Router, fanBlocked *atomic.Int64, log *slog.Logger) {
	var lastRouter, lastFan int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-clk.After(statsEvery):
		}
		r, f := router.Backpressure(), fanBlocked.Load()
		if r > lastRouter || f > lastFan {
			log.Warn("event pipeline backpressure", "worker_waits", r-lastRouter, "router_input_waits", f-lastFan,
				"worker_waits_total", r, "router_input_waits_total", f)
		}
		lastRouter, lastFan = r, f
	}
}

// failingSincer is the part of storage.Writer watchWriter needs.
type failingSincer interface{ FailingSince() time.Time }

// watchWriter raises one critical 数据库写入失败 once database writes have
// failed continuously for dbFailingAfter, and one info 数据库写入恢复 when they
// succeed again (I6).
func watchWriter(ctx context.Context, clk clock.Clock, w failingSincer, alert func(notify.Message), log *slog.Logger) {
	alerted := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-clk.After(dbCheckEvery):
		}
		since := w.FailingSince()
		switch {
		case !alerted && !since.IsZero() && clk.Now().Sub(since) >= dbFailingAfter:
			alerted = true
			log.Error("database writes keep failing", "since", since)
			alert(notify.Message{Title: "数据库写入失败", Level: config.LevelCritical, Lines: []string{
				fmt.Sprintf("从 %s 起数据库写入一直失败；数据暂存在内存（最多 1 万条，超出丢最旧的），重启会丢失", since.UTC().Format("15:04:05 UTC")),
			}})
		case alerted && since.IsZero():
			alerted = false
			log.Info("database writes recovered")
			alert(notify.Message{Title: "数据库写入恢复", Level: config.LevelInfo, Lines: []string{"数据库写入已恢复"}})
		}
	}
}

// batchWarmupAlerts wraps alert so that 预热失败 alerts arriving within window
// of the first one go out as a single message (I8); other alerts pass
// through. A lone failure keeps its per-symbol message.
func batchWarmupAlerts(ctx context.Context, clk clock.Clock, window time.Duration, alert func(notify.Message)) func(notify.Message) {
	const prefix = "预热失败: "
	var (
		mu      sync.Mutex
		pending []notify.Message
	)
	flush := func() {
		mu.Lock()
		ms := pending
		pending = nil
		mu.Unlock()
		switch len(ms) {
		case 0:
			return
		case 1:
			alert(ms[0])
			return
		}
		syms := make([]string, 0, len(ms))
		for _, m := range ms {
			syms = append(syms, strings.TrimPrefix(m.Title, prefix))
		}
		listed := strings.Join(syms[:min(len(syms), maxListedSymbols)], ", ")
		if len(syms) > maxListedSymbols {
			listed += " …"
		}
		alert(notify.Message{Title: fmt.Sprintf("预热失败: %d 个币种", len(ms)), Level: config.LevelWarn, Lines: []string{
			"以下币种的历史 K 线拉取失败或超时，相关周期要等实时 K 线积累后才可用: " + listed,
			ms[0].Lines[len(ms[0].Lines)-1], // one example error
		}})
	}
	return func(m notify.Message) {
		if !strings.HasPrefix(m.Title, prefix) {
			alert(m)
			return
		}
		mu.Lock()
		pending = append(pending, m)
		first := len(pending) == 1
		mu.Unlock()
		if first {
			go func() {
				select {
				case <-clk.After(window):
					flush()
				case <-ctx.Done():
				}
			}()
		}
	}
}

func registerMetrics(mon *health.Monitor, router *engine.Router, writer *storage.Writer, disp *notify.Dispatcher, cfg *config.Config) {
	mon.RegisterCounterFunc("traderbot_router_backpressure_total",
		"Times a market event waited for a full per-symbol worker channel.",
		func() float64 { return float64(router.Backpressure()) })
	mon.Registry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "traderbot_writer_failing",
		Help: "1 while database writes are failing continuously.",
	}, func() float64 {
		if writer.FailingSince().IsZero() {
			return 0
		}
		return 1
	}))
	mon.RegisterCounterFunc("traderbot_writer_dropped_total",
		"Items dropped by the storage writer because its buffer was full.",
		func() float64 { return float64(writer.Dropped()) })
	names := make([]string, 0, len(cfg.Channels))
	for name := range cfg.Channels {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		mon.Registry().MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "traderbot_dispatcher_dropped_total",
			Help:        "Notifications dropped because the channel queue was full.",
			ConstLabels: prometheus.Labels{"channel": name},
		}, func() float64 { return float64(disp.Dropped(name)) }))
		mon.Registry().MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "traderbot_notify_failures_total",
			Help:        "Notifications that failed on every attempt.",
			ConstLabels: prometheus.Labels{"channel": name},
		}, func() float64 { return float64(disp.Failures(name)) }))
	}
	mon.SetFailingChannels(disp.Failing)
}

// healthcheckURL turns a listen address into the local /healthz URL.
func healthcheckURL(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}

// probe is the -healthcheck mode: 0 if /healthz answers 200, else 1.
func probe(addr string) int {
	u, err := healthcheckURL(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(u)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}

// discardStore is the -no-db sink: every write succeeds and is thrown away.
type discardStore struct{}

func (discardStore) WriteKlines(context.Context, []market.Kline) error                  { return nil }
func (discardStore) WriteMarkPrices(context.Context, []market.MarkPriceEvent) error     { return nil }
func (discardStore) WriteOI(context.Context, []market.OpenInterestEvent) error          { return nil }
func (discardStore) WriteLongShort(context.Context, []market.LongShortEvent) error      { return nil }
func (discardStore) WriteLiquidations(context.Context, []market.LiquidationEvent) error { return nil }
func (discardStore) WriteSignals(context.Context, []signal.Signal) error                { return nil }
func (discardStore) Close()                                                             {}
