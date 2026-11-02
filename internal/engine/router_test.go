package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

func parseSyms(t *testing.T, syms string, rules ...string) *config.Config {
	t.Helper()
	y := "symbols: " + syms + "\nchannels:\n  tg: {type: telegram}\nrules:\n"
	for _, r := range rules {
		y += "  - " + r + "\n"
	}
	c, err := config.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const upRule = `{id: up, interval: 1m, when: {all: ["close > 100"]}, channels: [tg]}`

type routerHarness struct {
	r      *Router
	store  *config.Store
	out    chan signal.Signal
	in     chan market.Event
	cancel context.CancelFunc
	done   chan error

	mu      sync.Mutex
	history []string
}

func newRouterHarness(t *testing.T, cfg *config.Config, hist func(ctx context.Context, s string) (map[market.Interval][]market.Kline, error)) *routerHarness {
	t.Helper()
	clk := clock.NewFake(t0.Add(24 * time.Hour))
	h := &routerHarness{store: config.NewStore(cfg), out: make(chan signal.Signal, 64), in: make(chan market.Event), done: make(chan error, 1)}
	wrapped := func(ctx context.Context, s string) (map[market.Interval][]market.Kline, error) {
		h.mu.Lock()
		h.history = append(h.history, s)
		h.mu.Unlock()
		if hist == nil {
			return nil, nil
		}
		return hist(ctx, s)
	}
	h.r = NewRouter(h.store, signal.NewGate(clk), h.out, clk, wrapped)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.r.Run(ctx, h.in) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("router did not stop")
		}
	})
	return h
}

func (h *routerHarness) waitRunning(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := h.r.running()
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("running=%v want %v", got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (h *routerHarness) expect(t *testing.T, symbol string) signal.Signal {
	t.Helper()
	select {
	case s := <-h.out:
		if s.Symbol != symbol {
			t.Fatalf("signal for %s, want %s: %+v", s.Symbol, symbol, s)
		}
		return s
	case <-time.After(5 * time.Second):
		t.Fatalf("no signal for %s", symbol)
	}
	return signal.Signal{}
}

func TestRouter_StartsAndStopsWorkers(t *testing.T) {
	h := newRouterHarness(t, parseSyms(t, "[BTCUSDT]", upRule), nil)
	h.waitRunning(t, "BTCUSDT")

	h.in <- k1("ETHUSDT", 0, 105, 1, true) // unknown symbol: ignored
	h.in <- k1("BTCUSDT", 0, 105, 1, true)
	h.expect(t, "BTCUSDT")

	// Swap the pool: BTC stopped (gate state forgotten), ETH started.
	h.store.Set(parseSyms(t, "[ETHUSDT]", upRule))
	h.waitRunning(t, "ETHUSDT")
	h.in <- k1("ETHUSDT", 1, 105, 1, true)
	h.expect(t, "ETHUSDT")

	// Re-adding BTC: still-True rule fires again since ForgetSymbol cleared its edge state.
	h.store.Set(parseSyms(t, "[BTCUSDT, ETHUSDT]", upRule))
	h.waitRunning(t, "BTCUSDT", "ETHUSDT")
	h.in <- k1("BTCUSDT", 2, 105, 1, true)
	h.expect(t, "BTCUSDT")

	h.mu.Lock()
	hist := slices.Clone(h.history)
	h.mu.Unlock()
	if !slices.Equal(hist, []string{"BTCUSDT", "ETHUSDT", "BTCUSDT"}) {
		t.Fatalf("history calls=%v", hist)
	}

	// Closing in stops the router and every worker.
	close(h.in)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.done <- nil // for cleanup
	case <-time.After(5 * time.Second):
		t.Fatal("router did not return after in closed")
	}
	if got := h.r.running(); len(got) != 0 {
		t.Fatalf("workers still running: %v", got)
	}
}

func TestRouter_FansOutConfig(t *testing.T) {
	h := newRouterHarness(t, parseSyms(t, "[BTCUSDT, ETHUSDT]", `{id: never, interval: 1m, when: {all: ["close > 1000"]}, channels: [tg]}`), nil)
	h.waitRunning(t, "BTCUSDT", "ETHUSDT")
	h.store.Set(parseSyms(t, "[BTCUSDT, ETHUSDT]", upRule))
	// Workers apply the new cfg asynchronously; alternate bars until both fire.
	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for i := 0; len(seen) < 2; i++ {
		for _, s := range []string{"BTCUSDT", "ETHUSDT"} {
			if !seen[s] {
				h.in <- k1(s, 2*i, 90, 1, true)
				h.in <- k1(s, 2*i+1, 105, 1, true)
			}
		}
		select {
		case s := <-h.out:
			seen[s.Symbol] = true
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatalf("seen=%v", seen)
		}
	}
}

func TestRouter_WarmupOffLoopAndBackpressure(t *testing.T) {
	release := make(chan struct{})
	hist := func(ctx context.Context, s string) (map[market.Interval][]market.Kline, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return map[market.Interval][]market.Kline{market.I1m: {
			{Symbol: s, Interval: market.I1m, OpenTime: t0, Close: 90, Closed: true},
		}}, nil
	}
	h := newRouterHarness(t, parseSyms(t, "[BTCUSDT]", upRule), hist)
	h.waitRunning(t, "BTCUSDT")
	// Warmup is blocked; the router still accepts a full buffer of events.
	for i := 0; i < workerBuffer; i++ {
		select {
		case h.in <- k1("BTCUSDT", 1+i, 90, 1, true):
		case <-time.After(5 * time.Second):
			t.Fatalf("router blocked at event %d while warmup pending", i)
		}
	}
	if got := h.r.Backpressure(); got != 0 {
		t.Fatalf("backpressure=%d before overflow", got)
	}
	sent := make(chan struct{})
	go func() {
		h.in <- k1("BTCUSDT", 1+workerBuffer, 90, 1, true)
		h.in <- k1("BTCUSDT", 2+workerBuffer, 105, 1, true)
		close(sent)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.r.Backpressure() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("backpressure never counted")
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(release)
	<-sent
	// Nothing was dropped: the last (True) bar arrives after all False ones.
	s := h.expect(t, "BTCUSDT")
	if s.Price != 105 {
		t.Fatalf("signal=%+v", s)
	}
}

func TestRouter_HistoryErrorStartsCold(t *testing.T) {
	hist := func(ctx context.Context, s string) (map[market.Interval][]market.Kline, error) {
		return nil, errors.New("rest down")
	}
	h := newRouterHarness(t, parseSyms(t, "[BTCUSDT]", upRule), hist)
	h.waitRunning(t, "BTCUSDT")
	h.in <- k1("BTCUSDT", 0, 105, 1, true)
	h.expect(t, "BTCUSDT")
}
