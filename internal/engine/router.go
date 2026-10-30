package engine

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

// workerBuffer is each worker's event channel capacity.
const workerBuffer = 1024

// HistoryFunc returns warmup bars for a newly started symbol.
type HistoryFunc func(ctx context.Context, symbol string) (map[market.Interval][]market.Kline, error)

// Router runs one Worker per watched symbol, routes events to them by symbol
// and fans config snapshots out. Its receive loop never does slow work, so
// the upstream stream is drained promptly; it blocks (never drops) only when
// a worker's channel is full.
type Router struct {
	store   *config.Store
	gate    *signal.Gate
	out     chan<- signal.Signal
	clk     clock.Clock
	history HistoryFunc

	// Logger defaults to slog.Default().
	Logger *slog.Logger

	backpressure atomic.Int64

	mu      sync.Mutex // guards workers for running(); mutated only by Run
	workers map[string]*workerHandle
}

type workerHandle struct {
	in     chan market.Event
	cfgs   chan *config.Config
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRouter creates a router. history may be nil (workers start cold).
func NewRouter(store *config.Store, gate *signal.Gate, out chan<- signal.Signal, clk clock.Clock,
	history func(ctx context.Context, symbol string) (map[market.Interval][]market.Kline, error)) *Router {
	return &Router{store: store, gate: gate, out: out, clk: clk, history: history, workers: map[string]*workerHandle{}}
}

func (r *Router) log() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// Backpressure is the number of times an event had to wait for a full worker
// channel (worker_backpressure_total).
func (r *Router) Backpressure() int64 { return r.backpressure.Load() }

// Run routes in until ctx is cancelled or in is closed, then stops every
// worker and returns nil.
func (r *Router) Run(ctx context.Context, in <-chan market.Event) error {
	sub := r.store.Subscribe()
	defer r.stopAll()
	r.reconcile(ctx, r.store.Current())
	for {
		select {
		case <-ctx.Done():
			return nil
		case c := <-sub:
			r.reconcile(ctx, c)
		case ev, ok := <-in:
			if !ok {
				return nil
			}
			r.route(ctx, ev)
		}
	}
}

func (r *Router) route(ctx context.Context, ev market.Event) {
	h := r.workers[ev.EventSymbol()]
	if h == nil {
		return // not a watched symbol
	}
	select {
	case h.in <- ev:
		return
	default:
	}
	r.backpressure.Add(1)
	select {
	case h.in <- ev:
	case <-ctx.Done():
	}
}

// reconcile stops workers for removed symbols, starts new ones and hands the
// snapshot to the rest (latest wins).
func (r *Router) reconcile(ctx context.Context, cfg *config.Config) {
	if cfg == nil {
		return
	}
	want := map[string]bool{}
	for _, s := range cfg.Symbols {
		want[s] = true
	}
	for s := range r.workers {
		if !want[s] {
			r.stop(s)
		}
	}
	for _, s := range cfg.Symbols {
		if h := r.workers[s]; h != nil {
			offer(h.cfgs, cfg)
		} else {
			r.start(ctx, s, cfg)
		}
	}
}

// offer puts c in a buffer-1 channel, replacing any pending snapshot. Only
// the router sends on ch, so the loop terminates.
func offer(ch chan *config.Config, c *config.Config) {
	for {
		select {
		case ch <- c:
			return
		default:
			select {
			case <-ch:
			default:
			}
		}
	}
}

func (r *Router) start(ctx context.Context, symbol string, cfg *config.Config) {
	wctx, cancel := context.WithCancel(ctx)
	h := &workerHandle{
		in:     make(chan market.Event, workerBuffer),
		cfgs:   make(chan *config.Config, 1),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	w := NewWorker(symbol, cfg, r.gate, r.out, r.clk)
	w.log = r.log().With("symbol", symbol)
	go func() {
		defer close(h.done)
		// Warmup runs here, not in Run's loop: events buffer in h.in meanwhile.
		if r.history != nil {
			hist, err := r.history(wctx, symbol)
			switch {
			case wctx.Err() != nil:
				return
			case err != nil:
				w.log.Warn("engine: warmup history failed; starting cold", "err", err)
			default:
				w.Warmup(hist)
			}
		}
		_ = w.Run(wctx, h.in, h.cfgs)
	}()
	r.mu.Lock()
	r.workers[symbol] = h
	r.mu.Unlock()
}

func (r *Router) stop(symbol string) {
	h := r.workers[symbol]
	h.cancel()
	close(h.in)
	<-h.done
	r.gate.ForgetSymbol(symbol)
	r.mu.Lock()
	delete(r.workers, symbol)
	r.mu.Unlock()
}

func (r *Router) stopAll() {
	for s := range r.workers {
		r.stop(s)
	}
}

// running returns the symbols with a live worker, sorted.
func (r *Router) running() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.workers))
	for s := range r.workers {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}
