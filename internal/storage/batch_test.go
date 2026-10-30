package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

var t0 = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// fakeSink records every write. If gate is non-nil each call blocks until it
// is closed (or the call ctx ends). failSignals makes the first N
// WriteSignals calls fail.
type fakeSink struct {
	mu          sync.Mutex
	klines      []market.Kline
	marks       []market.MarkPriceEvent
	ois         []market.OpenInterestEvent
	lss         []market.LongShortEvent
	liqs        []market.LiquidationEvent
	sigs        []signal.Signal
	sigCalls    int
	calls       int
	ctxErrs     []error
	failSignals int
	gate        chan struct{}
}

func (f *fakeSink) enter(ctx context.Context) {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	f.calls++
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	f.mu.Unlock()
}

func (f *fakeSink) WriteKlines(ctx context.Context, ks []market.Kline) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.klines = append(f.klines, ks...)
	return nil
}

func (f *fakeSink) WriteMarkPrices(ctx context.Context, ms []market.MarkPriceEvent) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, ms...)
	return nil
}

func (f *fakeSink) WriteOI(ctx context.Context, os []market.OpenInterestEvent) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ois = append(f.ois, os...)
	return nil
}

func (f *fakeSink) WriteLongShort(ctx context.Context, ls []market.LongShortEvent) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lss = append(f.lss, ls...)
	return nil
}

func (f *fakeSink) WriteLiquidations(ctx context.Context, ls []market.LiquidationEvent) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.liqs = append(f.liqs, ls...)
	return nil
}

func (f *fakeSink) WriteSignals(ctx context.Context, ss []signal.Signal) error {
	f.enter(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sigCalls++
	if f.failSignals > 0 {
		f.failSignals--
		return errors.New("db down")
	}
	f.sigs = append(f.sigs, ss...)
	return nil
}

func (f *fakeSink) snapshotSigs() []signal.Signal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]signal.Signal(nil), f.sigs...)
}

func (f *fakeSink) sigCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sigCalls
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func sig(i int) signal.Signal {
	return signal.Signal{RuleID: "r", Symbol: "BTCUSDT", Time: t0.Add(time.Duration(i) * time.Second), Price: float64(i)}
}

type runner struct {
	w      *Writer
	in     chan any
	cancel context.CancelFunc
	done   chan error
}

func start(t *testing.T, s sink, batchSize int, flushEvery time.Duration, clk clock.Clock) *runner {
	t.Helper()
	w := newWriter(s, batchSize, flushEvery)
	w.Clock = clk
	r := &runner{w: w, in: make(chan any), done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.done <- w.Run(ctx, r.in) }()
	t.Cleanup(func() { cancel(); <-r.done })
	return r
}

func (r *runner) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		r.done <- nil // let Cleanup drain
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestWriterUnit_FlushOnSize(t *testing.T) {
	fs := &fakeSink{}
	r := start(t, fs, 3, time.Hour, clock.NewFake(t0))
	r.in <- sig(1)
	r.in <- market.KlineEvent{Kline: market.Kline{Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: t0, Close: 1, Closed: true}}
	if n := fs.sigCallCount(); n != 0 {
		t.Fatalf("flushed before batchSize reached (%d signal calls)", n)
	}
	r.in <- sig(2)
	eventually(t, "size flush", func() bool { return len(fs.snapshotSigs()) == 2 })
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.klines) != 1 {
		t.Fatalf("klines written = %d, want 1", len(fs.klines))
	}
}

func TestWriterUnit_FlushOnInterval(t *testing.T) {
	fs := &fakeSink{}
	clk := clock.NewFake(t0)
	r := start(t, fs, 500, 2*time.Second, clk)
	r.in <- sig(1) // unbuffered: once received, Run's ticker is armed
	clk.Advance(1999 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if n := fs.sigCallCount(); n != 0 {
		t.Fatalf("flushed before interval (%d calls)", n)
	}
	clk.Advance(time.Millisecond)
	eventually(t, "interval flush", func() bool { return len(fs.snapshotSigs()) == 1 })

	// The ticker re-arms.
	r.in <- sig(2)
	eventually(t, "second interval flush", func() bool {
		clk.Advance(2 * time.Second)
		return len(fs.snapshotSigs()) == 2
	})
}

func TestWriterUnit_UnclosedKlinesFiltered(t *testing.T) {
	fs := &fakeSink{}
	r := start(t, fs, 500, time.Hour, clock.NewFake(t0))
	k := market.Kline{Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: t0, Close: 1}
	r.in <- market.KlineEvent{Kline: k}
	k.Close, k.Closed = 2, true
	r.in <- market.KlineEvent{Kline: k}
	k.OpenTime, k.Close, k.Closed = t0.Add(time.Minute), 3, false
	r.in <- market.KlineEvent{Kline: k}
	r.in <- "unknown type"
	r.in <- 42
	r.stop(t)
	if len(fs.klines) != 1 || fs.klines[0].Close != 2 {
		t.Fatalf("klines = %+v, want only the closed bar", fs.klines)
	}
}

func TestWriterUnit_MarkPriceDownsampled(t *testing.T) {
	fs := &fakeSink{}
	r := start(t, fs, 500, time.Hour, clock.NewFake(t0))
	mp := func(sym string, off time.Duration) market.MarkPriceEvent {
		return market.MarkPriceEvent{Symbol: sym, Time: t0.Add(off), MarkPrice: off.Seconds()}
	}
	for _, e := range []market.MarkPriceEvent{
		mp("BTCUSDT", 5*time.Second),
		mp("BTCUSDT", 30*time.Second),
		mp("ETHUSDT", 30*time.Second),
		mp("BTCUSDT", 62*time.Second),
		mp("BTCUSDT", 119*time.Second),
		mp("BTCUSDT", 120*time.Second),
		mp("ETHUSDT", 59*time.Second),
	} {
		r.in <- e
	}
	r.stop(t)
	var got []string
	for _, m := range fs.marks {
		got = append(got, m.Symbol+"@"+m.Time.Sub(t0).String())
	}
	want := "BTCUSDT@5s ETHUSDT@30s BTCUSDT@1m2s BTCUSDT@2m0s"
	if strings.Join(got, " ") != want {
		t.Fatalf("marks = %v, want %s", got, want)
	}
}

func TestWriterUnit_OverflowDropsOldest(t *testing.T) {
	fs := &fakeSink{}
	r := start(t, fs, 1_000_000, time.Hour, clock.NewFake(t0))
	var mu sync.Mutex
	var hook int
	r.w.OnDrop = func(n int) { mu.Lock(); hook += n; mu.Unlock() }
	for i := 0; i < maxBuffered+5; i++ {
		r.in <- sig(i)
	}
	eventually(t, "drop count", func() bool { return r.w.Dropped() == 5 })
	r.stop(t)
	if len(fs.sigs) != maxBuffered {
		t.Fatalf("written = %d, want %d", len(fs.sigs), maxBuffered)
	}
	if fs.sigs[0].Price != 5 || fs.sigs[len(fs.sigs)-1].Price != float64(maxBuffered+4) {
		t.Fatalf("kept range %v..%v, want 5..%d", fs.sigs[0].Price, fs.sigs[len(fs.sigs)-1].Price, maxBuffered+4)
	}
	mu.Lock()
	defer mu.Unlock()
	if hook != 5 {
		t.Fatalf("OnDrop total = %d, want 5", hook)
	}
}

func TestWriterUnit_FailedFlushRetried(t *testing.T) {
	fs := &fakeSink{failSignals: 1}
	clk := clock.NewFake(t0)
	r := start(t, fs, 2, time.Hour, clk)
	r.in <- sig(1)
	r.in <- sig(2) // size flush, fails
	eventually(t, "first (failing) flush", func() bool { return fs.sigCallCount() == 1 })
	r.in <- sig(3)
	// A failure suppresses size-triggered retries until the next tick, so
	// a down DB is not hammered; the tick retries everything in order.
	eventually(t, "retry flush", func() bool {
		clk.Advance(time.Hour)
		return len(fs.snapshotSigs()) == 3
	})
	got := fs.snapshotSigs()
	for i, s := range got {
		if s.Price != float64(i+1) {
			t.Fatalf("retried order = %+v", got)
		}
	}
	if r.w.Dropped() != 0 {
		t.Fatalf("Dropped = %d, want 0", r.w.Dropped())
	}
}

func TestWriterUnit_FinalFlushOnCancel(t *testing.T) {
	fs := &fakeSink{}
	r := start(t, fs, 500, time.Hour, clock.NewFake(t0))
	r.in <- sig(1)
	r.in <- market.OpenInterestEvent{Symbol: "BTCUSDT", Time: t0, OI: 1}
	r.in <- market.LongShortEvent{Symbol: "BTCUSDT", Time: t0, Kind: "global", Ratio: 1}
	r.in <- market.LiquidationEvent{Symbol: "BTCUSDT", Time: t0, Side: "SELL", Price: 1, Qty: 2}
	r.stop(t)
	if len(fs.sigs) != 1 || len(fs.ois) != 1 || len(fs.lss) != 1 || len(fs.liqs) != 1 {
		t.Fatalf("final flush incomplete: sigs=%d ois=%d lss=%d liqs=%d", len(fs.sigs), len(fs.ois), len(fs.lss), len(fs.liqs))
	}
	for _, err := range fs.ctxErrs {
		if err != nil {
			t.Fatalf("final flush ran with a dead ctx: %v", err)
		}
	}
}

func TestWriterUnit_SlowSinkDoesNotBlockReceive(t *testing.T) {
	fs := &fakeSink{gate: make(chan struct{})}
	r := start(t, fs, 1, time.Hour, clock.NewFake(t0))
	const n = 2000
	sent := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			r.in <- sig(i)
		}
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream blocked by a slow sink")
	}
	if c := fs.sigCallCount(); c != 0 {
		t.Fatalf("sink finished %d calls while gated", c)
	}
	close(fs.gate)
	eventually(t, "all written", func() bool { return len(fs.snapshotSigs()) == n })
}

func TestStripTimescale(t *testing.T) {
	in := "A;\n-- timescale:begin\nCREATE EXTENSION x;\n-- timescale:end\nB;\n"
	if got := string(stripTimescale([]byte(in))); got != "A;\nB;\n" {
		t.Fatalf("stripTimescale = %q", got)
	}
	up, err := migrationsFS.ReadFile("migrations/0001_init.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(up), "create_hypertable") || !strings.Contains(string(up), "add_compression_policy") {
		t.Fatal("production migration lost its Timescale statements")
	}
	if s := string(stripTimescale(up)); strings.Contains(strings.ToLower(s), "timescaledb") || strings.Contains(s, "hypertable") {
		t.Fatalf("stripped migration still references Timescale:\n%s", s)
	}
}
