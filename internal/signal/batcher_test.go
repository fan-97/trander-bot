package signal

import (
	"context"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
)

const waitTimeout = 2 * time.Second

// noonUTC is outside any quiet window used below.
var noonUTC = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// harness drives a Batcher running in its own goroutine with a Fake clock.
//
// Flakiness: send() blocks until Run has fully processed the signal (via the
// unexported afterSignal hook), so any clk.After registered for it exists
// before the test calls Advance. After an Advance, tests wait for the
// observable result of the flush (a Batch or a record) before sending more.
// Run additionally flushes expired windows on every arrival, so a signal can
// never join a window whose deadline has already passed.
type harness struct {
	t       *testing.T
	clk     *clock.Fake
	mu      sync.Mutex
	nc      config.NotifyConfig
	in      chan Signal
	out     chan Batch
	recs    chan Signal
	stepped chan struct{}
	cancel  context.CancelFunc
	done    chan error
}

func newHarness(t *testing.T, start time.Time, nc config.NotifyConfig) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		clk:     clock.NewFake(start),
		nc:      nc,
		in:      make(chan Signal),
		out:     make(chan Batch),
		recs:    make(chan Signal, 100),
		stepped: make(chan struct{}),
		done:    make(chan error, 1),
	}
	b := NewBatcher(h.clk, h.cfg, h.out, func(s Signal) { h.recs <- s })
	b.afterSignal = func() { h.stepped <- struct{}{} }
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- b.Run(ctx, h.in) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(waitTimeout):
			t.Error("Run did not return after cancel")
		}
	})
	return h
}

func (h *harness) cfg() config.NotifyConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nc
}

func (h *harness) setCfg(nc config.NotifyConfig) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nc = nc
}

func (h *harness) send(s Signal) {
	h.t.Helper()
	select {
	case h.in <- s:
	case <-time.After(waitTimeout):
		h.t.Fatalf("timeout sending %s", s.RuleID)
	}
	select {
	case <-h.stepped:
	case <-time.After(waitTimeout):
		h.t.Fatalf("timeout waiting for Run to process %s", s.RuleID)
	}
}

func (h *harness) batch() Batch {
	h.t.Helper()
	select {
	case b := <-h.out:
		return b
	case <-time.After(waitTimeout):
		h.t.Fatal("timeout waiting for batch")
	}
	return Batch{}
}

func (h *harness) record() Signal {
	h.t.Helper()
	select {
	case s := <-h.recs:
		return s
	case <-time.After(waitTimeout):
		h.t.Fatal("timeout waiting for record")
	}
	return Signal{}
}

func (h *harness) noBatch() {
	h.t.Helper()
	select {
	case b := <-h.out:
		h.t.Fatalf("unexpected batch %+v", b)
	default:
	}
}

func (h *harness) noRecord() {
	h.t.Helper()
	select {
	case s := <-h.recs:
		h.t.Fatalf("unexpected record %+v", s)
	default:
	}
}

func sig(id string, lvl config.Level, chans ...string) Signal {
	return Signal{RuleID: id, Symbol: "BTCUSDT", Level: lvl, Channels: chans}
}

func ids(ss []Signal) []string {
	r := make([]string, len(ss))
	for i, s := range ss {
		r[i] = s.RuleID
	}
	return r
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func baseCfg() config.NotifyConfig {
	return config.NotifyConfig{MergeWindow: 10 * time.Second, RatePerMinute: 100, QuietMinLevel: config.LevelCritical}
}

func TestBatcher_MergesWithinWindow(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	h.send(sig("a", config.LevelWarn, "tg"))
	h.clk.Advance(3 * time.Second)
	h.send(sig("b", config.LevelWarn, "tg"))
	h.clk.Advance(3 * time.Second)
	h.send(sig("c", config.LevelInfo, "tg"))
	h.clk.Advance(3 * time.Second) // t=9s: window still open
	h.noBatch()

	h.clk.Advance(1 * time.Second) // t=10s: window closes
	b := h.batch()
	if b.Channel != "tg" || !eqStrings(ids(b.Signals), []string{"a", "b", "c"}) {
		t.Fatalf("batch = %s %v, want tg [a b c]", b.Channel, ids(b.Signals))
	}
	for i := 0; i < 3; i++ {
		if r := h.record(); r.Suppressed != "" {
			t.Fatalf("record %s suppressed %q", r.RuleID, r.Suppressed)
		}
	}

	// A new signal opens a fresh window.
	h.send(sig("d", config.LevelWarn, "tg"))
	h.clk.Advance(10 * time.Second)
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"d"}) {
		t.Fatalf("second batch = %v, want [d]", ids(b.Signals))
	}
}

func TestBatcher_PerChannel(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	h.send(sig("a", config.LevelWarn, "tg"))
	h.clk.Advance(5 * time.Second)
	h.send(sig("b", config.LevelWarn, "mail"))
	h.send(sig("c", config.LevelWarn, "tg", "mail"))

	h.clk.Advance(5 * time.Second) // tg window (opened t=0) closes
	b := h.batch()
	if b.Channel != "tg" || !eqStrings(ids(b.Signals), []string{"a", "c"}) {
		t.Fatalf("batch = %s %v, want tg [a c]", b.Channel, ids(b.Signals))
	}
	h.noBatch()

	h.clk.Advance(5 * time.Second) // mail window (opened t=5s) closes
	b = h.batch()
	if b.Channel != "mail" || !eqStrings(ids(b.Signals), []string{"b", "c"}) {
		t.Fatalf("batch = %s %v, want mail [b c]", b.Channel, ids(b.Signals))
	}

	// Each signal recorded exactly once, even "c" on two channels.
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		r := h.record()
		if r.Suppressed != "" {
			t.Fatalf("record %s suppressed %q", r.RuleID, r.Suppressed)
		}
		seen[r.RuleID]++
	}
	h.noRecord()
	if seen["a"] != 1 || seen["b"] != 1 || seen["c"] != 1 {
		t.Fatalf("records = %v", seen)
	}
}

func TestBatcher_RateLimit(t *testing.T) {
	nc := baseCfg()
	nc.MergeWindow = time.Second
	nc.RatePerMinute = 2
	h := newHarness(t, noonUTC, nc)

	var delivered, rated []string
	for _, id := range []string{"s1", "s2", "s3", "s4", "s5"} {
		h.send(sig(id, config.LevelWarn, "tg"))
		h.clk.Advance(time.Second)
		r := h.record()
		if r.RuleID != id {
			t.Fatalf("record %s, want %s", r.RuleID, id)
		}
		switch r.Suppressed {
		case "":
			b := h.batch()
			delivered = append(delivered, ids(b.Signals)...)
		case "rate":
			rated = append(rated, id)
		default:
			t.Fatalf("record %s suppressed %q", id, r.Suppressed)
		}
	}
	if !eqStrings(delivered, []string{"s1", "s2"}) || !eqStrings(rated, []string{"s3", "s4", "s5"}) {
		t.Fatalf("delivered=%v rated=%v", delivered, rated)
	}
	h.noBatch()

	// critical bypasses an empty bucket.
	h.send(sig("crit", config.LevelCritical, "tg"))
	h.clk.Advance(time.Second)
	if r := h.record(); r.Suppressed != "" {
		t.Fatalf("critical suppressed %q", r.Suppressed)
	}
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"crit"}) {
		t.Fatalf("batch = %v", ids(b.Signals))
	}

	// Continuous refill: 2/min is one token per 30s. s2 left 1/30 token at
	// t=2s; each later rated take adds 1/30, so at t=5s the bucket holds 4/30
	// (critical at t=6s did not touch it). At t=30s it holds 29/30 (rated);
	// at t=31s exactly 1 token (delivered).
	h.clk.Advance(23 * time.Second)
	h.send(sig("s6", config.LevelWarn, "tg"))
	h.clk.Advance(time.Second)
	if r := h.record(); r.Suppressed != "rate" {
		t.Fatalf("s6 suppressed %q at 29/30 token, want rate", r.Suppressed)
	}
	h.send(sig("s7", config.LevelWarn, "tg"))
	h.clk.Advance(time.Second)
	if r := h.record(); r.Suppressed != "" {
		t.Fatalf("s7 suppressed %q after refill", r.Suppressed)
	}
	h.batch()
}

func TestBatcher_RateLimitPartialBatch(t *testing.T) {
	nc := baseCfg()
	nc.RatePerMinute = 2
	h := newHarness(t, noonUTC, nc)
	h.send(sig("a", config.LevelWarn, "tg"))
	h.send(sig("b", config.LevelCritical, "tg"))
	h.send(sig("c", config.LevelWarn, "tg"))
	h.send(sig("d", config.LevelWarn, "tg"))
	h.clk.Advance(10 * time.Second)

	got := map[string]string{}
	for i := 0; i < 4; i++ {
		r := h.record()
		got[r.RuleID] = r.Suppressed
	}
	if got["a"] != "" || got["b"] != "" || got["c"] != "" || got["d"] != "rate" {
		t.Fatalf("records = %v", got)
	}
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"a", "b", "c"}) {
		t.Fatalf("batch = %v, want [a b c]", ids(b.Signals))
	}
}

func TestBatcher_RateLimitUnlimited(t *testing.T) {
	nc := baseCfg()
	nc.RatePerMinute = 0
	h := newHarness(t, noonUTC, nc)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		h.send(sig(id, config.LevelWarn, "tg"))
	}
	h.clk.Advance(10 * time.Second)
	if b := h.batch(); len(b.Signals) != 5 {
		t.Fatalf("batch = %v", ids(b.Signals))
	}
}

func TestBatcher_QuietHours(t *testing.T) {
	sh, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	nc := baseCfg()
	nc.QuietHours = &config.QuietHours{Start: "23:00", End: "07:00", TZ: "Asia/Shanghai"}
	nc.QuietMinLevel = config.LevelCritical

	// 23:30 Shanghai: inside the window (before midnight).
	h := newHarness(t, time.Date(2026, 10, 30, 23, 30, 0, 0, sh), nc)
	h.send(sig("w", config.LevelWarn, "tg"))
	if r := h.record(); r.RuleID != "w" || r.Suppressed != "quiet" {
		t.Fatalf("record = %s %q, want w quiet (on arrival)", r.RuleID, r.Suppressed)
	}
	h.send(sig("c", config.LevelCritical, "tg"))
	h.clk.Advance(10 * time.Second)
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"c"}) {
		t.Fatalf("batch = %v, want [c]", ids(b.Signals))
	}
	if r := h.record(); r.RuleID != "c" || r.Suppressed != "" {
		t.Fatalf("record = %s %q", r.RuleID, r.Suppressed)
	}

	cases := []struct {
		name  string
		at    time.Time
		nc    func(config.NotifyConfig) config.NotifyConfig
		quiet bool
	}{
		{"after midnight", time.Date(2026, 10, 31, 6, 59, 0, 0, sh), nil, true},
		{"end exclusive", time.Date(2026, 10, 31, 7, 0, 0, 0, sh), nil, false},
		{"start inclusive", time.Date(2026, 10, 31, 23, 0, 0, 0, sh), nil, true},
		{"daytime", time.Date(2026, 10, 31, 12, 0, 0, 0, sh), nil, false},
		// 23:30 UTC is 07:30 Shanghai: TZ must be honored.
		{"tz honored", time.Date(2026, 10, 30, 23, 30, 0, 0, time.UTC), nil, false},
		{"single-digit hour end", time.Date(2026, 10, 31, 6, 30, 0, 0, sh), func(c config.NotifyConfig) config.NotifyConfig {
			c.QuietHours = &config.QuietHours{Start: "23:00", End: "7:00", TZ: "Asia/Shanghai"}
			return c
		}, true},
		{"single-digit hour end exclusive", time.Date(2026, 10, 31, 7, 0, 0, 0, sh), func(c config.NotifyConfig) config.NotifyConfig {
			c.QuietHours = &config.QuietHours{Start: "23:00", End: "7:00", TZ: "Asia/Shanghai"}
			return c
		}, false},
		{"non-wrapping", time.Date(2026, 10, 31, 13, 0, 0, 0, sh), func(c config.NotifyConfig) config.NotifyConfig {
			c.QuietHours = &config.QuietHours{Start: "12:00", End: "14:00", TZ: "Asia/Shanghai"}
			return c
		}, true},
		{"start==end disabled", time.Date(2026, 10, 31, 1, 0, 0, 0, sh), func(c config.NotifyConfig) config.NotifyConfig {
			c.QuietHours = &config.QuietHours{Start: "07:00", End: "07:00", TZ: "Asia/Shanghai"}
			return c
		}, false},
		{"min level warn lets warn through", time.Date(2026, 10, 31, 1, 0, 0, 0, sh), func(c config.NotifyConfig) config.NotifyConfig {
			c.QuietMinLevel = config.LevelWarn
			return c
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := nc
			if tc.nc != nil {
				c = tc.nc(c)
			}
			h := newHarness(t, tc.at, c)
			h.send(sig("w", config.LevelWarn, "tg"))
			if tc.quiet {
				if r := h.record(); r.Suppressed != "quiet" {
					t.Fatalf("suppressed %q, want quiet", r.Suppressed)
				}
				return
			}
			h.clk.Advance(10 * time.Second)
			h.batch()
			if r := h.record(); r.Suppressed != "" {
				t.Fatalf("suppressed %q, want delivered", r.Suppressed)
			}
		})
	}
}

func TestBatcher_PreSuppressedRecordedNotDelivered(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	s := sig("cd", config.LevelCritical, "tg")
	s.Suppressed = "cooldown"
	h.send(s)
	if r := h.record(); r.Suppressed != "cooldown" {
		t.Fatalf("record suppressed %q", r.Suppressed)
	}
	h.clk.Advance(time.Minute)
	h.noBatch()
}

func TestBatcher_MultiChannelAllRated(t *testing.T) {
	nc := baseCfg()
	nc.RatePerMinute = 1
	nc.MergeWindow = time.Second
	h := newHarness(t, noonUTC, nc)
	// Drain both buckets.
	h.send(sig("a", config.LevelWarn, "tg", "mail"))
	h.clk.Advance(time.Second)
	h.record()
	h.batch()
	h.batch()
	// Second signal is rated on both channels: one record, no batch.
	h.send(sig("b", config.LevelWarn, "tg", "mail"))
	h.clk.Advance(time.Second)
	if r := h.record(); r.RuleID != "b" || r.Suppressed != "rate" {
		t.Fatalf("record = %s %q", r.RuleID, r.Suppressed)
	}
	h.noRecord()
	h.noBatch()
}

func TestBatcher_HotReloadMergeWindow(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	nc := baseCfg()
	nc.MergeWindow = 2 * time.Second
	h.setCfg(nc)
	h.send(sig("a", config.LevelWarn, "tg"))
	h.clk.Advance(2 * time.Second)
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"a"}) {
		t.Fatalf("batch = %v", ids(b.Signals))
	}
}

func TestBatcher_InClosedFlushes(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	h.send(sig("a", config.LevelWarn, "tg"))
	close(h.in)
	if b := h.batch(); !eqStrings(ids(b.Signals), []string{"a"}) {
		t.Fatalf("batch = %v", ids(b.Signals))
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		h.done <- err // let Cleanup observe it
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after in closed")
	}
}

func TestBatcher_CancelReturnsNil(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())
	h.send(sig("a", config.LevelWarn, "tg"))
	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		h.done <- err
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after cancel")
	}
	// Nobody was reading out, so the pending batch was dropped; its signal
	// was still recorded exactly once at flush.
	h.noBatch()
	if r := h.record(); r.RuleID != "a" {
		t.Fatalf("record = %s", r.RuleID)
	}
	h.noRecord()
}

func TestBatcher_HotReloadReArmsEarlierTimer(t *testing.T) {
	h := newHarness(t, noonUTC, baseCfg())   // MergeWindow 10s
	h.send(sig("a", config.LevelWarn, "tg")) // tg closes at t=10s
	nc := baseCfg()
	nc.MergeWindow = 2 * time.Second
	h.setCfg(nc)
	h.clk.Advance(time.Second)
	h.send(sig("b", config.LevelWarn, "mail")) // mail closes at t=3s
	h.clk.Advance(2 * time.Second)             // t=3s
	if b := h.batch(); b.Channel != "mail" || !eqStrings(ids(b.Signals), []string{"b"}) {
		t.Fatalf("batch = %s %v, want mail [b]", b.Channel, ids(b.Signals))
	}
	if r := h.record(); r.RuleID != "b" {
		t.Fatalf("record = %s, want b", r.RuleID)
	}
	h.noBatch()
	h.clk.Advance(7 * time.Second) // t=10s
	if b := h.batch(); b.Channel != "tg" || !eqStrings(ids(b.Signals), []string{"a"}) {
		t.Fatalf("batch = %s %v, want tg [a]", b.Channel, ids(b.Signals))
	}
}
