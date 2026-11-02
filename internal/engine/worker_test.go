package engine

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/indicator"
	"trader-bot/internal/market"
	"trader-bot/internal/rule"
	"trader-bot/internal/signal"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const sym = "BTCUSDT"

func parseCfg(t *testing.T, rules ...string) *config.Config {
	t.Helper()
	y := "symbols: [BTCUSDT, ETHUSDT]\nchannels:\n  tg: {type: telegram}\nrules:\n"
	for _, r := range rules {
		y += "  - " + r + "\n"
	}
	c, err := config.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const pumpRule = `{id: pump_with_volume, interval: 5m, when: {all: ["change_pct(1) > 3", "volume_ratio(20) > 2"]}, cooldown: 30m, channels: [tg]}`

// k1 is a 1m kline event at minute min.
func k1(s string, min int, close, vol float64, closed bool) market.KlineEvent {
	return market.KlineEvent{Kline: market.Kline{Symbol: s, Interval: market.I1m,
		OpenTime: t0.Add(time.Duration(min) * time.Minute),
		Open:     close, High: close, Low: close, Close: close, Volume: vol, Closed: closed}}
}

// feed5 returns five closed 1m bars making up 5m bucket i.
func feed5(i int, close, vol float64) []market.Event {
	var out []market.Event
	for m := 0; m < 5; m++ {
		out = append(out, k1(sym, i*5+m, close, vol/5, true))
	}
	return out
}

// hist5 returns n closed 5m bars starting at t0.
func hist5(n int, f func(i int) (close, vol float64)) map[market.Interval][]market.Kline {
	var ks []market.Kline
	for i := 0; i < n; i++ {
		c, v := f(i)
		ks = append(ks, market.Kline{Symbol: sym, Interval: market.I5m, OpenTime: t0.Add(time.Duration(i) * 5 * time.Minute),
			Open: c, High: c, Low: c, Close: c, Volume: v, Closed: true})
	}
	return map[market.Interval][]market.Kline{market.I5m: ks}
}

func flat(int) (float64, float64) { return 100, 10 }

type harness struct {
	w    *Worker
	gate *signal.Gate
	clk  *clock.Fake
	out  chan signal.Signal
}

func newHarness(t *testing.T, cfg *config.Config) *harness {
	clk := clock.NewFake(t0.Add(24 * time.Hour))
	g := signal.NewGate(clk)
	out := make(chan signal.Signal, 64)
	w := NewWorker(sym, cfg, g, out, clk)
	// Most tests feed bars dated long before the harness clock; the stale
	// check (on by default) is exercised by its own test.
	w.staleCheck = false
	return &harness{w: w, gate: g, clk: clk, out: out}
}

// run feeds evs through Run and returns every emitted signal after Run exits
// (closing in is the barrier: Run drains it before returning).
func (h *harness) run(t *testing.T, evs ...market.Event) []signal.Signal {
	t.Helper()
	in := make(chan market.Event, len(evs))
	for _, e := range evs {
		in <- e
	}
	close(in)
	if err := h.w.Run(context.Background(), in, nil); err != nil {
		t.Fatal(err)
	}
	return h.drain()
}

// handle feeds evs synchronously (no Run) and returns emitted signals.
func (h *harness) handle(t *testing.T, evs ...market.Event) []signal.Signal {
	t.Helper()
	for _, e := range evs {
		if err := h.w.handle(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	return h.drain()
}

func (h *harness) drain() []signal.Signal {
	var got []signal.Signal
	for {
		select {
		case s := <-h.out:
			got = append(got, s)
		default:
			return got
		}
	}
}

func ids(ss []signal.Signal) []string {
	var out []string
	for _, s := range ss {
		out = append(out, s.RuleID+"/"+s.Suppressed)
	}
	return out
}

func TestWorker_FiresOnClose(t *testing.T) {
	h := newHarness(t, parseCfg(t, pumpRule))
	h.w.Warmup(hist5(30, flat))
	got := h.run(t, feed5(30, 104, 50)...)
	if len(got) != 1 {
		t.Fatalf("signals=%v", ids(got))
	}
	s := got[0]
	if s.RuleID != "pump_with_volume" || s.Symbol != sym || s.Interval != market.I5m || s.Suppressed != "" {
		t.Fatalf("signal=%+v", s)
	}
	if !strings.Contains(s.Detail, "change_pct(1)=") || !strings.Contains(s.Detail, "volume_ratio(20)=") {
		t.Fatalf("detail=%q", s.Detail)
	}
	if s.Price != 104 || !s.Time.Equal(h.clk.Now()) || s.Level != config.LevelWarn || len(s.Channels) != 1 || s.Channels[0] != "tg" {
		t.Fatalf("signal=%+v", s)
	}
}

func TestWorker_WarmupDoesNotEmit(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: up, interval: 5m, when: {all: ["close > 50"]}, channels: [tg]}`))
	h.w.Warmup(hist5(30, flat))
	if got := h.drain(); len(got) != 0 {
		t.Fatalf("warmup emitted %v", ids(got))
	}
}

// I3: an interval gained on a reload with no history drops its first,
// partial bucket instead of feeding it to the indicators.
func TestWorker_ReloadedIntervalDropsPartialFirstBucket(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`))
	h.w.started = true
	h.w.reconfigure(parseCfg(t,
		`{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`,
		`{id: b, interval: 15m, when: {all: ["volume_ratio(2) > 1"]}, channels: [tg]}`))
	// Start 7 minutes into a 15m bucket: minutes 7..14 close a partial bucket.
	for m := 7; m < 15; m++ {
		h.handle(t, k1(sym, m, 100, 1, true))
	}
	if n := len(h.w.bars[market.I15m]); n != 0 {
		t.Fatalf("partial bucket kept: %d bars", n)
	}
	for m := 15; m < 30; m++ {
		h.handle(t, k1(sym, m, 100, 1, true))
	}
	if bs := h.w.bars[market.I15m]; len(bs) != 1 || bs[0].Volume != 15 {
		t.Fatalf("second bucket = %+v, want one full bucket (volume 15)", bs)
	}
}

// I3: with a history func, an interval gained on a reload is warmed from
// history (including the 1m bars of its in-progress bucket).
func TestWorker_ReloadedIntervalWarmsFromHistory(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`))
	h.w.started = true
	var q []market.Kline
	for i := 0; i < 20; i++ {
		q = append(q, market.Kline{Symbol: sym, Interval: market.I15m, OpenTime: t0.Add(time.Duration(i) * 15 * time.Minute),
			Open: 100, High: 100, Low: 100, Close: 100, Volume: 15, Closed: true})
	}
	var ones []market.Kline
	for m := 300; m < 307; m++ { // first 7 minutes of bucket 20
		ones = append(ones, k1(sym, m, 100, 1, true).Kline)
	}
	h.w.history = func(context.Context, string) (map[market.Interval][]market.Kline, error) {
		return map[market.Interval][]market.Kline{market.I15m: q, market.I1m: ones}, nil
	}
	h.w.reconfigure(parseCfg(t,
		`{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`,
		`{id: b, interval: 15m, when: {all: ["volume_ratio(2) > 1"]}, channels: [tg]}`))
	h.w.fetchCold(context.Background())
	h.w.applyHistory(context.Background(), <-h.w.histCh)
	if h.w.cold[market.I15m] {
		t.Fatal("interval still cold after history")
	}
	if n := len(h.w.bars[market.I15m]); n != 20 {
		t.Fatalf("warmed bars = %d, want 20", n)
	}
	for m := 307; m < 315; m++ {
		h.handle(t, k1(sym, m, 100, 1, true))
	}
	bs := h.w.bars[market.I15m]
	if len(bs) != 21 || bs[20].Volume != 15 {
		t.Fatalf("live bucket after history = %+v, want a complete bucket (volume 15)", bs[len(bs)-1])
	}
}

// N1: an interval that stays cold after its history fetch (no closed bar of
// it yet, e.g. a new listing or a timed-out warmup) must not be re-fetched in
// a loop; and a reload right at a bucket start is warm, not cold.
func TestWorker_ReloadedIntervalFetchedOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hist     func(now time.Time) map[market.Interval][]market.Kline
		wantCold bool
	}{
		{"no history for the interval", func(time.Time) map[market.Interval][]market.Kline { return nil }, true},
		{"reload at the start of a bucket", func(now time.Time) map[market.Interval][]market.Kline {
			// Closed 15m bars up to the bucket that just ended; no 1m bar of
			// the new bucket has closed yet.
			var q []market.Kline
			end := now.Truncate(15 * time.Minute)
			for i := 20; i >= 1; i-- {
				q = append(q, market.Kline{Symbol: sym, Interval: market.I15m, OpenTime: end.Add(-time.Duration(i) * 15 * time.Minute),
					Open: 100, High: 100, Low: 100, Close: 100, Volume: 15, Closed: true})
			}
			return map[market.Interval][]market.Kline{market.I15m: q}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, parseCfg(t, `{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`))
			h.clk.Advance(h.clk.Now().Truncate(15*time.Minute).Add(15*time.Minute).Sub(h.clk.Now()) + 20*time.Second)
			h.w.started = true
			var calls int
			h.w.history = func(context.Context, string) (map[market.Interval][]market.Kline, error) {
				calls++
				return tc.hist(h.clk.Now()), nil
			}
			h.w.reconfigure(parseCfg(t,
				`{id: a, interval: 5m, when: {all: ["close > 1"]}, channels: [tg]}`,
				`{id: b, interval: 15m, when: {all: ["close > 1"]}, channels: [tg]}`))
			h.w.fetchCold(context.Background())
			h.w.applyHistory(context.Background(), <-h.w.histCh)
			if h.w.fetching {
				t.Fatal("applyHistory started another fetch for an interval it already fetched")
			}
			select {
			case <-h.w.histCh:
				t.Fatal("a second history result arrived")
			case <-time.After(50 * time.Millisecond):
			}
			if calls != 1 {
				t.Fatalf("history called %d times, want 1", calls)
			}
			if got := h.w.cold[market.I15m]; got != tc.wantCold {
				t.Fatalf("cold = %v, want %v", got, tc.wantCold)
			}
		})
	}
}

// N2: a realtime rule evaluated on an in-progress bucket rebuilt from
// gap-filled (old) 1m bars is recorded as stale, not delivered.
func TestWorker_RealtimeOnReplayedBucketIsStale(t *testing.T) {
	cfg := `{id: rt, interval: 5m, trigger: realtime, cooldown: 0s, when: {all: ["close > 100"]}, channels: [tg]}`
	h := newHarness(t, parseCfg(t, cfg))
	h.w.staleCheck = true
	// Minute 0's bucket ended ~24h before the harness clock.
	got := h.handle(t, k1(sym, 0, 105, 1, true))
	if len(got) != 1 || got[0].Suppressed != "stale" {
		t.Fatalf("replayed bucket: want one stale signal, got %+v", got)
	}
}

// I2: a closed bar that arrives long after it closed (a gap fill after an
// outage) still updates indicators, but its signal is recorded as stale and
// never delivered; the same bar arriving on time fires normally.
func TestWorker_StaleGapFilledBarSuppressed(t *testing.T) {
	cfg := `{id: up, interval: 1m, cooldown: 0s, when: {all: ["close > 100"]}, channels: [tg]}`
	h := newHarness(t, parseCfg(t, cfg))
	h.w.staleCheck = true
	// The harness clock is t0+24h; a bar at minute 0 closed ~24h ago.
	got := h.handle(t, k1(sym, 0, 105, 1, true))
	if len(got) != 1 || got[0].Suppressed != "stale" {
		t.Fatalf("stale bar: want one stale-suppressed signal, got %+v", got)
	}

	fresh := newHarness(t, parseCfg(t, cfg))
	fresh.w.staleCheck = true
	minute := int(fresh.clk.Now().Sub(t0)/time.Minute) - 1 // closed just now
	if got := fresh.handle(t, k1(sym, minute, 105, 1, true)); len(got) != 1 || got[0].Suppressed != "" {
		t.Fatalf("fresh bar: want one delivered signal, got %+v", got)
	}
}

// R23: a condition already true at the end of warmup (i.e. before a restart)
// is primed as seen, so the first live close does not re-announce it.
func TestWorker_WarmupPrimesGate(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: up, interval: 5m, cooldown: 0s, when: {all: ["close > 50"]}, channels: [tg]}`))
	h.w.Warmup(hist5(30, flat))
	if got := h.handle(t, feed5(30, 100, 10)...); len(got) != 0 {
		t.Fatalf("re-fired a condition that was true before start: %v", ids(got))
	}
	h.handle(t, feed5(31, 40, 10)...) // goes False
	if got := h.handle(t, feed5(32, 100, 10)...); len(got) != 1 {
		t.Fatalf("did not fire on a real False->True edge: %v", ids(got))
	}
}

func TestWorker_NoFireOnUnclosed(t *testing.T) {
	h := newHarness(t, parseCfg(t, pumpRule))
	h.w.Warmup(hist5(30, flat))
	evs := feed5(30, 104, 50)
	last := evs[4].(market.KlineEvent)
	last.Closed = false
	evs[4] = last
	if got := h.run(t, evs...); len(got) != 0 {
		t.Fatalf("fired on unclosed bar: %v", ids(got))
	}
}

func TestWorker_DuplicateCloseEvaluatedOnce(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: up, interval: 1m, when: {all: ["close > 100"]}, channels: [tg]}`))
	got := h.run(t,
		k1(sym, 0, 105, 1, true),
		k1(sym, 0, 95, 1, true), // duplicate closed bar (revised): must be ignored
		k1(sym, 1, 106, 1, true),
	)
	if len(got) != 1 {
		t.Fatalf("signals=%v", ids(got))
	}
	ind := h.w.inds["1m|close"]
	cur, _ := ind.Value()
	prev, _ := ind.Prev()
	if cur != 106 || prev != 105 {
		t.Fatalf("close indicator cur=%v prev=%v (duplicate bar fed?)", cur, prev)
	}
}

func TestWorker_ConfigReloadAddsRule(t *testing.T) {
	h := newHarness(t, parseCfg(t, pumpRule))
	h.w.Warmup(hist5(30, func(int) (float64, float64) { return 101, 10 }))
	if got := h.handle(t, feed5(30, 101, 10)...); len(got) != 0 {
		t.Fatalf("signals=%v", ids(got))
	}
	// sma(5) is defined right away only if the retained bars are replayed.
	h.w.reconfigure(parseCfg(t, pumpRule, `{id: above_sma, interval: 5m, when: {all: ["sma(5) > 100"]}, channels: [tg]}`))
	got := h.handle(t, feed5(31, 101, 10)...)
	if len(got) != 1 || got[0].RuleID != "above_sma" {
		t.Fatalf("signals=%v", ids(got))
	}
}

func TestWorker_ConfigReloadViaRun(t *testing.T) {
	h := newHarness(t, parseCfg(t, pumpRule))
	h.w.Warmup(hist5(30, flat))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan market.Event)
	cfgs := make(chan *config.Config, 1)
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx, in, cfgs) }()
	cfgs <- parseCfg(t, `{id: up, interval: 1m, when: {all: ["close > 100"]}, channels: [tg]}`)
	// The cfg is applied eventually; keep feeding fresh bars until "up" fires.
	deadline := time.After(5 * time.Second)
	for i := 0; ; i++ {
		in <- k1(sym, 2*i, 90, 1, true)
		in <- k1(sym, 2*i+1, 105, 1, true)
		select {
		case s := <-h.out:
			if s.RuleID != "up" {
				t.Fatalf("signal=%+v", s)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatal("reloaded rule never fired")
		}
	}
}

func TestWorker_FundingRule(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: oversold_neg_funding, interval: 5m, when: {all: ["rsi(14) < 30", "funding_rate < -0.0005"]}, channels: [tg]}`))
	h.w.Warmup(hist5(30, func(i int) (float64, float64) { return 200 - 2*float64(i), 10 }))
	// Mixed kline+contract rule: the contract event alone does not evaluate it.
	if got := h.handle(t, market.MarkPriceEvent{Symbol: sym, Time: h.clk.Now(), MarkPrice: 139, FundingRate: -0.0007}); len(got) != 0 {
		t.Fatalf("fired without candle close: %v", ids(got))
	}
	got := h.handle(t, feed5(30, 139, 10)...)
	if len(got) != 1 || got[0].RuleID != "oversold_neg_funding" {
		t.Fatalf("signals=%v", ids(got))
	}
	if !strings.Contains(got[0].Detail, "funding_rate=-0.0007") || !strings.Contains(got[0].Detail, "rsi(14)=") {
		t.Fatalf("detail=%q", got[0].Detail)
	}
}

// R22: an on_close contract-only rule waits for its interval's close, so a
// funding rate hovering around the threshold cannot fire on every markPrice.
func TestWorker_ContractOnlyOnCloseWaitsForClose(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: neg_funding, interval: 1m, when: {all: ["funding_rate < -0.0005"]}, channels: [tg]}`))
	h.handle(t, k1(sym, 0, 123, 1, false))
	for i := 0; i < 30; i++ {
		fr := -0.0007
		if i%2 == 1 {
			fr = -0.0001
		}
		if got := h.handle(t, market.MarkPriceEvent{Symbol: sym, Time: h.clk.Now(), MarkPrice: 124, FundingRate: fr}); len(got) != 0 {
			t.Fatalf("on_close rule fired on a contract event: %v", ids(got))
		}
	}
	h.handle(t, market.MarkPriceEvent{Symbol: sym, Time: h.clk.Now(), MarkPrice: 124, FundingRate: -0.0007})
	got := h.handle(t, k1(sym, 0, 123, 1, true))
	if len(got) != 1 || got[0].RuleID != "neg_funding" {
		t.Fatalf("signals on close=%v", ids(got))
	}
}

func TestWorker_ContractOnlyRealtimeRuleOnEvent(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: neg_funding, interval: 1h, trigger: realtime, when: {all: ["funding_rate < -0.0005"]}, channels: [tg]}`))
	h.handle(t, k1(sym, 0, 123, 1, false))
	got := h.handle(t, market.MarkPriceEvent{Symbol: sym, Time: h.clk.Now(), MarkPrice: 124, FundingRate: -0.0007})
	if len(got) != 1 || got[0].RuleID != "neg_funding" || got[0].Price != 123 || got[0].Interval != market.I1h {
		t.Fatalf("signals=%+v", got)
	}
	// An unrelated contract event does not re-evaluate it.
	if got := h.handle(t, market.OpenInterestEvent{Symbol: sym, Time: h.clk.Now(), OI: 5}); len(got) != 0 {
		t.Fatalf("signals=%v", ids(got))
	}
}

func TestWorker_RealtimeFiresOnUnclosed(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: rt_pump, interval: 5m, trigger: realtime, when: {all: ["change_pct(1) > 3", "close > 103"]}, channels: [tg]}`))
	h.w.Warmup(hist5(30, flat))
	if got := h.handle(t, k1(sym, 150, 102, 1, false)); len(got) != 0 {
		t.Fatalf("fired early: %v", ids(got))
	}
	got := h.handle(t, k1(sym, 150, 104, 1, false))
	if len(got) != 1 || got[0].RuleID != "rt_pump" || got[0].Price != 104 {
		t.Fatalf("signals=%+v", got)
	}
	if !strings.Contains(got[0].Detail, "change_pct(1)=4") || !strings.Contains(got[0].Detail, "close=104") {
		t.Fatalf("detail=%q", got[0].Detail)
	}
}

func TestWorker_SharedIndicatorCreatedOnce(t *testing.T) {
	n := map[string]int{}
	orig := newIndicator
	newIndicator = func(name string, args []float64) (indicator.Indicator, error) {
		n[name]++
		return orig(name, args)
	}
	defer func() { newIndicator = orig }()
	h := newHarness(t, parseCfg(t,
		`{id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}`,
		`{id: b, interval: 5m, when: {all: ["rsi(14) > 70", "close > 1"]}, channels: [tg]}`,
		`{id: c, interval: 1h, when: {all: ["rsi(14) > 70"]}, channels: [tg]}`,
	))
	if n["rsi"] != 2 || n["close"] != 1 {
		t.Fatalf("constructions=%v", n)
	}
	if len(h.w.inds) != 3 || h.w.inds["5m|rsi(14)"] == nil || h.w.inds["1h|rsi(14)"] == nil {
		t.Fatalf("inds=%v", h.w.inds)
	}
	// Reloading the same rules reuses the existing instances.
	h.w.reconfigure(parseCfg(t,
		`{id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}`,
		`{id: b, interval: 5m, when: {all: ["rsi(14) > 70", "close > 1"]}, channels: [tg]}`,
	))
	if n["rsi"] != 2 || len(h.w.inds) != 2 {
		t.Fatalf("after reload constructions=%v inds=%d", n, len(h.w.inds))
	}
}

const upCooldown = `{id: up, interval: 1m, when: {all: ["close > 100"]}, cooldown: 1h, channels: [tg]}`

func TestWorker_RemovedRuleForgetsGate(t *testing.T) {
	other := `{id: other, interval: 1m, when: {all: ["close > 1000"]}, channels: [tg]}`
	h := newHarness(t, parseCfg(t, upCooldown))
	if got := h.handle(t, k1(sym, 0, 105, 1, true)); len(got) != 1 {
		t.Fatalf("signals=%v", ids(got))
	}
	h.w.reconfigure(parseCfg(t, other))
	h.w.reconfigure(parseCfg(t, upCooldown, other))
	got := h.handle(t, k1(sym, 1, 105, 1, true))
	if len(got) != 1 || got[0].RuleID != "up" || got[0].Suppressed != "" {
		t.Fatalf("re-added rule did not fire fresh: %+v", got)
	}
}

// A changed rule re-arms its edge but keeps its cooldown, so tuning a
// threshold while the condition holds cannot re-push inside the cooldown.
func TestWorker_ChangedRuleResetsEdgeKeepsCooldown(t *testing.T) {
	h := newHarness(t, parseCfg(t, upCooldown))
	h.handle(t, k1(sym, 0, 105, 1, true))
	h.w.reconfigure(parseCfg(t, `{id: up, interval: 1m, when: {all: ["close > 101"]}, cooldown: 1h, channels: [tg]}`))
	got := h.handle(t, k1(sym, 1, 105, 1, true))
	if len(got) != 1 || got[0].Suppressed != "cooldown" {
		t.Fatalf("changed rule: want a cooldown-suppressed edge, got %+v", got)
	}
	// Unchanged rule keeps its gate state: still True, no new edge.
	h.w.reconfigure(parseCfg(t, `{id: up, interval: 1m, when: {all: ["close > 101"]}, cooldown: 1h, channels: [tg, tg]}`))
	if got := h.handle(t, k1(sym, 2, 105, 1, true)); len(got) != 0 {
		t.Fatalf("unchanged rule refired: %v", ids(got))
	}
}

func TestWorker_CooldownSuppressedEmitted(t *testing.T) {
	h := newHarness(t, parseCfg(t, upCooldown))
	got := h.handle(t,
		k1(sym, 0, 105, 1, true),
		k1(sym, 1, 95, 1, true),
		k1(sym, 2, 106, 1, true),
	)
	if len(got) != 2 || got[0].Suppressed != "" || got[1].Suppressed != "cooldown" || got[1].Price != 106 {
		t.Fatalf("signals=%+v", got)
	}
}

func TestWorker_RunStopsOnCancel(t *testing.T) {
	h := newHarness(t, parseCfg(t, pumpRule))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx, make(chan market.Event), nil) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on cancel")
	}
}

func TestWorker_SendHonoursCtx(t *testing.T) {
	cfg := parseCfg(t, `{id: up, interval: 1m, when: {all: ["close > 100"]}, channels: [tg]}`)
	clk := clock.NewFake(t0)
	w := NewWorker(sym, cfg, signal.NewGate(clk), make(chan signal.Signal), clk) // nobody reads out
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan market.Event, 1)
	in <- k1(sym, 0, 105, 1, true)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, in, nil) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run blocked on out after cancel")
	}
}

func TestValues_ContractOperands(t *testing.T) {
	h := newHarness(t, parseCfg(t,
		`{id: c, interval: 1h, when: {all: ["oi_change_pct(2) > 10", "liq_volume(5) > 1000", "long_short_ratio > 1", "top_long_short_ratio > 1", "mark_price > 1", "oi > 1"]}, channels: [tg]}`))
	w := h.w
	get := func(expr string) (float64, float64, bool) {
		t.Helper()
		for _, o := range ruleFuncs(w.rules[0]) {
			if o.String() == expr {
				return w.values(market.I1h, nil).Get(o)
			}
		}
		t.Fatalf("no operand %s", expr)
		return 0, 0, false
	}
	if _, _, ok := get("oi_change_pct(2)"); ok {
		t.Fatal("oi_change_pct ok without samples")
	}
	if _, _, ok := get("liq_volume(5)"); ok {
		t.Fatal("liq_volume ok before stream start")
	}
	w.started = true
	if c, _, ok := get("liq_volume(5)"); !ok || c != 0 {
		t.Fatalf("liq_volume=%v ok=%v; want 0,true once started", c, ok)
	}
	now := h.clk.Now()
	for i, oi := range []float64{100, 105, 110, 121} {
		h.handle(t, market.OpenInterestEvent{Symbol: sym, Time: now.Add(time.Duration(i) * time.Minute), OI: oi})
	}
	// latest 121 vs 2 samples ago 105; prev: 110 vs 100.
	if c, p, ok := get("oi_change_pct(2)"); !ok || abs(c-(121.0/105-1)*100) > 1e-9 || abs(p-10) > 1e-9 {
		t.Fatalf("oi_change_pct cur=%v prev=%v ok=%v", c, p, ok)
	}
	if c, p, ok := get("oi"); !ok || c != 121 || p != 110 {
		t.Fatalf("oi cur=%v prev=%v ok=%v", c, p, ok)
	}
	h.handle(t,
		market.LiquidationEvent{Symbol: sym, Time: now.Add(-6 * time.Minute), Price: 10, Qty: 1000}, // outside window
		market.LiquidationEvent{Symbol: sym, Time: now.Add(-4 * time.Minute), Price: 10, Qty: 50},
		market.LiquidationEvent{Symbol: sym, Time: now.Add(-1 * time.Minute), Price: 20, Qty: 10},
	)
	if c, _, ok := get("liq_volume(5)"); !ok || c != 700 {
		t.Fatalf("liq_volume=%v ok=%v", c, ok)
	}
	h.clk.Advance(2 * time.Minute) // the -4m one ages out
	if c, _, ok := get("liq_volume(5)"); !ok || c != 200 {
		t.Fatalf("liq_volume after advance=%v ok=%v", c, ok)
	}
	h.handle(t,
		market.LongShortEvent{Symbol: sym, Kind: "global", Ratio: 1.5},
		market.LongShortEvent{Symbol: sym, Kind: "top", Ratio: 0.8},
		market.MarkPriceEvent{Symbol: sym, MarkPrice: 50, FundingRate: 0.0001},
	)
	if c, _, ok := get("long_short_ratio"); !ok || c != 1.5 {
		t.Fatalf("lsr=%v ok=%v", c, ok)
	}
	if c, _, ok := get("top_long_short_ratio"); !ok || c != 0.8 {
		t.Fatalf("top lsr=%v ok=%v", c, ok)
	}
	if c, p, ok := get("mark_price"); !ok || c != 50 || !isNaN(p) {
		t.Fatalf("mark_price=%v prev=%v ok=%v (prev needs two samples)", c, p, ok)
	}
}

func TestValues_ProvisionalChangePctAndVolume(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: rt, interval: 5m, trigger: realtime, when: {all: ["change_pct(2) > 1", "volume > 1"]}, channels: [tg]}`))
	h.w.Warmup(hist5(30, func(i int) (float64, float64) { return 100 + float64(i), 10 }))
	live := market.Kline{Symbol: sym, Interval: market.I5m, OpenTime: t0.Add(150 * time.Minute), Close: 135, Volume: 7}
	v := h.w.values(market.I5m, &live)
	for _, o := range ruleFuncs(h.w.rules[0]) {
		c, _, ok := v.Get(o)
		switch o.String() {
		case "change_pct(2)": // vs closed close 2 bars back: bar 28 = 128
			if !ok || abs(c-(135.0/128-1)*100) > 1e-9 {
				t.Fatalf("change_pct(2)=%v ok=%v", c, ok)
			}
		case "volume":
			if !ok || c != 7 {
				t.Fatalf("volume=%v ok=%v", c, ok)
			}
		}
	}
}

func ruleFuncs(r config.Rule) []rule.Operand { return rule.Funcs(r.When) }
func abs(x float64) float64                  { return math.Abs(x) }
func isNaN(x float64) bool                   { return math.IsNaN(x) }

func TestWorker_WarmupSeedsInProgressBucket(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: vol, interval: 5m, when: {all: ["volume > 40"]}, channels: [tg]}`))
	hist := hist5(30, flat)
	for m := 150; m < 153; m++ { // first 3 minutes of bucket 30, already closed
		hist[market.I1m] = append(hist[market.I1m], k1(sym, m, 100, 10, true).Kline)
	}
	h.w.Warmup(hist)
	got := h.run(t, k1(sym, 153, 100, 10, true), k1(sym, 154, 100, 10, true))
	if len(got) != 1 || !strings.Contains(got[0].Detail, "volume=50") {
		t.Fatalf("bucket not complete: %+v", got)
	}
}
