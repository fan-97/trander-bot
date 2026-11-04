package engine

import (
	"strings"
	"testing"
	"time"

	"trader-bot/internal/market"
)

// seqRule is a 5m rule armed by a 15m close below 100 ("pullback") that
// fires on a 5m close above 110 ("breakout").
func seqRule(extra string) string {
	return `{id: seq, interval: 5m, when: "close > 110", setup: {when: "close@15m < 100"` + extra + `}, cooldown: 0s, channels: [tg]}`
}

func TestSequence_TriggerMustFollowSetup(t *testing.T) {
	h := newHarness(t, parseCfg(t, seqRule(", within: 1h")))
	if got := h.handle(t, minutes(0, 15, 100)...); len(got) != 0 {
		t.Fatalf("quiet start fired: %v", ids(got))
	}
	// Breakout before any pullback: not a signal.
	if got := h.handle(t, minutes(15, 20, 120)...); len(got) != 0 {
		t.Fatalf("breakout before setup fired: %v", ids(got))
	}
	// 15m bucket 15..29 closes at 90 on minute 29: armed (15m updates before
	// the 5m rule on the shared close).
	if got := h.handle(t, minutes(20, 30, 90)...); len(got) != 0 {
		t.Fatalf("setup alone fired: %v", ids(got))
	}
	got := h.handle(t, minutes(30, 35, 120)...)
	if len(got) != 1 || !got[0].BarTime.Equal(t0.Add(30*time.Minute)) {
		t.Fatalf("want one fire on the 00:30 bar, got %+v", got)
	}
	if !strings.Contains(got[0].Detail, "close=120 > 110 | 前置: close@15m=90 < 100") {
		t.Errorf("detail %q", got[0].Detail)
	}
	// The arming is used up: a second breakout needs a new setup edge.
	if got := h.handle(t, append(minutes(35, 40, 90), minutes(40, 45, 120)...)...); len(got) != 0 {
		t.Fatalf("one arming fired twice: %v", ids(got))
	}
}

func TestSequence_WithinExpires(t *testing.T) {
	for _, tc := range []struct {
		within string
		fires  bool
	}{{"5m", false}, {"1h", true}} {
		h := newHarness(t, parseCfg(t, seqRule(", within: "+tc.within)))
		h.handle(t, minutes(0, 15, 100)...)
		h.handle(t, minutes(15, 30, 90)...)  // armed at 00:30
		h.handle(t, minutes(30, 45, 100)...) // setup still True on the 00:35/00:40 bars, False from 00:45
		got := h.handle(t, minutes(45, 50, 120)...)
		if (len(got) == 1) != tc.fires {
			t.Errorf("within %s: got %v, want fire=%v", tc.within, ids(got), tc.fires)
		}
	}
}

func TestSequence_Invalidate(t *testing.T) {
	h := newHarness(t, parseCfg(t, seqRule(`, within: 1h, invalidate: "close < 80"`)))
	h.handle(t, minutes(0, 15, 100)...)
	h.handle(t, minutes(15, 30, 90)...) // armed
	h.handle(t, minutes(30, 35, 70)...) // invalidated
	if got := h.handle(t, minutes(35, 40, 120)...); len(got) != 0 {
		t.Fatalf("fired after invalidate: %v", ids(got))
	}
}

func histIv(iv market.Interval, n int, c float64) []market.Kline {
	var ks []market.Kline
	for i := 0; i < n; i++ {
		ks = append(ks, market.Kline{Symbol: sym, Interval: iv, OpenTime: t0.Add(time.Duration(i) * iv.Duration()),
			Open: c, High: c, Low: c, Close: c, Volume: 1, Closed: true})
	}
	return ks
}

func TestSequence_WarmupPrimes(t *testing.T) {
	// Setup already True at boot: armed, so the next breakout fires.
	h := newHarness(t, parseCfg(t, seqRule(", within: 1h")))
	h.w.Warmup(map[market.Interval][]market.Kline{market.I15m: histIv(market.I15m, 4, 90), market.I5m: histIv(market.I5m, 12, 100)})
	if got := h.handle(t, minutes(60, 65, 120)...); len(got) != 1 {
		t.Fatalf("armed at boot: got %v", ids(got))
	}
	// A breakout already in progress at boot is not a new edge.
	h2 := newHarness(t, parseCfg(t, seqRule(", within: 1h")))
	h2.w.Warmup(map[market.Interval][]market.Kline{market.I15m: histIv(market.I15m, 4, 90), market.I5m: histIv(market.I5m, 12, 120)})
	if got := h2.handle(t, minutes(60, 65, 120)...); len(got) != 0 {
		t.Fatalf("pre-existing breakout fired: %v", ids(got))
	}
}

// A realtime two-stage rule fires intraday once, on the live bar.
func TestSequence_Realtime(t *testing.T) {
	h := newHarness(t, parseCfg(t, `{id: seq, interval: 5m, trigger: realtime, when: "close > 110", setup: {when: "close@15m < 100", within: 1h}, cooldown: 0s, channels: [tg]}`))
	h.handle(t, minutes(0, 15, 100)...)
	h.handle(t, minutes(15, 30, 90)...) // armed
	got := h.handle(t, k1(sym, 30, 120, 1, false), k1(sym, 30, 125, 1, false))
	if len(got) != 1 || !got[0].BarLive {
		t.Fatalf("want one live fire, got %+v", got)
	}
}
