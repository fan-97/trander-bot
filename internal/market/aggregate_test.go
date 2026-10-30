package market

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func bar(min int, v float64, closed bool) Kline {
	p := float64(min + 1)
	return Kline{Symbol: "BTCUSDT", Interval: I1m, OpenTime: t0.Add(time.Duration(min) * time.Minute),
		Open: p, High: p, Low: p, Close: p, Volume: v, QuoteVolume: v * 10, Closed: closed}
}

func TestAggregator_5mFrom1m(t *testing.T) {
	a := NewAggregator("BTCUSDT", []Interval{I5m})
	for i := 0; i < 5; i++ {
		out := a.Add(bar(i, 1, true))
		if len(out) != 1 {
			t.Fatalf("len=%d", len(out))
		}
		if i < 4 && out[0].Closed {
			t.Fatalf("bar %d closed early", i)
		}
		if i == 4 {
			k := out[0]
			if !k.Closed || k.Open != 1 || k.Close != 5 || k.High != 5 || k.Low != 1 || k.Volume != 5 || k.QuoteVolume != 50 || !k.OpenTime.Equal(t0) || k.Interval != I5m {
				t.Fatalf("bad bar %+v", k)
			}
		}
	}
	// closed bucket never re-emitted
	if out := a.Add(bar(4, 1, true)); len(out) != 0 {
		t.Fatalf("re-emitted: %+v", out)
	}
}

func TestAggregator_UnclosedUpdateReplaces(t *testing.T) {
	a := NewAggregator("BTCUSDT", []Interval{I5m})
	a.Add(bar(0, 1, false))
	out := a.Add(bar(0, 3, false))
	if out[0].Volume != 3 {
		t.Fatalf("volume=%v", out[0].Volume)
	}
	out = a.Add(bar(1, 2, false))
	if out[0].Volume != 5 {
		t.Fatalf("volume=%v", out[0].Volume)
	}
}

func TestAggregator_GapStartsNewBar(t *testing.T) {
	a := NewAggregator("BTCUSDT", []Interval{I5m, I1m})
	a.Add(bar(0, 1, true))
	a.Add(bar(1, 1, true))
	out := a.Add(bar(6, 1, true)) // skips to next 5m bucket
	if len(out) != 3 {
		t.Fatalf("len=%d %+v", len(out), out)
	}
	if !out[0].Closed || out[0].Interval != I5m || !out[0].OpenTime.Equal(t0) || out[0].Volume != 2 {
		t.Fatalf("prev: %+v", out[0])
	}
	if out[1].Closed || !out[1].OpenTime.Equal(t0.Add(5*time.Minute)) || out[1].Volume != 1 {
		t.Fatalf("new: %+v", out[1])
	}
	if out[2].Interval != I1m {
		t.Fatalf("pass-through: %+v", out[2])
	}
}

func TestEventSymbol(t *testing.T) {
	evs := []Event{KlineEvent{bar(0, 1, true)}, MarkPriceEvent{Symbol: "BTCUSDT"}, LiquidationEvent{Symbol: "BTCUSDT"}, OpenInterestEvent{Symbol: "BTCUSDT"}, LongShortEvent{Symbol: "BTCUSDT"}}
	for _, e := range evs {
		if e.EventSymbol() != "BTCUSDT" {
			t.Fatalf("%T", e)
		}
	}
}
