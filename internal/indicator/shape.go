package indicator

import (
	"math"

	"trader-bot/internal/market"
)

// Bar-shape helpers. They are plain functions of one bar so the engine can
// apply the same definitions to an unclosed bar (realtime rules).

// AmplitudePct is (high - low) / prevClose * 100.
func AmplitudePct(k market.Kline, prevClose float64) (float64, bool) {
	if prevClose == 0 {
		return 0, false
	}
	return (k.High - k.Low) / prevClose * 100, true
}

// UpperWick is high - max(open, close).
func UpperWick(k market.Kline) float64 { return k.High - math.Max(k.Open, k.Close) }

// LowerWick is min(open, close) - low.
func LowerWick(k market.Kline) float64 { return math.Min(k.Open, k.Close) - k.Low }

// minBodyFrac floors the body at 0.01% of close so a doji never divides by 0.
const minBodyFrac = 1e-4

// WickBodyRatio is the longer wick divided by the body (floored).
func WickBodyRatio(k market.Kline) (float64, bool) {
	body := math.Max(math.Abs(k.Close-k.Open), math.Abs(k.Close)*minBodyFrac)
	if body == 0 {
		return 0, false
	}
	return math.Max(UpperWick(k), LowerWick(k)) / body, true
}

// barFunc is an indicator whose value is a function of the latest closed bar.
type barFunc struct {
	series
	f func(k market.Kline) (float64, bool)
}

func (b *barFunc) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if v, ok := b.f(k); ok {
		b.set(v)
	}
}

func newBarFunc(f func(market.Kline) (float64, bool)) *barFunc { return &barFunc{f: f} }

func NewOpen() Indicator {
	return newBarFunc(func(k market.Kline) (float64, bool) { return k.Open, true })
}
func NewHigh() Indicator {
	return newBarFunc(func(k market.Kline) (float64, bool) { return k.High, true })
}
func NewLow() Indicator {
	return newBarFunc(func(k market.Kline) (float64, bool) { return k.Low, true })
}

// NewUpperWickPct is the upper wick as % of close.
func NewUpperWickPct() Indicator {
	return newBarFunc(func(k market.Kline) (float64, bool) { return pctOfClose(UpperWick(k), k) })
}

// NewLowerWickPct is the lower wick as % of close.
func NewLowerWickPct() Indicator {
	return newBarFunc(func(k market.Kline) (float64, bool) { return pctOfClose(LowerWick(k), k) })
}

func NewWickBodyRatio() Indicator { return newBarFunc(WickBodyRatio) }

func pctOfClose(v float64, k market.Kline) (float64, bool) {
	if k.Close == 0 {
		return 0, false
	}
	return v / k.Close * 100, true
}

// AmplitudePctInd is (high - low) / previous close * 100; needs 2 closed bars.
type AmplitudePctInd struct {
	series
	prevClose float64
	have      bool
}

func NewAmplitudePct() *AmplitudePctInd { return &AmplitudePctInd{} }

func (a *AmplitudePctInd) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if a.have {
		if v, ok := AmplitudePct(k, a.prevClose); ok {
			a.set(v)
		}
	}
	a.prevClose, a.have = k.Close, true
}

// WickATR is a wick length in multiples of ATR(n) of the previous bars (the
// current bar excluded, so a spike does not dilute its own ratio).
type WickATR struct {
	series
	atr  *ATR
	wick func(market.Kline) float64
}

func NewUpperWickATR(n int) *WickATR { return &WickATR{atr: NewATR(n), wick: UpperWick} }
func NewLowerWickATR(n int) *WickATR { return &WickATR{atr: NewATR(n), wick: LowerWick} }

func (w *WickATR) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if a, ok := w.atr.Value(); ok && a > 0 {
		w.set(w.wick(k) / a)
	}
	w.atr.Update(k)
}
