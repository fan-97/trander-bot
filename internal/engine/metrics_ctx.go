package engine

import (
	"math"
	"time"

	"trader-bot/internal/indicator"
	"trader-bot/internal/market"
	"trader-bot/internal/rule"
)

// source identifies which contract event feeds an operand.
type source uint8

const (
	srcMark source = 1 << iota
	srcOI
	srcGlobalLS
	srcTopLS
	srcLiq
)

var contractSource = map[string]source{
	"funding_rate":         srcMark,
	"mark_price":           srcMark,
	"oi":                   srcOI,
	"oi_change_pct":        srcOI,
	"long_short_ratio":     srcGlobalLS,
	"top_long_short_ratio": srcTopLS,
	"liq_volume":           srcLiq,
}

const (
	minOIKeep     = 256       // OI samples kept at least
	minLiqHorizon = time.Hour // liquidations kept at least this long
	minuteNS      = float64(time.Minute)
)

// sample holds the latest two values of a scalar contract series.
type sample struct {
	cur, prev float64
	n         int
}

func (s *sample) set(v float64) { s.prev, s.cur = s.cur, v; s.n++ }

func (s sample) get() (cur, prev float64, ok bool) {
	if s.n == 0 {
		return 0, 0, false
	}
	if s.n < 2 {
		return s.cur, math.NaN(), true
	}
	return s.cur, s.prev, true
}

type liq struct {
	t        time.Time
	notional float64
}

// contract is the symbol-level (interval-independent) contract data.
type contract struct {
	funding, mark, globalLS, topLS sample

	oi     []float64 // OI samples, oldest first
	oiKeep int

	liqs       []liq // in arrival order
	liqHorizon time.Duration
}

// apply records ev and returns which source it fed (0 if none).
func (c *contract) apply(ev market.Event, now time.Time) source {
	switch e := ev.(type) {
	case market.MarkPriceEvent:
		c.funding.set(e.FundingRate)
		c.mark.set(e.MarkPrice)
		return srcMark
	case market.OpenInterestEvent:
		c.oi = append(c.oi, e.OI)
		c.trimOI()
		return srcOI
	case market.LongShortEvent:
		switch e.Kind {
		case "global":
			c.globalLS.set(e.Ratio)
			return srcGlobalLS
		case "top":
			c.topLS.set(e.Ratio)
			return srcTopLS
		}
	case market.LiquidationEvent:
		c.liqs = append(c.liqs, liq{t: e.Time, notional: e.Price * e.Qty})
		c.pruneLiqs(now)
		return srcLiq
	}
	return 0
}

func (c *contract) trimOI() {
	keep := max(c.oiKeep, minOIKeep)
	if len(c.oi) > 2*keep {
		c.oi = append(c.oi[:0], c.oi[len(c.oi)-keep:]...)
	}
}

func (c *contract) pruneLiqs(now time.Time) {
	cutoff := now.Add(-max(c.liqHorizon, minLiqHorizon))
	i := 0
	for i < len(c.liqs) && !c.liqs[i].t.After(cutoff) {
		i++
	}
	if i > 0 {
		c.liqs = append(c.liqs[:0], c.liqs[i:]...)
	}
}

// size adjusts retention to the windows the current rules need.
func (c *contract) size(oiNeed int, liqNeed time.Duration) {
	c.oiKeep = oiNeed
	c.liqHorizon = liqNeed
}

func posInt(args []float64) (int, bool) {
	if len(args) != 1 || args[0] != math.Trunc(args[0]) || args[0] < 1 || args[0] > 1e6 {
		return 0, false
	}
	return int(args[0]), true
}

func pctChange(now, base float64) (float64, bool) {
	if base == 0 {
		return 0, false
	}
	return (now/base - 1) * 100, true
}

// get resolves a contract operand. started reports whether the worker's live
// stream is running (liq_volume is 0, not unavailable, from then on).
func (c *contract) get(o rule.Operand, now time.Time, started bool) (cur, prev float64, ok bool) {
	switch o.Func {
	case "funding_rate":
		return c.funding.get()
	case "mark_price":
		return c.mark.get()
	case "long_short_ratio":
		return c.globalLS.get()
	case "top_long_short_ratio":
		return c.topLS.get()
	case "oi":
		n := len(c.oi)
		if n == 0 {
			return 0, 0, false
		}
		if n < 2 {
			return c.oi[n-1], math.NaN(), true
		}
		return c.oi[n-1], c.oi[n-2], true
	case "oi_change_pct":
		k, valid := posInt(o.Args)
		n := len(c.oi)
		if !valid || n < k+1 {
			return 0, 0, false
		}
		cur, ok = pctChange(c.oi[n-1], c.oi[n-1-k])
		if !ok {
			return 0, 0, false
		}
		prev = math.NaN()
		if n >= k+2 {
			if p, pok := pctChange(c.oi[n-2], c.oi[n-2-k]); pok {
				prev = p
			}
		}
		return cur, prev, true
	case "liq_volume":
		if !started || len(o.Args) != 1 || !(o.Args[0] > 0) || math.IsInf(o.Args[0], 0) {
			return 0, 0, false
		}
		from := now.Add(-time.Duration(o.Args[0] * minuteNS))
		var lastIn float64
		for _, l := range c.liqs {
			if l.t.After(from) && !l.t.After(now) {
				cur += l.notional
				lastIn = l.notional
			}
		}
		// prev is the window value before the most recent liquidation in it.
		return cur, cur - lastIn, true
	}
	return 0, 0, false
}

// values implements rule.Values for one rule interval. With live == nil it
// reads closed-bar indicators; with live set it is the provisional view used
// by realtime rules (see provisional for the operands that read the live bar).
// Contract operands are symbol-level either way. now is fixed at
// construction so Get is pure across Eval and Describe.
type values struct {
	w    *Worker
	iv   market.Interval
	now  time.Time
	live *market.Kline
}

func (w *Worker) values(iv market.Interval, live *market.Kline) values {
	return values{w: w, iv: iv, now: w.clk.Now(), live: live}
}

func indKey(iv market.Interval, o rule.Operand) string { return string(iv) + "|" + o.String() }

func (v values) Get(o rule.Operand) (cur, prev float64, ok bool) {
	if o.IsContract() {
		return v.w.contract.get(o, v.now, v.w.started)
	}
	if v.live != nil {
		if cur, prev, ok, handled := v.provisional(o); handled {
			return cur, prev, ok
		}
	}
	ind := v.w.inds[indKey(v.iv, o)]
	if ind == nil {
		return 0, 0, false
	}
	cur, ok = ind.Value()
	if !ok {
		return 0, 0, false
	}
	prev, pok := ind.Prev()
	if !pok {
		prev = math.NaN()
	}
	return cur, prev, true
}

// provisional resolves the realtime-capable operands against the live bar;
// prev is the corresponding value of the last closed bar.
func (v values) provisional(o rule.Operand) (cur, prev float64, ok, handled bool) {
	bars := v.w.bars[v.iv]
	n := len(bars)
	prev = math.NaN()
	switch o.Func {
	case "close":
		if n > 0 {
			prev = bars[n-1].Close
		}
		return v.live.Close, prev, true, true
	case "volume":
		if n > 0 {
			prev = bars[n-1].Volume
		}
		return v.live.Volume, prev, true, true
	case "change_pct":
		k, valid := posInt(o.Args)
		if !valid || n < k {
			return 0, 0, false, true
		}
		cur, ok = pctChange(v.live.Close, bars[n-k].Close)
		if !ok {
			return 0, 0, false, true
		}
		if n >= k+1 {
			if p, pok := pctChange(bars[n-1].Close, bars[n-1-k].Close); pok {
				prev = p
			}
		}
		return cur, prev, true, true
	case "open", "high", "low":
		pick := func(k market.Kline) float64 {
			switch o.Func {
			case "open":
				return k.Open
			case "high":
				return k.High
			}
			return k.Low
		}
		if n > 0 {
			prev = pick(bars[n-1])
		}
		return pick(*v.live), prev, true, true
	case "amplitude_pct":
		if n < 1 {
			return 0, 0, false, true
		}
		cur, ok = indicator.AmplitudePct(*v.live, bars[n-1].Close)
		if !ok {
			return 0, 0, false, true
		}
		if n >= 2 {
			if p, pok := indicator.AmplitudePct(bars[n-1], bars[n-2].Close); pok {
				prev = p
			}
		}
		return cur, prev, true, true
	case "volume_ratio":
		// The live bar's volume so far against the mean of the previous k
		// closed bars: early in a bar it understates, so it errs on quiet.
		k, valid := posInt(o.Args)
		if !valid || n < k {
			return 0, 0, false, true
		}
		cur, ok = volRatio(v.live.Volume, bars[n-k:])
		if !ok {
			return 0, 0, false, true
		}
		if n >= k+1 {
			if p, pok := volRatio(bars[n-1].Volume, bars[n-1-k:n-1]); pok {
				prev = p
			}
		}
		return cur, prev, true, true
	}
	return 0, 0, false, false
}

func volRatio(vol float64, base []market.Kline) (float64, bool) {
	var sum float64
	for _, b := range base {
		sum += b.Volume
	}
	if sum == 0 {
		return 0, false
	}
	return vol / (sum / float64(len(base))), true
}
