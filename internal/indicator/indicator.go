// Package indicator provides incremental technical indicators that follow
// TradingView (Pine v5) seeding conventions. Every indicator consumes only
// closed klines: Update ignores klines with Closed == false.
package indicator

import (
	"fmt"
	"math"

	"trader-bot/internal/market"
)

// Indicator is an incrementally updated indicator.
type Indicator interface {
	// Update feeds a kline; klines with Closed == false are ignored.
	Update(k market.Kline)
	// Value returns the latest value; ok is false until warmed up.
	Value() (float64, bool)
	// Prev returns the previous defined value; ok is false when fewer than
	// two values have been defined.
	Prev() (float64, bool)
}

// series tracks the latest two defined values.
type series struct {
	cur, prev float64
	count     int
}

func (s *series) set(v float64) {
	s.prev, s.cur = s.cur, v
	s.count++
}

func (s *series) Value() (float64, bool) { return s.cur, s.count >= 1 }
func (s *series) Prev() (float64, bool)  { return s.prev, s.count >= 2 }

// ema is a seeded exponential average (Pine ta.ema with alpha=2/(n+1), ta.rma
// with alpha=1/n): the first value is the SMA of the first n inputs.
type ema struct {
	n     int
	alpha float64
	sum   float64
	seen  int
	cur   float64
}

func newEMAState(n int, alpha float64) *ema { return &ema{n: n, alpha: alpha} }

// push feeds a value and returns the average once defined.
func (e *ema) push(v float64) (float64, bool) {
	if e.seen < e.n {
		e.sum += v
		e.seen++
		if e.seen < e.n {
			return 0, false
		}
		e.cur = e.sum / float64(e.n)
		return e.cur, true
	}
	e.cur = e.alpha*v + (1-e.alpha)*e.cur
	return e.cur, true
}

type factory func(args []float64) (Indicator, error)

func positiveInt(name string, v float64) (int, error) {
	if v != math.Trunc(v) || v < 1 || v > 1e6 {
		return 0, fmt.Errorf("indicator %s: argument %v must be a positive integer", name, v)
	}
	return int(v), nil
}

func ints(name string, args []float64, want int) ([]int, error) {
	if len(args) != want {
		return nil, fmt.Errorf("indicator %s: want %d args, got %d", name, want, len(args))
	}
	out := make([]int, want)
	for i, a := range args {
		v, err := positiveInt(name, a)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func one(name string, mk func(n int) Indicator) factory {
	return func(args []float64) (Indicator, error) {
		v, err := ints(name, args, 1)
		if err != nil {
			return nil, err
		}
		return mk(v[0]), nil
	}
}

func macd(name string, out MACDOut) factory {
	return func(args []float64) (Indicator, error) {
		v, err := ints(name, args, 3)
		if err != nil {
			return nil, err
		}
		return NewMACD(v[0], v[1], v[2], out), nil
	}
}

func boll(name string, out BollOut) factory {
	return func(args []float64) (Indicator, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("indicator %s: want 2 args (n,k), got %d", name, len(args))
		}
		n, err := positiveInt(name, args[0])
		if err != nil {
			return nil, err
		}
		if args[1] <= 0 || math.IsNaN(args[1]) || math.IsInf(args[1], 0) {
			return nil, fmt.Errorf("indicator %s: k must be positive, got %v", name, args[1])
		}
		return NewBoll(n, args[1], out), nil
	}
}

func zero(name string, mk func() Indicator) factory {
	return func(args []float64) (Indicator, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("indicator %s: want 0 args, got %d", name, len(args))
		}
		return mk(), nil
	}
}

var registry = map[string]factory{
	"sma":          one("sma", func(n int) Indicator { return NewSMA(n) }),
	"ema":          one("ema", func(n int) Indicator { return NewEMA(n) }),
	"rsi":          one("rsi", func(n int) Indicator { return NewRSI(n) }),
	"macd":         macd("macd", MACDLine),
	"macd_signal":  macd("macd_signal", MACDSignal),
	"macd_hist":    macd("macd_hist", MACDHist),
	"boll_upper":   boll("boll_upper", BollUpper),
	"boll_mid":     boll("boll_mid", BollMid),
	"boll_lower":   boll("boll_lower", BollLower),
	"atr":          one("atr", func(n int) Indicator { return NewATR(n) }),
	"volume_ratio": one("volume_ratio", func(n int) Indicator { return NewVolumeRatio(n) }),
	"change_pct":   one("change_pct", func(n int) Indicator { return NewChangePct(n) }),
	"highest":      one("highest", func(n int) Indicator { return NewHighest(n) }),
	"lowest":       one("lowest", func(n int) Indicator { return NewLowest(n) }),
	"close":        zero("close", func() Indicator { return NewClose() }),
	"volume":       zero("volume", func() Indicator { return NewVolume() }),
}

// New builds an indicator by registry name, validating the argument count and
// that integer-valued arguments are positive integers.
func New(name string, args []float64) (Indicator, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown indicator %q", name)
	}
	return f(args)
}
