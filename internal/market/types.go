package market

import "time"

// Interval is a Binance kline interval.
type Interval string

const (
	I1m  Interval = "1m"
	I5m  Interval = "5m"
	I15m Interval = "15m"
	I1h  Interval = "1h"
	I4h  Interval = "4h"
	I1d  Interval = "1d"
)

// Duration returns the interval length, or 0 for an unknown interval.
func (i Interval) Duration() time.Duration {
	switch i {
	case I1m:
		return time.Minute
	case I5m:
		return 5 * time.Minute
	case I15m:
		return 15 * time.Minute
	case I1h:
		return time.Hour
	case I4h:
		return 4 * time.Hour
	case I1d:
		return 24 * time.Hour
	}
	return 0
}

// Valid reports whether i is a supported interval.
func (i Interval) Valid() bool { return i.Duration() > 0 }

// Kline is a single candlestick. Closed is true once the bar is final.
type Kline struct {
	Symbol      string
	Interval    Interval
	OpenTime    time.Time
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	QuoteVolume float64
	Closed      bool
}

// Event is any market data event carried on the bus.
type Event interface{ EventSymbol() string }

// KlineEvent wraps a Kline update.
type KlineEvent struct{ Kline }

// MarkPriceEvent carries mark price and funding information.
type MarkPriceEvent struct {
	Symbol      string
	Time        time.Time
	MarkPrice   float64
	FundingRate float64
	NextFunding time.Time
}

// LiquidationEvent carries a forced liquidation order.
type LiquidationEvent struct {
	Symbol string
	Time   time.Time
	Side   string
	Price  float64
	Qty    float64
}

// OpenInterestEvent carries an open interest sample.
type OpenInterestEvent struct {
	Symbol string
	Time   time.Time
	OI     float64
}

// LongShortEvent carries a long/short ratio sample.
type LongShortEvent struct {
	Symbol string
	Time   time.Time
	Kind   string // global|top
	Ratio  float64
}

func (e KlineEvent) EventSymbol() string        { return e.Symbol }
func (e MarkPriceEvent) EventSymbol() string    { return e.Symbol }
func (e LiquidationEvent) EventSymbol() string  { return e.Symbol }
func (e OpenInterestEvent) EventSymbol() string { return e.Symbol }
func (e LongShortEvent) EventSymbol() string    { return e.Symbol }
