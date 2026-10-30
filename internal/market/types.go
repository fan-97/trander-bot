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
