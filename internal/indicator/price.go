package indicator

import "trader-bot/internal/market"

// Close returns the latest closed bar's close.
type Close struct{ series }

func NewClose() *Close { return &Close{} }

func (c *Close) Update(k market.Kline) {
	if k.Closed {
		c.set(k.Close)
	}
}

// Highest is the highest high of the previous n bars, excluding the current
// bar (for breakout checks); needs n+1 closed bars.
type Highest struct {
	series
	ring *Ring
}

func NewHighest(n int) *Highest { return &Highest{ring: NewRing(n + 1)} }

func (h *Highest) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	h.ring.Push(k.High)
	if !h.ring.Full() {
		return
	}
	m := h.ring.At(0)
	for i := 1; i < h.ring.Len()-1; i++ {
		if v := h.ring.At(i); v > m {
			m = v
		}
	}
	h.set(m)
}

// Lowest is the lowest low of the previous n bars, excluding the current bar;
// needs n+1 closed bars.
type Lowest struct {
	series
	ring *Ring
}

func NewLowest(n int) *Lowest { return &Lowest{ring: NewRing(n + 1)} }

func (l *Lowest) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	l.ring.Push(k.Low)
	if !l.ring.Full() {
		return
	}
	m := l.ring.At(0)
	for i := 1; i < l.ring.Len()-1; i++ {
		if v := l.ring.At(i); v < m {
			m = v
		}
	}
	l.set(m)
}
