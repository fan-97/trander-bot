package indicator

import "trader-bot/internal/market"

// RSI is Wilder's RSI (Pine ta.rsi): rma of gains and losses, each seeded with
// the SMA of the first n changes, so the first value is at bar index n.
type RSI struct {
	series
	up, dn   *ema
	prev     float64
	havePrev bool
}

func NewRSI(n int) *RSI {
	a := 1.0 / float64(n)
	return &RSI{up: newEMAState(n, a), dn: newEMAState(n, a)}
}

func (r *RSI) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if !r.havePrev {
		r.prev, r.havePrev = k.Close, true
		return
	}
	ch := k.Close - r.prev
	r.prev = k.Close
	g, l := 0.0, 0.0
	if ch > 0 {
		g = ch
	} else {
		l = -ch
	}
	u, ok1 := r.up.push(g)
	d, ok2 := r.dn.push(l)
	if !ok1 || !ok2 {
		return
	}
	switch {
	case d == 0:
		r.set(100)
	case u == 0:
		r.set(0)
	default:
		r.set(100 - 100/(1+u/d))
	}
}

// ATR is Wilder's average true range (rma of TR seeded with SMA of first n).
type ATR struct {
	series
	st        *ema
	prevClose float64
	have      bool
}

func NewATR(n int) *ATR { return &ATR{st: newEMAState(n, 1.0/float64(n))} }

func (a *ATR) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	tr := k.High - k.Low
	if a.have {
		if v := abs(k.High - a.prevClose); v > tr {
			tr = v
		}
		if v := abs(k.Low - a.prevClose); v > tr {
			tr = v
		}
	}
	a.prevClose, a.have = k.Close, true
	if v, ok := a.st.push(tr); ok {
		a.set(v)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// ChangePct is (C_t / C_{t-n} - 1) * 100; needs n+1 closed bars.
type ChangePct struct {
	series
	ring *Ring
}

func NewChangePct(n int) *ChangePct { return &ChangePct{ring: NewRing(n + 1)} }

func (c *ChangePct) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	c.ring.Push(k.Close)
	if !c.ring.Full() {
		return
	}
	if old := c.ring.At(0); old != 0 {
		c.set((k.Close/old - 1) * 100)
	}
}
