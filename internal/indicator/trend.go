package indicator

import (
	"math"

	"trader-bot/internal/market"
)

// SMA is a simple moving average of closes.
type SMA struct {
	series
	ring *Ring
	sum  float64
}

func NewSMA(n int) *SMA { return &SMA{ring: NewRing(n)} }

func (s *SMA) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if s.ring.Full() {
		s.sum -= s.ring.At(0)
	}
	s.ring.Push(k.Close)
	s.sum += k.Close
	if s.ring.Full() {
		s.set(s.sum / float64(s.ring.Len()))
	}
}

// EMA is a TradingView-style EMA seeded with the SMA of the first n closes.
type EMA struct {
	series
	st *ema
}

func NewEMA(n int) *EMA { return &EMA{st: newEMAState(n, 2.0/float64(n+1))} }

func (e *EMA) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if v, ok := e.st.push(k.Close); ok {
		e.set(v)
	}
}

// MACDOut selects which MACD output an indicator exposes.
type MACDOut int

const (
	MACDLine MACDOut = iota
	MACDSignal
	MACDHist
)

// MACD is ema(fast)-ema(slow) with an EMA signal line seeded by the SMA of the
// first sig defined line values.
type MACD struct {
	series
	fast, slow *ema
	sig        *ema
	out        MACDOut
	fv, sv     float64
	fok, sok   bool
}

func NewMACD(fast, slow, sig int, out MACDOut) *MACD {
	return &MACD{
		fast: newEMAState(fast, 2.0/float64(fast+1)),
		slow: newEMAState(slow, 2.0/float64(slow+1)),
		sig:  newEMAState(sig, 2.0/float64(sig+1)),
		out:  out,
	}
}

func (m *MACD) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	if v, ok := m.fast.push(k.Close); ok {
		m.fv, m.fok = v, true
	}
	if v, ok := m.slow.push(k.Close); ok {
		m.sv, m.sok = v, true
	}
	if !m.fok || !m.sok {
		return
	}
	line := m.fv - m.sv
	sg, sok := m.sig.push(line)
	switch m.out {
	case MACDLine:
		m.set(line)
	case MACDSignal:
		if sok {
			m.set(sg)
		}
	case MACDHist:
		if sok {
			m.set(line - sg)
		}
	}
}

// BollOut selects which Bollinger output an indicator exposes.
type BollOut int

const (
	BollUpper BollOut = iota
	BollMid
	BollLower
	BollWidth
)

// Boll is Bollinger Bands with population standard deviation.
type Boll struct {
	series
	ring *Ring
	k    float64
	out  BollOut
}

func NewBoll(n int, k float64, out BollOut) *Boll {
	return &Boll{ring: NewRing(n), k: k, out: out}
}

func (b *Boll) Update(k market.Kline) {
	if !k.Closed {
		return
	}
	b.ring.Push(k.Close)
	if !b.ring.Full() {
		return
	}
	n := float64(b.ring.Len())
	var sum float64
	for i := 0; i < b.ring.Len(); i++ {
		sum += b.ring.At(i)
	}
	mid := sum / n
	var sq float64
	for i := 0; i < b.ring.Len(); i++ {
		d := b.ring.At(i) - mid
		sq += d * d
	}
	dev := math.Sqrt(sq / n)
	switch b.out {
	case BollUpper:
		b.set(mid + b.k*dev)
	case BollMid:
		b.set(mid)
	case BollLower:
		b.set(mid - b.k*dev)
	case BollWidth:
		if mid != 0 {
			b.set(2 * b.k * dev / mid)
		}
	}
}
