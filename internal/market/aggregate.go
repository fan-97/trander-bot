package market

import "time"

// Aggregator builds higher-interval klines from a stream of 1m klines
// (closed or still updating) for a single symbol.
type Aggregator struct {
	symbol  string
	targets []Interval
	state   map[Interval]*bucketState
}

type bucketState struct {
	active bool
	start  time.Time
	bars   map[time.Time]Kline // 1m bars keyed by OpenTime
	closed bool                // closed bar already emitted
}

// NewAggregator creates an aggregator emitting the given target intervals.
func NewAggregator(symbol string, targets []Interval) *Aggregator {
	a := &Aggregator{symbol: symbol, targets: append([]Interval(nil), targets...), state: map[Interval]*bucketState{}}
	for _, t := range a.targets {
		a.state[t] = &bucketState{}
	}
	return a
}

// Add feeds a 1m kline update and returns the affected target klines in
// target order. A bucket abandoned without closing (gap) is emitted once as
// closed before the new bucket's state.
func (a *Aggregator) Add(k1m Kline) []Kline {
	var out []Kline
	for _, t := range a.targets {
		d := t.Duration()
		if t == I1m {
			out = append(out, k1m)
			continue
		}
		if d <= 0 {
			continue
		}
		st := a.state[t]
		bs := k1m.OpenTime.UTC().Truncate(d)
		if st.active && bs.Before(st.start) {
			continue // late update for an old bucket
		}
		if st.active && bs.After(st.start) {
			if !st.closed {
				prev := a.build(t, st)
				prev.Closed = true
				out = append(out, prev)
			}
			st.active = false
		}
		if !st.active {
			st.active, st.start, st.closed = true, bs, false
			st.bars = map[time.Time]Kline{}
		}
		if st.closed {
			continue
		}
		st.bars[k1m.OpenTime.UTC()] = k1m
		cur := a.build(t, st)
		if k1m.Closed && k1m.OpenTime.UTC().Add(time.Minute).Equal(bs.Add(d)) {
			cur.Closed = true
			st.closed = true
		}
		out = append(out, cur)
	}
	return out
}

func (a *Aggregator) build(t Interval, st *bucketState) Kline {
	k := Kline{Symbol: a.symbol, Interval: t, OpenTime: st.start}
	var first, last time.Time
	n := 0
	for ot, b := range st.bars {
		if n == 0 || ot.Before(first) {
			first = ot
			k.Open = b.Open
		}
		if n == 0 || ot.After(last) {
			last = ot
			k.Close = b.Close
		}
		if n == 0 || b.High > k.High {
			k.High = b.High
		}
		if n == 0 || b.Low < k.Low {
			k.Low = b.Low
		}
		k.Volume += b.Volume
		k.QuoteVolume += b.QuoteVolume
		n++
	}
	return k
}
