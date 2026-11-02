package signal

import (
	"context"
	"fmt"
	"sort"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
)

// Batch is the set of signals delivered to one channel when its merge window closes.
type Batch struct {
	Channel string
	Signals []Signal
}

// Batcher groups signals per channel into merge windows, applies quiet hours
// and a per-channel rate limit, and records every signal exactly once.
type Batcher struct {
	clk    clock.Clock
	cfg    func() config.NotifyConfig
	out    chan<- Batch
	record func(Signal)

	// Run-goroutine state.
	windows map[string]*window
	buckets map[string]*bucket
	locs    map[string]*time.Location

	// afterSignal, when set (tests only), is called from Run after an incoming
	// signal has been fully processed, including any clk.After registration.
	afterSignal func()
}

// entry tracks one signal across all the channels it is pending on.
type entry struct {
	sig       Signal
	remaining int  // channels not yet decided
	delivered bool // delivered on at least one channel
}

type window struct {
	channel  string
	deadline time.Time
	entries  []*entry
}

// bucket is a token bucket in integer "token-nanoseconds": one token equals
// time.Minute of credit, refill adds elapsed*rate, capacity is rate tokens.
// A full refill therefore always takes exactly one minute, and the arithmetic
// is exact.
type bucket struct {
	credit int64
	last   time.Time
}

func NewBatcher(clk clock.Clock, cfg func() config.NotifyConfig, out chan<- Batch, record func(Signal)) *Batcher {
	return &Batcher{
		clk:     clk,
		cfg:     cfg,
		out:     out,
		record:  record,
		windows: map[string]*window{},
		buckets: map[string]*bucket{},
		locs:    map[string]*time.Location{},
	}
}

// Run processes signals from in until in is closed (pending batches are
// flushed with blocking sends, abandoned only if ctx ends) or ctx is done
// (pending batches are flushed best-effort without blocking). It returns nil
// in both cases. Run must be called from a single goroutine.
func (b *Batcher) Run(ctx context.Context, in <-chan Signal) error {
	var (
		timer         <-chan time.Time
		timerDeadline time.Time
	)
	arm := func() {
		d, ok := b.earliest()
		if !ok {
			timer = nil
			return
		}
		if timer != nil && !d.Before(timerDeadline) {
			return // existing timer fires no later than needed
		}
		timer, timerDeadline = b.clk.After(d.Sub(b.clk.Now())), d
	}
	for {
		select {
		case <-ctx.Done():
			b.flush(ctx, func(w *window) bool { return true }, false)
			return nil
		case s, ok := <-in:
			if !ok {
				b.flush(ctx, func(w *window) bool { return true }, true)
				return nil
			}
			b.flushExpired(ctx)
			b.add(s)
			arm()
			if b.afterSignal != nil {
				b.afterSignal()
			}
		case <-timer:
			timer = nil
			b.flushExpired(ctx)
			arm()
		}
	}
}

func (b *Batcher) earliest() (time.Time, bool) {
	var d time.Time
	found := false
	for _, w := range b.windows {
		if !found || w.deadline.Before(d) {
			d, found = w.deadline, true
		}
	}
	return d, found
}

func (b *Batcher) flushExpired(ctx context.Context) {
	now := b.clk.Now()
	b.flush(ctx, func(w *window) bool { return !w.deadline.After(now) }, true)
}

// add handles one arriving signal.
func (b *Batcher) add(s Signal) {
	if s.Suppressed != "" {
		b.record(s) // R2: recorded, never delivered
		return
	}
	cfg := b.cfg()
	if s.Level != config.LevelCritical && b.inQuiet(cfg) && s.Level.Rank() < cfg.QuietMinLevel.Rank() {
		s.Suppressed = "quiet"
		b.record(s)
		return
	}
	chans := dedupe(s.Channels)
	if len(chans) == 0 {
		b.record(s) // Suppressed="": unreachable in practice, config.Validate forbids rules without channels
		return
	}
	e := &entry{sig: s, remaining: len(chans)}
	for _, ch := range chans {
		w := b.windows[ch]
		if w == nil {
			w = &window{channel: ch, deadline: b.clk.Now().Add(cfg.MergeWindow)}
			b.windows[ch] = w
		}
		w.entries = append(w.entries, e)
	}
}

// flush closes every window selected by pick, in deadline then channel order.
// Rate decisions and records happen first; then each non-empty batch is sent,
// blocking (until ctx ends) when block is true, otherwise only if out is ready.
func (b *Batcher) flush(ctx context.Context, pick func(*window) bool, block bool) {
	var ws []*window
	for _, w := range b.windows {
		if pick(w) {
			ws = append(ws, w)
		}
	}
	sort.Slice(ws, func(i, j int) bool {
		if !ws[i].deadline.Equal(ws[j].deadline) {
			return ws[i].deadline.Before(ws[j].deadline)
		}
		return ws[i].channel < ws[j].channel
	})
	// Decide (and record) every window before sending any batch, so records
	// are not held up behind a slow consumer of out.
	var batches []Batch
	for _, w := range ws {
		delete(b.windows, w.channel)
		if batch := b.decide(w); len(batch.Signals) > 0 {
			batches = append(batches, batch)
		}
	}
	for _, batch := range batches {
		if block {
			select {
			case b.out <- batch:
				continue
			case <-ctx.Done():
				block = false
			}
		}
		select {
		case b.out <- batch:
		default: // best effort on shutdown: drop
		}
	}
}

// decide applies the rate limit to w's entries, records every entry whose
// last pending channel this was, and returns the batch to deliver.
func (b *Batcher) decide(w *window) Batch {
	rate := b.cfg().RatePerMinute
	now := b.clk.Now()
	batch := Batch{Channel: w.channel}
	for _, e := range w.entries {
		if e.sig.Level == config.LevelCritical || b.take(w.channel, rate, now) {
			e.delivered = true
			batch.Signals = append(batch.Signals, e.sig)
		}
		e.remaining--
		if e.remaining == 0 {
			r := e.sig
			if !e.delivered {
				r.Suppressed = "rate"
			}
			b.record(r)
		}
	}
	batch.Signals = collapse(batch.Signals)
	return batch
}

// collapse merges repeats of one (rule, symbol) into a single line, keeping
// the first one's position and the latest one's values, and marks the count
// as "(×N)" so the user sees one line instead of N.
func collapse(ss []Signal) []Signal {
	type key struct{ rule, symbol string }
	idx := map[key]int{}
	count := map[key]int{}
	var out []Signal
	for _, s := range ss {
		k := key{s.RuleID, s.Symbol}
		count[k]++
		if i, ok := idx[k]; ok {
			out[i] = s
			continue
		}
		idx[k] = len(out)
		out = append(out, s)
	}
	for k, i := range idx {
		if n := count[k]; n > 1 {
			out[i].Detail = fmt.Sprintf("%s (×%d)", out[i].Detail, n)
		}
	}
	return out
}

// take consumes one token from ch's bucket; rate <= 0 means unlimited.
func (b *Batcher) take(ch string, rate int, now time.Time) bool {
	if rate <= 0 {
		return true
	}
	capacity := int64(rate) * int64(time.Minute)
	bk := b.buckets[ch]
	if bk == nil {
		bk = &bucket{credit: capacity, last: now}
		b.buckets[ch] = bk
	}
	if el := now.Sub(bk.last); el >= time.Minute {
		bk.credit = capacity
	} else if el > 0 {
		bk.credit += int64(el) * int64(rate)
	}
	if bk.credit > capacity {
		bk.credit = capacity // also clamps after a hot-reload shrink
	}
	bk.last = now
	if bk.credit < int64(time.Minute) {
		return false
	}
	bk.credit -= int64(time.Minute)
	return true
}

// inQuiet reports whether clk.Now() falls in cfg's quiet hours. Start==End,
// a nil window, or unparsable values (rejected by config.Validate) mean none.
func (b *Batcher) inQuiet(cfg config.NotifyConfig) bool {
	q := cfg.QuietHours
	if q == nil {
		return false
	}
	start, ok1 := parseHHMM(q.Start)
	end, ok2 := parseHHMM(q.End)
	if !ok1 || !ok2 || start == end {
		return false
	}
	loc, ok := b.locs[q.TZ]
	if !ok {
		l, err := time.LoadLocation(q.TZ)
		if err != nil {
			l = nil
		}
		b.locs[q.TZ], loc = l, l // nil caches the failure
	}
	if loc == nil {
		return false
	}
	t := b.clk.Now().In(loc)
	m := t.Hour()*60 + t.Minute()
	if start < end {
		return m >= start && m < end
	}
	return m >= start || m < end // wraps midnight
}

// parseHHMM uses the same parser as config.Validate so both accept the same
// inputs (e.g. "7:00").
func parseHHMM(s string) (int, bool) {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, false
	}
	return t.Hour()*60 + t.Minute(), true
}

func dedupe(chans []string) []string {
	var r []string
	seen := map[string]bool{}
	for _, c := range chans {
		if !seen[c] {
			seen[c] = true
			r = append(r, c)
		}
	}
	return r
}
