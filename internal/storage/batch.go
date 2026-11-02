package storage

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

// maxBuffered caps the items held in memory (buffered plus in flight). Past
// it the oldest buffered items are dropped.
const maxBuffered = 10000

// flushTimeout bounds one regular flush; finalFlushTimeout bounds the flush
// done after ctx ends.
const (
	flushTimeout      = 30 * time.Second
	finalFlushTimeout = 5 * time.Second
)

// Sink is the persistence surface the Writer needs; *Store implements it.
// Tests and callers without a database can supply their own.
type Sink interface {
	WriteKlines(ctx context.Context, ks []market.Kline) error
	WriteMarkPrices(ctx context.Context, ms []market.MarkPriceEvent) error
	WriteOI(ctx context.Context, os []market.OpenInterestEvent) error
	WriteLongShort(ctx context.Context, ls []market.LongShortEvent) error
	WriteLiquidations(ctx context.Context, ls []market.LiquidationEvent) error
	WriteSignals(ctx context.Context, ss []signal.Signal) error
}

var _ Sink = (*Store)(nil)

// Writer batches market events and signals into the Store without ever
// blocking its producer. Set the exported fields before calling Run.
type Writer struct {
	s          Sink
	batchSize  int
	flushEvery time.Duration

	// Clock drives the flush interval. Defaults to clock.Real.
	Clock clock.Clock
	// OnDrop, if set, is called (from Run's goroutine) at most once per flush
	// interval, and once more at shutdown, with the number of items dropped
	// since the last call (buffer full, or lost in the final flush). Summing
	// per tick keeps a burst from becoming one call per item.
	OnDrop func(n int)
	// Logger defaults to slog.Default().
	Logger *slog.Logger

	dropped      atomic.Int64
	failingSince atomic.Int64 // UnixNano of the first failed flush in a row; 0 when healthy
}

// FailingSince returns when flushes started failing continuously, or the zero
// time when the last flush succeeded (I6). Safe for concurrent use.
func (w *Writer) FailingSince() time.Time {
	if ns := w.failingSince.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// NewWriter returns a Writer flushing every batchSize items (default 500) or
// every flushEvery (default 2s), whichever comes first.
func NewWriter(s *Store, batchSize int, flushEvery time.Duration) *Writer {
	return NewWriterWithSink(s, batchSize, flushEvery)
}

// NewWriterWithSink is NewWriter for any Sink.
func NewWriterWithSink(s Sink, batchSize int, flushEvery time.Duration) *Writer {
	if batchSize <= 0 {
		batchSize = 500
	}
	if flushEvery <= 0 {
		flushEvery = 2 * time.Second
	}
	return &Writer{s: s, batchSize: batchSize, flushEvery: flushEvery}
}

// Dropped returns how many items have been discarded due to overflow.
func (w *Writer) Dropped() int64 { return w.dropped.Load() }

// Run consumes in until ctx ends (a closed in just stops receiving), then performs a final flush with a fresh
// timeout and returns nil. Closed klines, one mark price per symbol per
// minute, OI, long/short, liquidations and signals are stored; anything else
// is ignored. Flushes run on a separate goroutine, at most one at a time and
// at most batchSize items each, so a slow database never blocks receiving and
// overflow always drops the oldest buffered items. Drops are logged as one
// summary per tick; items lost in the final flush also count as dropped. A failed flush keeps its items for
// the next one; a failure also pauses size-triggered flushes until the next
// interval tick so a down database is not retried in a tight loop.
func (w *Writer) Run(ctx context.Context, in <-chan any) error {
	clk := w.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	log := w.Logger
	if log == nil {
		log = slog.Default()
	}

	var (
		buf      []any
		inflight int                // items in the running flush, 0 if none
		results  = make(chan []any) // failed items of the finished flush
		lastMark = map[string]time.Time{}
		failing  bool
		unlogged int64 // drops not yet reported in the per-tick summary
		tick     = clk.After(w.flushEvery)
	)

	drop := func(n int) {
		w.dropped.Add(int64(n))
		unlogged += int64(n)
	}
	logDrops := func() {
		if unlogged > 0 {
			log.Warn("storage: writer buffer full, dropped oldest", "dropped", unlogged, "total", w.dropped.Load())
			if w.OnDrop != nil {
				w.OnDrop(int(unlogged))
			}
			unlogged = 0
		}
	}
	// startFlush hands the oldest batchSize items to a flush goroutine. The
	// rest stay in buf, where trim can still drop the oldest of them.
	startFlush := func() {
		if inflight > 0 || len(buf) == 0 {
			return
		}
		n := min(len(buf), w.batchSize)
		batch := append([]any(nil), buf[:n]...)
		buf = buf[n:]
		inflight = n
		go func() {
			fctx, cancel := context.WithTimeout(ctx, flushTimeout)
			defer cancel()
			results <- w.flush(fctx, log, batch)
		}()
	}
	trim := func() {
		if over := inflight + len(buf) - maxBuffered; over > 0 {
			over = min(over, len(buf))
			buf = buf[over:] // append reallocates as cap shrinks, releasing the old array
			drop(over)
		}
	}
	finish := func(failed []any) {
		inflight = 0
		failing = len(failed) > 0
		if !failing {
			w.failingSince.Store(0)
		} else if w.failingSince.Load() == 0 {
			w.failingSince.Store(clk.Now().UnixNano())
		}
		if failing {
			buf = append(failed, buf...)
			trim()
		}
	}

	for {
		select {
		case <-ctx.Done():
			if inflight > 0 {
				finish(<-results)
			}
			if len(buf) > 0 {
				fctx, cancel := context.WithTimeout(context.Background(), finalFlushTimeout)
				if lost := w.flush(fctx, log, buf); len(lost) > 0 {
					drop(len(lost))
					log.Error("storage: final flush failed, items lost", "n", len(lost))
				}
				cancel()
			}
			logDrops()
			return nil
		case failed := <-results:
			finish(failed)
			if !failing && len(buf) >= w.batchSize {
				startFlush()
			}
		case <-tick:
			tick = clk.After(w.flushEvery)
			failing = false
			logDrops()
			startFlush()
		case v, open := <-in:
			if !open {
				in = nil // closed input: stop receiving, keep flushing until ctx ends
				continue
			}
			item, ok := w.accept(v, lastMark)
			if !ok {
				continue
			}
			buf = append(buf, item)
			trim()
			if !failing && len(buf) >= w.batchSize {
				startFlush()
			}
		}
	}
}

// accept filters and normalizes one incoming value.
func (w *Writer) accept(v any, lastMark map[string]time.Time) (any, bool) {
	switch e := v.(type) {
	case market.KlineEvent:
		if !e.Closed {
			return nil, false
		}
		return e.Kline, true
	case market.Kline:
		if !e.Closed {
			return nil, false
		}
		return e, true
	case market.MarkPriceEvent:
		bucket := e.Time.Truncate(time.Minute)
		if last, ok := lastMark[e.Symbol]; ok && !bucket.After(last) {
			return nil, false
		}
		lastMark[e.Symbol] = bucket
		return e, true
	case market.OpenInterestEvent, market.LongShortEvent, market.LiquidationEvent, signal.Signal:
		return e, true
	}
	return nil, false
}

// flush writes batch grouped by type and returns the items whose write
// failed, in their original order.
func (w *Writer) flush(ctx context.Context, log *slog.Logger, batch []any) []any {
	var (
		ks   []market.Kline
		ms   []market.MarkPriceEvent
		ois  []market.OpenInterestEvent
		lss  []market.LongShortEvent
		liqs []market.LiquidationEvent
		sigs []signal.Signal
	)
	for _, v := range batch {
		switch e := v.(type) {
		case market.Kline:
			ks = append(ks, e)
		case market.MarkPriceEvent:
			ms = append(ms, e)
		case market.OpenInterestEvent:
			ois = append(ois, e)
		case market.LongShortEvent:
			lss = append(lss, e)
		case market.LiquidationEvent:
			liqs = append(liqs, e)
		case signal.Signal:
			sigs = append(sigs, e)
		}
	}
	type kind int
	const (
		kKline kind = iota
		kMark
		kOI
		kLS
		kLiq
		kSig
	)
	failed := map[kind]bool{}
	try := func(k kind, n int, write func() error) {
		if n == 0 {
			return
		}
		if err := write(); err != nil {
			failed[k] = true
			log.Warn("storage: flush failed, will retry", "n", n, "err", err)
		}
	}
	try(kKline, len(ks), func() error { return w.s.WriteKlines(ctx, ks) })
	try(kMark, len(ms), func() error { return w.s.WriteMarkPrices(ctx, ms) })
	try(kOI, len(ois), func() error { return w.s.WriteOI(ctx, ois) })
	try(kLS, len(lss), func() error { return w.s.WriteLongShort(ctx, lss) })
	try(kLiq, len(liqs), func() error { return w.s.WriteLiquidations(ctx, liqs) })
	try(kSig, len(sigs), func() error { return w.s.WriteSignals(ctx, sigs) })
	if len(failed) == 0 {
		return nil
	}
	var out []any
	for _, v := range batch {
		var k kind
		switch v.(type) {
		case market.Kline:
			k = kKline
		case market.MarkPriceEvent:
			k = kMark
		case market.OpenInterestEvent:
			k = kOI
		case market.LongShortEvent:
			k = kLS
		case market.LiquidationEvent:
			k = kLiq
		case signal.Signal:
			k = kSig
		}
		if failed[k] {
			out = append(out, v)
		}
	}
	return out
}
