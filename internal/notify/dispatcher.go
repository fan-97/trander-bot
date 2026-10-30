package notify

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/signal"
)

const (
	defaultQueueSize = 100
	// maxAttempts is the total number of Send attempts per channel.
	maxAttempts = 3
	// sendTimeout bounds each Send attempt. It is applied with
	// context.WithTimeout and therefore runs on the real clock, not the
	// injected one: it guards against a hung network call, which only
	// real time can measure.
	sendTimeout = 10 * time.Second
	// drainTimeout bounds (in real time) how long Run waits for queued
	// messages to be delivered after its input channel is closed.
	drainTimeout = 10 * time.Second
)

// backoff[i] is the wait before attempt i+2 (exponential); it has
// maxAttempts-1 entries.
var backoff = []time.Duration{time.Second, 2 * time.Second}

type item struct {
	msg      Message
	fallback bool // already a fallback delivery: never forwarded again
	system   bool // system alert: never forwarded to a fallback
}

// queue is a bounded FIFO that drops its oldest item when full.
type queue struct {
	mu      sync.Mutex
	items   []item
	size    int
	dropped int64
	wake    chan struct{} // capacity 1; signalled on push
}

// Dispatcher delivers formatted batches and system alerts to notifiers.
// Run only formats and enqueues; each channel has its own goroutine that
// does Send, retry with backoff and fallback forwarding, so a slow channel
// never blocks Run or the other channels.
type Dispatcher struct {
	// Logger receives delivery errors and drops. Set it before Run;
	// nil means slog.Default().
	Logger *slog.Logger

	ns       map[string]Notifier
	fallback map[string]string
	clk      clock.Clock
	queues   map[string]*queue

	// pending counts enqueued messages not yet fully handled; it lets Run
	// know when the queues are drained after its input is closed.
	pmu     sync.Mutex
	pending int
	idle    chan struct{} // capacity 1; signalled when pending drops to 0

	// onWait, when set (tests only), is called after a backoff wait has been
	// registered with clk.After.
	onWait func(time.Duration)
}

// NewDispatcher returns a Dispatcher for the notifiers ns (keyed by channel
// name). fallback maps a channel to its backup channel. queueSize <= 0 means 100.
func NewDispatcher(ns map[string]Notifier, fallback map[string]string, queueSize int, clk clock.Clock) *Dispatcher {
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	d := &Dispatcher{
		ns:       ns,
		fallback: fallback,
		clk:      clk,
		queues:   make(map[string]*queue, len(ns)),
		idle:     make(chan struct{}, 1),
	}
	for name := range ns {
		d.queues[name] = &queue{size: queueSize, wake: make(chan struct{}, 1)}
	}
	return d
}

func (d *Dispatcher) log() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// Dropped returns how many messages channel has dropped because its queue
// was full. Unknown channels return 0.
func (d *Dispatcher) Dropped(channel string) int64 {
	q, ok := d.queues[channel]
	if !ok {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// SendSystem enqueues a system alert on every channel. System alerts are not
// forwarded to fallback channels. It never blocks and is safe for concurrent use.
func (d *Dispatcher) SendSystem(m Message) {
	for name := range d.queues {
		d.enqueue(name, item{msg: m, system: true})
	}
}

func (d *Dispatcher) addPending(n int) {
	d.pmu.Lock()
	d.pending += n
	zero := d.pending == 0
	d.pmu.Unlock()
	if zero {
		select {
		case d.idle <- struct{}{}:
		default:
		}
	}
}

func (d *Dispatcher) isIdle() bool {
	d.pmu.Lock()
	defer d.pmu.Unlock()
	return d.pending == 0
}

// enqueue pushes it onto channel's queue without blocking, dropping the
// oldest queued message if the queue is full.
func (d *Dispatcher) enqueue(channel string, it item) {
	q, ok := d.queues[channel]
	if !ok {
		d.log().Warn("notify: unknown channel, message dropped", "channel", channel, "title", it.msg.Title)
		return
	}
	d.addPending(1)
	q.mu.Lock()
	dropped := false
	if len(q.items) >= q.size {
		q.items[0] = item{} // release the dropped message
		q.items = q.items[1:]
		q.dropped++
		dropped = true
	}
	q.items = append(q.items, it)
	q.mu.Unlock()
	if dropped {
		d.log().Warn("notify: queue full, dropped oldest message", "channel", channel)
		d.addPending(-1)
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *queue) pop() (item, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return item{}, false
	}
	it := q.items[0]
	q.items[0] = item{}
	q.items = q.items[1:]
	return it, true
}

// Run formats every batch from in and enqueues it on its channel. It returns
// nil when ctx is cancelled (after stopping the channel goroutines), or when
// in is closed and the queues have drained (waiting at most 10s real time).
func (d *Dispatcher) Run(ctx context.Context, in <-chan signal.Batch) error {
	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for name, n := range d.ns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.worker(wctx, name, n, d.queues[name])
		}()
	}
	shutdown := func() {
		stop()
		wg.Wait()
	}

	for {
		select {
		case <-ctx.Done():
			shutdown()
			return nil
		case b, ok := <-in:
			if !ok {
				d.drain(ctx)
				shutdown()
				return nil
			}
			if len(b.Signals) == 0 {
				// The Batcher never emits an empty batch; ignore one defensively.
				continue
			}
			d.enqueue(b.Channel, item{msg: Format(b)})
		}
	}
}

// drain waits until every queued message has been handled, ctx is done, or
// drainTimeout elapses.
func (d *Dispatcher) drain(ctx context.Context) {
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	for !d.isIdle() {
		select {
		case <-d.idle:
		case <-timer.C:
			d.log().Warn("notify: drain timed out, undelivered messages discarded")
			return
		case <-ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) worker(ctx context.Context, name string, n Notifier, q *queue) {
	for {
		it, ok := q.pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
				continue
			}
		}
		d.deliver(ctx, name, n, it)
		d.addPending(-1)
		if ctx.Err() != nil {
			return
		}
	}
}

// deliver sends it with retries; after the final failure it forwards the
// message once to the channel's fallback.
func (d *Dispatcher) deliver(ctx context.Context, name string, n Notifier, it item) {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := backoff[attempt-1]
			ch := d.clk.After(wait)
			if d.onWait != nil {
				d.onWait(wait)
			}
			select {
			case <-ch:
			case <-ctx.Done():
				return
			}
		}
		actx, cancel := context.WithTimeout(ctx, sendTimeout)
		err = n.Send(actx, it.msg)
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		d.log().Warn("notify: send failed", "channel", name, "attempt", attempt+1, "err", err)
	}
	d.log().Error("notify: delivery failed", "channel", name, "attempts", maxAttempts, "err", err, "title", it.msg.Title)
	if it.fallback || it.system {
		return
	}
	fb := d.fallback[name]
	if fb == "" {
		return
	}
	if _, ok := d.queues[fb]; !ok {
		d.log().Error("notify: unknown fallback channel, message dropped", "channel", name, "fallback", fb)
		return
	}
	d.enqueue(fb, item{msg: it.msg, fallback: true})
}
