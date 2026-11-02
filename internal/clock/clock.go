// Package clock abstracts time so that all business code obtains "now" and
// timers through an injected Clock, which tests can control.
package clock

import (
	"sync"
	"time"
)

// Clock is the time source used by business code.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// Real is the production Clock backed by the time package.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// After waits for d to elapse and then sends the current time on the returned channel.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Fake is a manually advanced Clock for tests. It is safe for concurrent use.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFake returns a Fake whose current time is t.
func NewFake(t time.Time) *Fake {
	return &Fake{now: t}
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After returns a channel that fires once the fake clock has been advanced by d.
// A non-positive d fires immediately.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, waiter{deadline: f.now.Add(d), ch: ch})
	return ch
}

// Advance moves the fake time forward by d and fires every After channel whose
// deadline has been reached.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)
	pending := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.deadline.After(f.now) {
			// Buffered with capacity 1 and sent exactly once, so this never blocks.
			w.ch <- f.now
			continue
		}
		pending = append(pending, w)
	}
	f.waiters = pending
}
