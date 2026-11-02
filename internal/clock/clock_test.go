package clock

import (
	"sync"
	"testing"
	"time"
)

func TestFake_AdvanceFiresAfter(t *testing.T) {
	start := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	f := NewFake(start)

	ch := f.After(5 * time.Second)

	f.Advance(4 * time.Second)
	select {
	case <-ch:
		t.Fatal("After(5s) fired after only 4s")
	default:
	}

	f.Advance(1 * time.Second)
	select {
	case <-ch:
	default:
		t.Fatal("After(5s) did not fire after 5s total")
	}

	if got, want := f.Now(), start.Add(5*time.Second); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
}

func TestFake_AfterNonPositiveFiresImmediately(t *testing.T) {
	f := NewFake(time.Unix(0, 0))

	for _, d := range []time.Duration{0, -time.Second} {
		select {
		case <-f.After(d):
		default:
			t.Fatalf("After(%v) did not fire immediately", d)
		}
	}
}

func TestFake_ConcurrentUse(t *testing.T) {
	f := NewFake(time.Unix(0, 0))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = f.After(time.Second)
			_ = f.Now()
		}()
		go func() {
			defer wg.Done()
			f.Advance(time.Millisecond)
		}()
	}
	wg.Wait()
}

func TestReal_NowAndAfter(t *testing.T) {
	var c Clock = Real{}
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("Real.Now() is earlier than time.Now() taken before the call")
	}
	select {
	case <-c.After(time.Millisecond):
	case <-time.After(5 * time.Second):
		t.Fatal("Real.After(1ms) did not fire")
	}
}
