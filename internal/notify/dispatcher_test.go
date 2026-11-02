package notify

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

// fakeNotifier records every Send call and returns results from a script.
type fakeNotifier struct {
	name  string
	mu    sync.Mutex
	calls []Message
	// fail returns the error for the n-th (0-based) call; nil means success.
	fail func(n int) error
	// block, if set, makes Send wait until ctx is done.
	block bool
	sent  chan Message // every call is reported here (buffered)
}

func newFake(name string) *fakeNotifier {
	return &fakeNotifier{name: name, sent: make(chan Message, 64)}
}

func (f *fakeNotifier) Name() string { return f.name }

func (f *fakeNotifier) Send(ctx context.Context, m Message) error {
	f.mu.Lock()
	n := len(f.calls)
	f.calls = append(f.calls, m)
	f.mu.Unlock()
	f.sent <- m
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.fail != nil {
		return f.fail(n)
	}
	return nil
}

func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
	var zero T
	return zero
}

func noRecv[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %v", what, v)
	case <-time.After(50 * time.Millisecond):
	}
}

func isChannelAlert(m Message) bool { return strings.HasPrefix(m.Title, "渠道失败: ") }

// recvMsg is recv that skips 渠道失败 alerts.
func recvMsg(t *testing.T, ch <-chan Message, what string) Message {
	t.Helper()
	for {
		if m := recv(t, ch, what); !isChannelAlert(m) {
			return m
		}
	}
}

// noRecvMsg is noRecv that tolerates 渠道失败 alerts.
func noRecvMsg(t *testing.T, ch <-chan Message, what string) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case m := <-ch:
			if !isChannelAlert(m) {
				t.Fatalf("unexpected %s: %v", what, m)
			}
		case <-deadline:
			return
		}
	}
}

func batch(ch, sym string) signal.Batch {
	return signal.Batch{Channel: ch, Signals: []signal.Signal{{
		RuleID: "r1", Symbol: sym, Interval: market.I5m, Level: config.LevelWarn, Price: 1, Detail: "x",
	}}}
}

type harness struct {
	d      *Dispatcher
	clk    *clock.Fake
	waits  chan time.Duration
	in     chan signal.Batch
	cancel context.CancelFunc
	done   chan error
}

func start(t *testing.T, ns map[string]Notifier, fb map[string]string, qsize int, opts ...func(*Dispatcher)) *harness {
	t.Helper()
	h := &harness{
		clk:   clock.NewFake(time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)),
		waits: make(chan time.Duration, 64),
		in:    make(chan signal.Batch),
		done:  make(chan error, 1),
	}
	h.d = NewDispatcher(ns, fb, qsize, h.clk)
	h.d.onWait = func(d time.Duration) { h.waits <- d }
	for _, o := range opts {
		o(h.d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.d.Run(ctx, h.in) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return h
}

// advanceWait waits for the dispatcher to register a backoff wait, checks
// its duration and advances the fake clock past it.
func (h *harness) advanceWait(t *testing.T, want time.Duration) {
	t.Helper()
	got := recv(t, h.waits, "backoff wait")
	if got != want {
		t.Fatalf("backoff = %v, want %v", got, want)
	}
	h.clk.Advance(got)
}

func TestDispatcher_RetryThenSuccess(t *testing.T) {
	a := newFake("a")
	a.fail = func(n int) error {
		if n < 2 {
			return errors.New("boom")
		}
		return nil
	}
	h := start(t, map[string]Notifier{"a": a}, nil, 0)
	h.in <- batch("a", "BTCUSDT")

	recv(t, a.sent, "attempt 1")
	h.advanceWait(t, time.Second)
	recv(t, a.sent, "attempt 2")
	h.advanceWait(t, 2*time.Second)
	recv(t, a.sent, "attempt 3")
	noRecv(t, a.sent, "4th attempt")
	noRecv(t, h.waits, "wait after success")
	if a.count() != 3 {
		t.Fatalf("calls = %d, want 3", a.count())
	}
}

func TestDispatcher_FallbackAfter3Failures(t *testing.T) {
	a := newFake("a")
	a.fail = func(int) error { return errors.New("down") }
	b := newFake("b")
	h := start(t, map[string]Notifier{"a": a, "b": b}, map[string]string{"a": "b"}, 0)
	h.in <- batch("a", "ETHUSDT")

	recv(t, a.sent, "attempt 1")
	h.advanceWait(t, time.Second)
	recv(t, a.sent, "attempt 2")
	h.advanceWait(t, 2*time.Second)
	recv(t, a.sent, "attempt 3")

	m := recvMsg(t, b.sent, "fallback delivery")
	if m.Title != "[WARN] 1 signal(s)" {
		t.Fatalf("fallback title = %q", m.Title)
	}
	noRecvMsg(t, b.sent, "second fallback delivery")
	// b gets the fallback plus one 渠道失败 alert about a.
	if a.count() != 3 || b.count() != 2 {
		t.Fatalf("calls a=%d b=%d, want 3/2", a.count(), b.count())
	}
}

func TestDispatcher_FallbackNotCascaded(t *testing.T) {
	down := func(int) error { return errors.New("down") }
	a, b, c := newFake("a"), newFake("b"), newFake("c")
	a.fail, b.fail = down, down
	h := start(t, map[string]Notifier{"a": a, "b": b, "c": c},
		map[string]string{"a": "b", "b": "c"}, 0)
	h.in <- batch("a", "BTCUSDT")

	// Advance every backoff wait as soon as it is registered.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case d := <-h.waits:
				h.clk.Advance(d)
			case <-stop:
				return
			}
		}
	}()

	deadline := time.After(2 * time.Second)
	for a.count() < 3 || b.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("calls a=%d b=%d, want 3/3", a.count(), b.count())
		case <-time.After(5 * time.Millisecond):
		}
	}
	noRecvMsg(t, c.sent, "cascaded delivery to c")
}

func TestDispatcher_SlowChannelDoesNotBlock(t *testing.T) {
	a := newFake("a")
	a.block = true
	b := newFake("b")
	h := start(t, map[string]Notifier{"a": a, "b": b}, nil, 0)

	h.in <- batch("a", "BTCUSDT")
	recv(t, a.sent, "a send started")
	h.in <- batch("a", "BTCUSDT") // queued behind the blocked send
	begin := time.Now()
	h.in <- batch("b", "ETHUSDT")
	recv(t, b.sent, "b delivery")
	if el := time.Since(begin); el > 100*time.Millisecond {
		t.Fatalf("b delivery took %v, want < 100ms", el)
	}
}

func TestDispatcher_QueueOverflowDropsOldest(t *testing.T) {
	a := newFake("a")
	a.block = true
	h := start(t, map[string]Notifier{"a": a}, nil, 2)

	h.in <- batch("a", "S0")
	recv(t, a.sent, "S0 send started") // worker is now stuck in S0
	h.in <- batch("a", "S1")
	h.in <- batch("a", "S2")
	h.in <- batch("a", "S3") // queue holds S1,S2 -> S1 dropped

	deadline := time.After(2 * time.Second)
	for h.d.Dropped("a") != 1 {
		select {
		case <-deadline:
			t.Fatalf("Dropped = %d, want 1", h.d.Dropped("a"))
		case <-time.After(5 * time.Millisecond):
		}
	}
	q := h.d.queues["a"]
	q.mu.Lock()
	var syms []string
	for _, it := range q.items {
		syms = append(syms, it.msg.Links[0].URL)
	}
	q.mu.Unlock()
	if len(syms) != 2 || syms[0] != binanceURL("S2") || syms[1] != binanceURL("S3") {
		t.Fatalf("queue = %v, want S2,S3", syms)
	}
	if h.d.Dropped("unknown") != 0 {
		t.Fatal("unknown channel dropped != 0")
	}
}

func TestDispatcher_SendSystemReachesAllChannels(t *testing.T) {
	a, b := newFake("a"), newFake("b")
	b.fail = func(int) error { return errors.New("down") }
	c := newFake("c")
	h := start(t, map[string]Notifier{"a": a, "b": b, "c": c}, map[string]string{"b": "c"}, 0)

	m := Message{Title: "WS disconnected", Level: config.LevelCritical}
	h.d.SendSystem(m)
	for _, f := range []*fakeNotifier{a, b, c} {
		got := recv(t, f.sent, f.name+" system message")
		if got.Title != m.Title {
			t.Fatalf("%s got %q", f.name, got.Title)
		}
	}
	h.advanceWait(t, time.Second)
	recv(t, b.sent, "b attempt 2")
	h.advanceWait(t, 2*time.Second)
	recv(t, b.sent, "b attempt 3")
	noRecvMsg(t, c.sent, "system message forwarded to fallback")
}

// I4: a channel whose delivery finally fails raises one 渠道失败 alert on
// every OTHER channel (not on itself), at most once per 10 minutes, and is
// reported by Failures/Failing.
func TestDispatcher_ChannelFailureAlertsOthers(t *testing.T) {
	a, b := newFake("a"), newFake("b")
	a.fail = func(int) error { return errors.New("401 unauthorized") }
	h := start(t, map[string]Notifier{"a": a, "b": b}, nil, 0)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case d := <-h.waits:
				h.clk.Advance(d)
			case <-stop:
				return
			}
		}
	}()

	for i := 0; i < failingAfter; i++ {
		h.in <- batch("a", "BTCUSDT")
	}
	m := recv(t, b.sent, "渠道失败 alert on b")
	if m.Title != "渠道失败: a" || !strings.Contains(strings.Join(m.Lines, " "), "401") {
		t.Fatalf("alert = %+v", m)
	}
	deadline := time.After(2 * time.Second)
	for h.d.Failures("a") != failingAfter {
		select {
		case <-deadline:
			t.Fatalf("Failures(a) = %d, want %d", h.d.Failures("a"), failingAfter)
		case <-time.After(5 * time.Millisecond):
		}
	}
	noRecv(t, b.sent, "second 渠道失败 alert inside 10 minutes")
	a.mu.Lock()
	aCalls := slices.Clone(a.calls)
	a.mu.Unlock()
	for _, f := range aCalls {
		if isChannelAlert(f) {
			t.Fatalf("failing channel alerted about itself: %+v", f)
		}
	}
	if got := h.d.Failing(); len(got) != 0 {
		t.Fatalf("Failing before the window elapsed = %v", got)
	}
	h.clk.Advance(failingWindow)
	if got := h.d.Failing(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Failing = %v, want [a]", got)
	}
	if h.d.Failures("b") != 0 || len(h.d.Failing()) != 1 {
		t.Fatal("healthy channel b reported as failing")
	}
}

func TestDispatcher_ReturnsWhenInputClosed(t *testing.T) {
	a := newFake("a")
	clk := clock.NewFake(time.Unix(0, 0))
	d := NewDispatcher(map[string]Notifier{"a": a}, nil, 0, clk)
	in := make(chan signal.Batch, 2)
	in <- batch("a", "BTCUSDT")
	in <- batch("a", "ETHUSDT")
	close(in)
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), in) }()
	err := recv(t, done, "Run return")
	if err != nil {
		t.Fatalf("Run = %v", err)
	}
	if a.count() != 2 {
		t.Fatalf("calls = %d, want 2 (queue drained)", a.count())
	}
}

func TestFormat(t *testing.T) {
	ts := time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC)
	b := signal.Batch{Channel: "tg", Signals: []signal.Signal{
		{RuleID: "pump_with_volume", Symbol: "BTCUSDT", Interval: market.I5m, Level: config.LevelWarn,
			Time: ts, Price: 67321.5, Detail: "change_pct(1)=3.4 > 3; volume_ratio(20)=2.1 > 2"},
		{RuleID: "rsi_low", Symbol: "ETHUSDT", Interval: market.I1h, Level: config.LevelCritical,
			Time: ts, Price: 2400, Detail: ""},
		{RuleID: "dip", Symbol: "BTCUSDT", Interval: market.I1m, Level: config.LevelInfo,
			Time: ts, Price: 0.000123, Detail: "close<x"},
	}}
	m := Format(b)
	if m.Title != "[CRITICAL] 3 signal(s)" {
		t.Errorf("Title = %q", m.Title)
	}
	if m.Level != config.LevelCritical {
		t.Errorf("Level = %q", m.Level)
	}
	wantLines := []string{
		"[5m] BTCUSDT pump_with_volume @ 67321.5 | change_pct(1)=3.4 > 3; volume_ratio(20)=2.1 > 2",
		"[1h] ETHUSDT rsi_low @ 2400 | rsi_low",
		"[1m] BTCUSDT dip @ 0.000123 | close<x",
	}
	if len(m.Lines) != len(wantLines) {
		t.Fatalf("Lines = %q", m.Lines)
	}
	for i := range wantLines {
		if m.Lines[i] != wantLines[i] {
			t.Errorf("Lines[%d] = %q, want %q", i, m.Lines[i], wantLines[i])
		}
	}
	wantLinks := []Link{
		{Text: "Binance BTCUSDT", URL: "https://www.binance.com/zh-CN/futures/BTCUSDT"},
		{Text: "TradingView BTCUSDT", URL: "https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSDT.P"},
		{Text: "Binance ETHUSDT", URL: "https://www.binance.com/zh-CN/futures/ETHUSDT"},
		{Text: "TradingView ETHUSDT", URL: "https://www.tradingview.com/chart/?symbol=BINANCE:ETHUSDT.P"},
	}
	if len(m.Links) != len(wantLinks) {
		t.Fatalf("Links = %v", m.Links)
	}
	for i := range wantLinks {
		if m.Links[i] != wantLinks[i] {
			t.Errorf("Links[%d] = %v, want %v", i, m.Links[i], wantLinks[i])
		}
	}

	single := Format(batch("tg", "SOLUSDT"))
	if single.Title != "[WARN] 1 signal(s)" {
		t.Errorf("single Title = %q", single.Title)
	}
}

func TestDispatcher_CancelDuringBackoffReturnsPromptly(t *testing.T) {
	a := newFake("a")
	a.fail = func(int) error { return errors.New("down") }
	h := start(t, map[string]Notifier{"a": a}, nil, 0)
	h.in <- batch("a", "BTCUSDT")
	recv(t, a.sent, "attempt 1")
	recv(t, h.waits, "backoff wait") // worker is now parked in clk.After(1s)

	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
		h.done <- err // let the cleanup see it too
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return promptly after cancel during backoff")
	}
	if a.count() != 1 {
		t.Fatalf("calls = %d, want 1", a.count())
	}
}

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestDispatcher_UnknownFallbackLoggedAndDropped(t *testing.T) {
	var logs syncBuf
	a := newFake("a")
	a.fail = func(int) error { return errors.New("down") }
	h := start(t, map[string]Notifier{"a": a}, map[string]string{"a": "nope"}, 0,
		func(d *Dispatcher) { d.Logger = slog.New(slog.NewTextHandler(&logs, nil)) })
	h.in <- batch("a", "BTCUSDT")
	recv(t, a.sent, "attempt 1")
	h.advanceWait(t, time.Second)
	recv(t, a.sent, "attempt 2")
	h.advanceWait(t, 2*time.Second)
	recv(t, a.sent, "attempt 3")

	deadline := time.After(2 * time.Second)
	for !strings.Contains(logs.String(), "unknown fallback") {
		select {
		case <-deadline:
			t.Fatalf("no unknown-fallback log; logs:\n%s", logs.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if !strings.Contains(logs.String(), "fallback=nope") {
		t.Fatalf("log lacks fallback name:\n%s", logs.String())
	}
	noRecv(t, a.sent, "re-delivery to a")
	if h.d.Dropped("a") != 0 || h.d.Dropped("nope") != 0 {
		t.Fatal("unknown fallback must not count as queue drop")
	}
}

func TestDispatcher_EmptyBatchIgnored(t *testing.T) {
	a := newFake("a")
	h := start(t, map[string]Notifier{"a": a}, nil, 0)
	h.in <- signal.Batch{Channel: "a"}
	noRecv(t, a.sent, "delivery of empty batch")
}

func TestFormat_EmptyBatch(t *testing.T) {
	m := Format(signal.Batch{Channel: "a"})
	if m.Title != "[INFO] 0 signal(s)" || m.Level != config.LevelInfo || len(m.Lines) != 0 || len(m.Links) != 0 {
		t.Fatalf("Format(empty) = %+v", m)
	}
}
