package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/market"
	"trader-bot/internal/notify"
)

type alertLog struct {
	mu sync.Mutex
	ms []notify.Message
}

func (a *alertLog) add(m notify.Message) { a.mu.Lock(); a.ms = append(a.ms, m); a.mu.Unlock() }
func (a *alertLog) titles() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, m := range a.ms {
		out = append(out, m.Title)
	}
	return out
}

// warmupFixture: 1m klines are served at once; 5m requests are handled by slow.
func warmupFixture(t *testing.T, slow http.HandlerFunc) (func(context.Context, string) (map[market.Interval][]market.Kline, error), *alertLog) {
	t.Helper()
	now := time.Date(2026, 10, 31, 12, 0, 30, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("interval") != "1m" {
			slow(w, r)
			return
		}
		open := now.Truncate(time.Minute).Add(-time.Minute)
		fmt.Fprintf(w, `[[%d,"1","1","1","1","1",%d,"1"]]`, open.UnixMilli(), open.Add(time.Minute).UnixMilli()-1)
	}))
	t.Cleanup(srv.Close)
	clk := clock.NewFake(now)
	rest := binance.NewREST(srv.URL, 0)
	rest.Clock = clk
	cfg, err := config.Parse([]byte(`
symbols: [BTCUSDT]
rules:
  - {id: r, interval: 5m, when: close > 1, channels: [w]}
channels:
  w: {type: webhook, params: {url: "http://x"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	stream := binance.NewStream("ws://127.0.0.1:1/market", rest, clk, nil)
	alerts := &alertLog{}
	return warmup(rest, stream, config.NewStore(cfg), clk, alerts.add, nil, slog.Default()), alerts
}

func TestWarmup_TimeoutReturnsPartialHistoryAndAlerts(t *testing.T) {
	old := warmupTimeout
	warmupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { warmupTimeout = old })
	hist, alerts := warmupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // 5m request hangs past the timeout
	})
	got, err := hist(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatalf("err = %v, want partial history", err)
	}
	if len(got[market.I1m]) != 1 {
		t.Errorf("1m bars = %d, want the 1 already fetched", len(got[market.I1m]))
	}
	if _, ok := got[market.I5m]; ok {
		t.Error("5m present although its fetch timed out")
	}
	if a := alerts.titles(); len(a) != 1 || a[0] != "预热失败: BTCUSDT" {
		t.Errorf("alerts = %v", a)
	}
}

func TestWarmup_ErrorAlertsAndStartsCold(t *testing.T) {
	hist, alerts := warmupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := hist(context.Background(), "BTCUSDT"); err == nil || !strings.Contains(err.Error(), "5m") {
		t.Fatalf("err = %v, want the 5m failure", err)
	}
	if a := alerts.titles(); len(a) != 1 || a[0] != "预热失败: BTCUSDT" {
		t.Errorf("alerts = %v", a)
	}
}

// I7: fetched warmup bars are also handed to persist (the database writer).
func TestWarmup_PersistsFetchedBars(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 30, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := time.Minute
		if r.URL.Query().Get("interval") == "5m" {
			d = 5 * time.Minute
		}
		open := now.Truncate(d).Add(-d)
		fmt.Fprintf(w, `[[%d,"1","1","1","1","1",%d,"1"]]`, open.UnixMilli(), open.Add(d).UnixMilli()-1)
	}))
	t.Cleanup(srv.Close)
	clk := clock.NewFake(now)
	rest := binance.NewREST(srv.URL, 0)
	rest.Clock = clk
	cfg, err := config.Parse([]byte("symbols: [BTCUSDT]\nrules:\n  - {id: r, interval: 5m, when: close > 1, channels: [w]}\nchannels:\n  w: {type: webhook, params: {url: \"http://x\"}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted []market.Kline
	hist := warmup(rest, binance.NewStream("ws://127.0.0.1:1/market", rest, clk, nil), config.NewStore(cfg), clk,
		func(notify.Message) {}, func(ks []market.Kline) { persisted = append(persisted, ks...) }, slog.Default())
	if _, err := hist(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	ivs := map[market.Interval]bool{}
	for _, k := range persisted {
		ivs[k.Interval] = true
	}
	if len(persisted) != 2 || !ivs[market.I1m] || !ivs[market.I5m] {
		t.Fatalf("persisted = %+v, want one 1m and one 5m bar", persisted)
	}
}

// I8: 预热失败 alerts within the window go out as one message; a lone one
// keeps its per-symbol title; other alerts pass straight through.
func TestBatchWarmupAlerts(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	got := &alertLog{}
	alert := batchWarmupAlerts(context.Background(), clk, 5*time.Second, got.add)
	for _, s := range []string{"AUSDT", "BUSDT", "CUSDT"} {
		alert(notify.Message{Title: "预热失败: " + s, Lines: []string{"x", "boom"}})
	}
	alert(notify.Message{Title: "其它"})
	if a := got.titles(); len(a) != 1 || a[0] != "其它" {
		t.Fatalf("before window: %v", a)
	}
	eventuallyMain(t, func() bool { clk.Advance(5 * time.Second); return len(got.titles()) == 2 })
	if a := got.titles(); a[1] != "预热失败: 3 个币种" || !strings.Contains(strings.Join(got.ms[1].Lines, " "), "AUSDT, BUSDT, CUSDT") {
		t.Fatalf("aggregated = %v %v", a, got.ms[1].Lines)
	}
	alert(notify.Message{Title: "预热失败: DUSDT", Lines: []string{"x", "boom"}})
	eventuallyMain(t, func() bool { clk.Advance(5 * time.Second); return len(got.titles()) == 3 })
	if a := got.titles(); a[2] != "预热失败: DUSDT" {
		t.Fatalf("lone failure = %v", a)
	}
}

// I6: a writer failing for dbFailingAfter raises one critical alert, then
// one recovery when it succeeds again.
func TestWatchWriter(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	fw := &fakeFailing{}
	got := &alertLog{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { watchWriter(ctx, clk, fw, got.add, slog.Default()); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	fw.set(clk.Now())
	eventuallyMain(t, func() bool { clk.Advance(dbCheckEvery); return len(got.titles()) == 1 })
	if a := got.titles(); a[0] != "数据库写入失败" || clk.Now().Sub(time.Unix(0, 0)) < dbFailingAfter {
		t.Fatalf("alerts = %v at %v", a, clk.Now())
	}
	fw.set(time.Time{})
	eventuallyMain(t, func() bool { clk.Advance(dbCheckEvery); return len(got.titles()) == 2 })
	if a := got.titles(); a[1] != "数据库写入恢复" {
		t.Fatalf("alerts = %v", a)
	}
}

type fakeFailing struct {
	mu    sync.Mutex
	since time.Time
}

func (f *fakeFailing) set(t time.Time) { f.mu.Lock(); f.since = t; f.mu.Unlock() }
func (f *fakeFailing) FailingSince() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.since
}

func eventuallyMain(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestWarmup_ShutdownDoesNotAlert(t *testing.T) {
	hist, alerts := warmupFixture(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	if _, err := hist(ctx, "BTCUSDT"); err == nil {
		t.Fatal("want an error on cancel")
	}
	if a := alerts.titles(); len(a) != 0 {
		t.Errorf("alerts on shutdown: %v", a)
	}
}

type lastTimeSink struct {
	Store
	last map[market.Interval]time.Time
	err  error
}

func (s lastTimeSink) LastKlineTime(_ context.Context, _ string, iv market.Interval) (time.Time, bool, error) {
	t, ok := s.last[iv]
	return t, ok, s.err
}

// A restart re-fetches hundreds of bars per interval; only those newer than
// the database's latest are written, so warmup does not flood the writer or
// rewrite (compressed) history.
func TestUnstored(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ks := make([]market.Kline, 5)
	for i := range ks {
		ks[i] = market.Kline{Symbol: "BTCUSDT", Interval: market.I1h, OpenTime: base.Add(time.Duration(i) * time.Hour), Closed: true}
	}
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)

	got := unstored(ctx, lastTimeSink{last: map[market.Interval]time.Time{market.I1h: base.Add(2 * time.Hour)}}, ks, log)
	if len(got) != 2 || !got[0].OpenTime.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("got %d bars from %v, want 2 from +3h", len(got), got)
	}
	if got := unstored(ctx, lastTimeSink{last: map[market.Interval]time.Time{}}, ks, log); len(got) != 5 {
		t.Fatalf("empty table: got %d, want all 5", len(got))
	}
	if got := unstored(ctx, lastTimeSink{err: errors.New("db down")}, ks, log); len(got) != 5 {
		t.Fatalf("lookup error: got %d, want all 5 (old behaviour)", len(got))
	}
	if got := unstored(ctx, &memSink{}, ks, log); len(got) != 5 {
		t.Fatalf("store without lookup: got %d, want all 5", len(got))
	}
}
