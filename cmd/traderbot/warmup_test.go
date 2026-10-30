package main

import (
	"context"
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
	return warmup(rest, stream, config.NewStore(cfg), clk, alerts.add, slog.Default()), alerts
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
