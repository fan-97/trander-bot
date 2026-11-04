package main

import (
	"context"
	"log/slog"
	"reflect"
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

// autoSource serves every symbol in growth; a symbol's recent volume is
// growth times its baseline.
type autoSource struct {
	mu     sync.Mutex
	growth map[string]float64
	calls  int
}

func (s *autoSource) set(g map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.growth = g
}

func (s *autoSource) Contracts(context.Context) ([]binance.Contract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	var out []binance.Contract
	for sym := range s.growth {
		out = append(out, binance.Contract{Symbol: sym, BaseAsset: strings.TrimSuffix(sym, "USDT"), UnderlyingType: "COIN"})
	}
	return out, nil
}

func (s *autoSource) Tickers24h(context.Context) ([]binance.Ticker24h, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []binance.Ticker24h
	for sym := range s.growth {
		out = append(out, binance.Ticker24h{Symbol: sym, QuoteVolume: 1e9})
	}
	return out, nil
}

func (s *autoSource) Klines(_ context.Context, sym string, _ market.Interval, start time.Time, n int) ([]market.Kline, error) {
	s.mu.Lock()
	g := s.growth[sym]
	s.mu.Unlock()
	var out []market.Kline
	for i := 0; i < n-1; i++ { // closed days; the recent 3 carry the growth
		v := 100.0
		if i >= n-1-3 {
			v *= g
		}
		out = append(out, market.Kline{Symbol: sym, Interval: market.I1d, OpenTime: start.AddDate(0, 0, i),
			Open: 1, Close: 1, QuoteVolume: v, Closed: true})
	}
	return out, nil
}

func autoCfg(t *testing.T, enabled bool) *config.Config {
	t.Helper()
	c, err := config.Parse([]byte(`symbols: [BTCUSDT]
channels: {w: {type: webhook, params: {url: "http://x"}}}
rules: [{id: r, interval: 5m, when: "close > 1", channels: [w]}]
auto_symbols: {enabled: ` + map[bool]string{true: "true", false: "false"}[enabled] + `, top: 1, keep_rank: 1, days: 3, base_days: 4, min_quote_volume: 0, min_listing_days: 0, refresh: 1h}
`))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAutoLoop(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC))
	src := &autoSource{growth: map[string]float64{"AUSDT": 3, "BUSDT": 2, "BTCUSDT": 9}}
	raw := config.NewStore(autoCfg(t, true))
	out := make(chan []string, 4)
	var mu sync.Mutex
	var titles []string
	alert := func(m notify.Message) {
		mu.Lock()
		titles = append(titles, m.Title+": "+strings.Join(m.Lines, " | "))
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sub := raw.Subscribe()
	go func() {
		defer close(done)
		autoLoop(ctx, clk, src, raw, sub, nil, false, out, alert, slog.Default())
	}()
	t.Cleanup(func() { cancel(); <-done })

	recv := func(advance bool) []string {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if advance {
				clk.Advance(time.Hour)
			}
			select {
			case v := <-out:
				return v
			case <-time.After(20 * time.Millisecond):
			}
		}
		t.Fatal("no selection")
		return nil
	}

	// First run is immediate; the manual BTCUSDT is never auto-selected.
	if got := recv(false); !reflect.DeepEqual(got, []string{"AUSDT"}) {
		t.Fatalf("first selection %v", got)
	}
	mu.Lock()
	if len(titles) != 1 || !strings.Contains(titles[0], "自动币种更新") || !strings.Contains(titles[0], "+AUSDT #1 量增 ×3.00") {
		t.Errorf("alerts %q", titles)
	}
	mu.Unlock()

	// After refresh B overtakes A (keep_rank 1: A is not kept).
	src.set(map[string]float64{"AUSDT": 1.5, "BUSDT": 4, "BTCUSDT": 9})
	if got := recv(true); !reflect.DeepEqual(got, []string{"BUSDT"}) {
		t.Fatalf("refreshed selection %v", got)
	}

	// Disabling on reload removes the auto symbols at once.
	raw.Set(autoCfg(t, false))
	if got := recv(false); len(got) != 0 {
		t.Fatalf("disabled selection %v", got)
	}
	mu.Lock()
	if last := titles[len(titles)-1]; !strings.Contains(last, "自动选币已关闭") || !strings.Contains(last, "BUSDT") {
		t.Errorf("disable alert %q", last)
	}
	mu.Unlock()

	// Re-enabling re-selects immediately, without waiting for refresh.
	raw.Set(autoCfg(t, true))
	if got := recv(false); !reflect.DeepEqual(got, []string{"BUSDT"}) {
		t.Fatalf("re-enabled selection %v", got)
	}
}

func TestFilterSymbols_MergesAuto(t *testing.T) {
	c := autoCfg(t, true)
	c.Rules[0].Symbols = []string{config.ManualSymbols, "btcusdt"}
	have := map[string]bool{"BTCUSDT": true, "WIFUSDT": true, "SOLUSDT": true}
	got, _ := filterSymbols(c, have, []string{"WIFUSDT", "BTCUSDT", "GONEUSDT", "SOLUSDT"})
	if !reflect.DeepEqual(got.Symbols, []string{"BTCUSDT", "WIFUSDT", "SOLUSDT"}) {
		t.Errorf("pool %v", got.Symbols)
	}
	// BTCUSDT is manual and stays manual; a delisted auto pick is dropped.
	if !reflect.DeepEqual(got.Auto, []string{"WIFUSDT", "SOLUSDT"}) {
		t.Errorf("auto %v", got.Auto)
	}
	if !reflect.DeepEqual(got.Rules[0].Symbols, []string{"manual", "BTCUSDT"}) {
		t.Errorf("rule symbols %v", got.Rules[0].Symbols)
	}
	if len(got.RulesFor("WIFUSDT")) != 0 || len(got.RulesFor("BTCUSDT")) != 1 {
		t.Error("manual-only rule applied to an auto symbol")
	}
	if c.Auto != nil {
		t.Error("input config mutated")
	}
}
