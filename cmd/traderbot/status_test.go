package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/health"
	"trader-bot/internal/notify"
	"trader-bot/internal/rule"
	"trader-bot/internal/signal"
	"trader-bot/internal/universe"
)

func TestStatusHandler(t *testing.T) {
	c, err := config.Parse([]byte(`symbols: [BTCUSDT]
channels: {w: {type: webhook, params: {url: "http://x"}}}
auto_symbols: {top: 5}
notify: {tz: UTC}
rules:
  - id: pb
    interval: 15m
    setup: {when: "low@1h <= ema(20)@4h", within: 8h}
    when: {all: ["close > highest(20)", "volume_ratio(20) > 2"]}
    targets: {side: long, stop: 1.5, take: [2]}
    channels: [w]
  - {id: manual_only, symbols: manual, interval: 1h, when: "rsi(14) < 30", channels: [w]}
  - {id: off, enabled: false, interval: 1h, when: "close > 1", channels: [w]}
`))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{"BTCUSDT": true, "WIFUSDT": true}
	running, _ := filterSymbols(c, have, []string{"WIFUSDT"})
	store := config.NewStore(running)

	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(now)
	gate := signal.NewGate(clk)
	gate.Observe("pb", "WIFUSDT", rule.True, 0)
	mon := health.NewMonitor(clk, func(notify.Message) {}, time.Minute)
	mon.SetSymbols(running.Symbols)
	auto := &autoState{}
	auto.ran(now.Add(-time.Hour), now.Add(5*time.Hour), []universe.Row{
		{Symbol: "WIFUSDT", Rank: 1, Score: 3.4, Growth: 3.4, AvgVolume: 1.2e8, ChangePct: 12, Selected: true},
		{Symbol: "XUSDT", Rank: 2},
	})

	rec := httptest.NewRecorder()
	statusHandler(store, auto, gate, mon, clk).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("code %d type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var doc statusDoc
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("%v\n%s", err, rec.Body)
	}
	if doc.Symbols.Total != 2 || strings.Join(doc.Symbols.Manual, ",") != "BTCUSDT" || strings.Join(doc.Symbols.Auto, ",") != "WIFUSDT" {
		t.Errorf("symbols %+v", doc.Symbols)
	}
	a := doc.AutoSymbols
	if a == nil || a.Top != 5 || len(a.Selected) != 1 || a.Selected[0].Symbol != "WIFUSDT" || a.NextRun == nil || !a.NextRun.Equal(now.Add(5*time.Hour)) {
		t.Errorf("auto %+v", a)
	}
	byID := map[string]statusRule{}
	for _, r := range doc.Rules {
		byID[r.ID] = r
	}
	pb := byID["pb"]
	if !pb.Enabled || strings.Join(pb.AppliesTo, ",") != "BTCUSDT,WIFUSDT" || pb.When != "all(close > highest(20); volume_ratio(20) > 2)" ||
		pb.Setup == nil || pb.Setup.When != "low@1h <= ema(20)@4h" || pb.Setup.Within != "8h0m0s" ||
		pb.Targets == nil || pb.Targets.Side != config.SideLong || !pb.LastFired["WIFUSDT"].Equal(now) {
		t.Errorf("pb %+v", pb)
	}
	if mo := byID["manual_only"]; strings.Join(mo.AppliesTo, ",") != "BTCUSDT" {
		t.Errorf("manual_only applies to %v", mo.AppliesTo)
	}
	if off := byID["off"]; off.Enabled || len(off.AppliesTo) != 0 {
		t.Errorf("disabled rule %+v", off)
	}
	if f, ok := doc.Feeds["WIFUSDT"]; !ok || f.LastEvent != nil {
		t.Errorf("feeds %+v", doc.Feeds)
	}
	// The raw JSON uses snake_case keys a human can read.
	for _, k := range []string{`"applies_to"`, `"last_fired"`, `"avg_daily_quote_volume"`, `"side": "long"`} {
		if !strings.Contains(rec.Body.String(), k) {
			t.Errorf("body lacks %s", k)
		}
	}

	rec = httptest.NewRecorder()
	statusHandler(store, auto, gate, mon, clk).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/status", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST code %d", rec.Code)
	}
}
