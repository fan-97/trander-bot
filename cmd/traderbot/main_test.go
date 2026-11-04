package main

import (
	"reflect"
	"testing"

	"trader-bot/internal/config"
	"trader-bot/internal/market"
)

func TestWarmupPlan(t *testing.T) {
	cases := []struct {
		ivs  []market.Interval
		want map[market.Interval]int
	}{
		{[]market.Interval{market.I5m, market.I1h}, map[market.Interval]int{market.I5m: 500, market.I1h: 500, market.I1m: 60}},
		{[]market.Interval{market.I1m}, map[market.Interval]int{market.I1m: 500}},
		{[]market.Interval{market.I1m, market.I1d}, map[market.Interval]int{market.I1m: 1440, market.I1d: 500}},
		{[]market.Interval{market.I15m}, map[market.Interval]int{market.I15m: 500, market.I1m: 15}},
	}
	for _, c := range cases {
		if got := warmupPlan(c.ivs); !reflect.DeepEqual(got, c.want) {
			t.Errorf("warmupPlan(%v) = %v, want %v", c.ivs, got, c.want)
		}
	}
}

func TestFilterSymbols(t *testing.T) {
	cfg, err := config.Parse([]byte(`
symbols: [btcusdt, ETHUSDT, NOPEUSDT]
rules:
  - id: r
    symbols: [btcusdt, NOPEUSDT]
    interval: 5m
    when: close > 1
    channels: [w]
channels:
  w: {type: webhook, params: {url: "http://x"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{"BTCUSDT": true, "ETHUSDT": true}
	got, dropped := filterSymbols(cfg, have, nil)
	if !reflect.DeepEqual(got.Symbols, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Errorf("symbols = %v", got.Symbols)
	}
	if !reflect.DeepEqual(dropped, []string{"NOPEUSDT"}) {
		t.Errorf("dropped = %v", dropped)
	}
	if !reflect.DeepEqual(got.Rules[0].Symbols, []string{"BTCUSDT", "NOPEUSDT"}) {
		t.Errorf("rule symbols = %v", got.Rules[0].Symbols)
	}
	if len(got.RulesFor("BTCUSDT")) != 1 {
		t.Error("rule no longer applies to BTCUSDT")
	}
	if cfg.Symbols[0] != "btcusdt" || cfg.Rules[0].Symbols[0] != "btcusdt" {
		t.Error("input config was mutated")
	}
	if !reflect.DeepEqual(got.Intervals, cfg.Intervals) {
		t.Errorf("intervals = %v", got.Intervals)
	}
}

func TestHealthcheckURL(t *testing.T) {
	cases := map[string]string{
		":8080":          "http://127.0.0.1:8080/healthz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/healthz",
		"127.0.0.1:8081": "http://127.0.0.1:8081/healthz",
		"[::]:8080":      "http://127.0.0.1:8080/healthz",
		"example:80":     "http://example:80/healthz",
	}
	for in, want := range cases {
		got, err := healthcheckURL(in)
		if err != nil || got != want {
			t.Errorf("healthcheckURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := healthcheckURL("nonsense"); err == nil {
		t.Error("want error for an address without a port")
	}
}

// The shipped example must load and build its notifiers once the secrets
// it references are set.
func TestExampleConfigBuilds(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "123:abc")
	t.Setenv("TELEGRAM_CHAT_ID", "42")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/api/webhooks/1/x")
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	cfg, err := config.Load("../../configs/config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newDispatcher(cfg, nil, nil); err != nil {
		t.Fatal(err)
	}
}
