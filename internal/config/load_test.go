package config

import (
	"strings"
	"testing"
	"time"

	"trader-bot/internal/market"
)

func TestLoad_Example(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "tok")
	t.Setenv("TELEGRAM_CHAT_ID", "1")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example.invalid/api/webhooks/1/x")
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/db")
	c, err := Load("../../configs/config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Rules) != 2 {
		t.Fatalf("rules=%d", len(c.Rules))
	}
	if c.Rules[0].Cooldown != 30*time.Minute {
		t.Errorf("cooldown=%v", c.Rules[0].Cooldown)
	}
	if c.Rules[1].Trigger != TriggerOnClose {
		t.Errorf("trigger=%v", c.Rules[1].Trigger)
	}
	if c.Rules[1].Level != LevelWarn || c.Rules[1].Symbols[0] != "all" {
		t.Errorf("defaults: %+v", c.Rules[1])
	}
	if c.Notify.MergeWindow != 10*time.Second || c.Notify.RatePerMinute != 20 || c.Notify.QuietMinLevel != LevelCritical {
		t.Errorf("notify=%+v", c.Notify)
	}
	if got := c.Channels["telegram"].Params["token"]; got != "tok" {
		t.Errorf("env expand: %q", got)
	}
	if len(c.Intervals) != 2 || c.Intervals[0] != market.I5m {
		t.Errorf("intervals=%v", c.Intervals)
	}
	if c.Binance.RESTURL != "https://fapi.binance.com" || c.Binance.WSURL != "wss://fstream.binance.com/market" {
		t.Errorf("binance=%+v", c.Binance)
	}
}

func TestDefaultWSURLHasMarketPath(t *testing.T) {
	// The root path connects but Binance sends no market data on it.
	if defaultWSURL != "wss://fstream.binance.com/market" {
		t.Fatalf("defaultWSURL = %q", defaultWSURL)
	}
}

func TestValidate_CollectsErrors(t *testing.T) {
	yml := `
symbols: [BTCUSDT]
channels:
  tg: {type: telegram}
rules:
  - {id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}
  - {id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [nope]}
  - {id: b, interval: 7m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}
  - {id: c, symbols: [DOGEUSDT], interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}
`
	_, err := Parse([]byte(yml))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"duplicate rule id", "unknown channel", "unknown interval", "not in watch pool"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
	if n := len(strings.Split(err.Error(), "\n")); n != 4 {
		t.Errorf("want 4 errors, got %d:\n%v", n, err)
	}
}

func TestValidate_NoChannels(t *testing.T) {
	_, err := Parse([]byte(`
symbols: [BTCUSDT]
rules:
  - {id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}}
`))
	if err == nil || !strings.Contains(err.Error(), "no channels") {
		t.Fatalf("err=%v", err)
	}
}

func TestRulesFor_All(t *testing.T) {
	c := &Config{
		Symbols: []string{"BTCUSDT", "ETHUSDT"},
		Rules: []Rule{
			{ID: "x", Symbols: []string{"all"}},
			{ID: "y", Symbols: []string{"ETHUSDT"}},
		},
	}
	if got := c.RulesFor("BTCUSDT"); len(got) != 1 || got[0].ID != "x" {
		t.Errorf("BTC: %+v", got)
	}
	if got := c.RulesFor("ETHUSDT"); len(got) != 2 {
		t.Errorf("ETH: %+v", got)
	}
}

const okRule = `{id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}`

func TestParse_BuildAndValidateErrorsTogether(t *testing.T) {
	_, err := Parse([]byte(`
symbols: [BTCUSDT]
channels: {tg: {type: telegram}}
rules:
  - {id: a, interval: 5m, cooldown: abc, when: {all: ["rsi(14) < 30"]}, channels: [tg]}
  - {id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}
`))
	if err == nil || !strings.Contains(err.Error(), "invalid cooldown") || !strings.Contains(err.Error(), "duplicate rule id") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidate_ExplicitIntervals(t *testing.T) {
	_, err := Parse([]byte("symbols: [BTCUSDT]\nintervals: [1h, 1h]\nchannels: {tg: {type: telegram}}\nrules: [" + okRule + "]"))
	if err == nil || !strings.Contains(err.Error(), "not in intervals list") {
		t.Fatalf("err=%v", err)
	}
	c, err := Parse([]byte("symbols: [BTCUSDT]\nintervals: [5m, 5m]\nchannels: {tg: {type: telegram}}\nrules: [" + okRule + "]"))
	if err != nil || len(c.Intervals) != 1 {
		t.Fatalf("c=%v err=%v", c, err)
	}
}

func TestValidate_WhenNull(t *testing.T) {
	_, err := Parse([]byte("symbols: [BTCUSDT]\nchannels: {tg: {type: telegram}}\nrules:\n  - {id: a, interval: 5m, when: null, channels: [tg]}"))
	if err == nil || !strings.Contains(err.Error(), "missing when") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidate_BadExpression(t *testing.T) {
	_, err := Parse([]byte("symbols: [BTCUSDT]\nchannels: {tg: {type: telegram}}\nrules:\n  - {id: a, interval: 5m, when: {all: [\"foo(1) > 2\"]}, channels: [tg]}\n  - {id: b, interval: 5m, when: {all: [\"rsi(14) <\"]}, channels: [tg]}"))
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "missing when") {
		t.Errorf("spurious missing when: %v", err)
	}
	for _, w := range []string{`rule "a"`, "foo(1) > 2", `rule "b"`, "rsi(14) <", "line 4"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("err lacks %q: %v", w, err)
		}
	}
}

func TestValidate_RealtimeOperands(t *testing.T) {
	head := "symbols: [BTCUSDT]\nchannels: {tg: {type: telegram}}\nrules:\n"
	_, err := Parse([]byte(head + `  - {id: a, interval: 5m, trigger: realtime, when: {all: ["rsi(14) < 30"]}, channels: [tg]}`))
	if err == nil || !strings.Contains(err.Error(), `rule "a"`) || !strings.Contains(err.Error(), "rsi(14)") || !strings.Contains(err.Error(), "realtime") {
		t.Fatalf("err=%v", err)
	}
	// rsi is fine for on_close rules
	if _, err := Parse([]byte(head + `  - {id: a, interval: 5m, when: {all: ["rsi(14) < 30"]}, channels: [tg]}`)); err != nil {
		t.Fatalf("on_close: %v", err)
	}
	ok := `  - {id: a, interval: 5m, trigger: realtime, when: {any: ["close > 100", "change_pct(3) > 2", "volume > 1000", "funding_rate < -0.0005", "oi_change_pct(4) > 10", "liq_volume(5) > 1000000", "amplitude_pct > 3", "volume_ratio(20) > 1.5", "high > 1", "low < 1", "open > 1"]}, channels: [tg]}`
	if _, err := Parse([]byte(head + ok)); err != nil {
		t.Fatalf("allowed operands rejected: %v", err)
	}
	// wick shape is only known once the bar closes
	_, err = Parse([]byte(head + `  - {id: a, interval: 5m, trigger: realtime, when: {all: ["lower_wick_atr(14) > 2"]}, channels: [tg]}`))
	if err == nil || !strings.Contains(err.Error(), "lower_wick_atr(14)") {
		t.Fatalf("wick in realtime accepted: %v", err)
	}
}

// I4: a ${VAR} that is not set is a validation error naming the variable
// (never its value); a variable set to "" is allowed.
func TestBuild_UnsetEnvVarRejected(t *testing.T) {
	t.Setenv("TB_TEST_EMPTY", "")
	cfg := "symbols: [BTCUSDT]\nrules:\n  - {id: a, interval: 1m, when: {all: [\"close > 1\"]}, channels: [tg]}\n" +
		"channels: {tg: {type: telegram, params: {token: \"${TB_TEST_UNSET_TOKEN}\", chat_id: \"${TB_TEST_EMPTY}\"}}}\n" +
		"database: {dsn: \"${TB_TEST_UNSET_DSN}\"}\n"
	_, err := Parse([]byte(cfg))
	if err == nil {
		t.Fatal("unset env vars accepted")
	}
	for _, want := range []string{"TB_TEST_UNSET_TOKEN", "TB_TEST_UNSET_DSN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "TB_TEST_EMPTY") {
		t.Errorf("set-but-empty variable rejected: %v", err)
	}
}

// R21: an omitted cooldown defaults to max(interval, 15m); an explicit 0s
// still means "no cooldown".
func TestBuild_DefaultCooldown(t *testing.T) {
	head := "symbols: [BTCUSDT]\nchannels: {tg: {type: telegram}}\nrules:\n"
	c, err := Parse([]byte(head +
		"  - {id: m1, interval: 1m, when: {all: [\"close > 1\"]}, channels: [tg]}\n" +
		"  - {id: h4, interval: 4h, when: {all: [\"close > 1\"]}, channels: [tg]}\n" +
		"  - {id: zero, interval: 1m, cooldown: 0s, when: {all: [\"close > 1\"]}, channels: [tg]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]time.Duration{"m1": 15 * time.Minute, "h4": 4 * time.Hour, "zero": 0}
	for _, r := range c.Rules {
		if r.Cooldown != want[r.ID] {
			t.Errorf("rule %s cooldown=%v want %v", r.ID, r.Cooldown, want[r.ID])
		}
	}
}
