package universe

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"trader-bot/internal/config"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/market"
)

var now = time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)

func cfg() config.AutoSymbols {
	return config.AutoSymbols{Enabled: true, Top: 2, RankBy: config.RankVolumeGrowth, Days: 3, BaseDays: 4,
		MinQuoteVolume: 100, MinListingDays: 30, KeepRank: 3, Refresh: time.Hour}
}

// bars builds closed daily bars, oldest first, ending yesterday, from quote
// volumes; closes rise by step per day from 100. A final open bar is added.
func bars(sym string, step float64, vols ...float64) []market.Kline {
	var out []market.Kline
	day := now.Truncate(24*time.Hour).AddDate(0, 0, -len(vols))
	px := 100.0
	for i, v := range vols {
		out = append(out, market.Kline{Symbol: sym, Interval: market.I1d, OpenTime: day.AddDate(0, 0, i),
			Open: px, Close: px + step, QuoteVolume: v, Closed: true})
		px += step
	}
	// today's bar is open: huge volume that must not count
	out = append(out, market.Kline{Symbol: sym, Interval: market.I1d, OpenTime: day.AddDate(0, 0, len(vols)),
		Open: px, Close: px * 10, QuoteVolume: 1e12})
	return out
}

func TestCandidates(t *testing.T) {
	old := now.AddDate(-1, 0, 0)
	cs := []binance.Contract{
		{Symbol: "BTCUSDT", BaseAsset: "BTC", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "BTCDOMUSDT", BaseAsset: "BTCDOM", UnderlyingType: "INDEX", OnboardDate: old},
		{Symbol: "USDCUSDT", BaseAsset: "USDC", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "PAXGUSDT", BaseAsset: "PAXG", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "NEWUSDT", BaseAsset: "NEW", UnderlyingType: "COIN", OnboardDate: now.AddDate(0, 0, -5)},
		{Symbol: "THINUSDT", BaseAsset: "THIN", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "BANUSDT", BaseAsset: "BAN", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "MANUSDT", BaseAsset: "MAN", UnderlyingType: "COIN", OnboardDate: old},
		{Symbol: "NODATEUSDT", BaseAsset: "NODATE"},
	}
	var ts []binance.Ticker24h
	for _, c := range cs {
		v := 1000.0
		if c.Symbol == "THINUSDT" {
			v = 10
		}
		ts = append(ts, binance.Ticker24h{Symbol: c.Symbol, QuoteVolume: v})
	}
	c := cfg()
	c.Exclude = []string{"BANUSDT"}
	got := Candidates(cs, ts, c, []string{"MANUSDT"}, now)
	if want := []string{"BTCUSDT", "NODATEUSDT"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestRank_Modes(t *testing.T) {
	daily := map[string][]market.Kline{
		// base 4 days, recent 3 days
		"HOTUSDT":  bars("HOTUSDT", 1, 100, 100, 100, 100, 400, 400, 400),         // growth 4, 104 -> 107 over the 3 days
		"BIGUSDT":  bars("BIGUSDT", -5, 1000, 1000, 1000, 1000, 1200, 1200, 1200), // growth 1.2, biggest, falling
		"WARMUSDT": bars("WARMUSDT", 2, 100, 100, 100, 100, 200, 200, 200),        // growth 2, +6%
	}
	for mode, want := range map[config.RankBy][]string{
		config.RankVolumeGrowth: {"HOTUSDT", "WARMUSDT"},
		config.RankQuoteVolume:  {"BIGUSDT", "HOTUSDT"},
		config.RankAbsChange:    {"BIGUSDT", "WARMUSDT"}, // -15% then +6%
		config.RankGain:         {"WARMUSDT", "HOTUSDT"},
	} {
		c := cfg()
		c.RankBy = mode
		got, rows := Rank(daily, c, nil)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v want %v (rows %+v)", mode, got, want, rows)
		}
	}
	_, rows := Rank(daily, cfg(), nil)
	if r := rows[0]; r.Symbol != "HOTUSDT" || r.Rank != 1 || r.Growth != 4 || r.AvgVolume != 400 || math.Abs(r.ChangePct-(107.0/104-1)*100) > 1e-9 {
		t.Errorf("open bar leaked or metrics wrong: %+v", r)
	}
}

func TestRank_Rejections(t *testing.T) {
	daily := map[string][]market.Kline{
		"OKUSDT":    bars("OKUSDT", 1, 100, 100, 100, 100, 300, 300, 300),
		"THINUSDT":  bars("THINUSDT", 1, 1, 1, 1, 1, 50, 50, 50), // growth 50 but avg 50 < 100
		"YOUNGUSDT": bars("YOUNGUSDT", 1, 100, 500, 500, 500),    // baseline 1 day < 4/2
		"SHORTUSDT": bars("SHORTUSDT", 1, 500, 500),              // 2 of 3 days
	}
	got, rows := Rank(daily, cfg(), nil)
	if !reflect.DeepEqual(got, []string{"OKUSDT"}) {
		t.Fatalf("got %v", got)
	}
	reasons := map[string]string{}
	for _, r := range rows {
		reasons[r.Symbol] = r.Reason
	}
	for sym, want := range map[string]string{"THINUSDT": "liquidity", "YOUNGUSDT": "baseline", "SHORTUSDT": "history", "OKUSDT": ""} {
		if !strings.HasPrefix(reasons[sym], want) || (want == "" && reasons[sym] != "") {
			t.Errorf("%s reason %q want %q", sym, reasons[sym], want)
		}
	}
	// quote_volume needs no baseline, so YOUNGUSDT qualifies there.
	c := cfg()
	c.RankBy = config.RankQuoteVolume
	if got, _ := Rank(daily, c, nil); !reflect.DeepEqual(got, []string{"YOUNGUSDT", "OKUSDT"}) {
		t.Errorf("quote_volume got %v", got)
	}
}

func TestRank_Hysteresis(t *testing.T) {
	daily := map[string][]market.Kline{
		"AUSDT": bars("AUSDT", 1, 100, 100, 100, 100, 500, 500, 500), // growth 5
		"BUSDT": bars("BUSDT", 1, 100, 100, 100, 100, 400, 400, 400), // 4
		"CUSDT": bars("CUSDT", 1, 100, 100, 100, 100, 300, 300, 300), // 3
		"DUSDT": bars("DUSDT", 1, 100, 100, 100, 100, 200, 200, 200), // 2
	}
	// C was selected before and is rank 3 <= keep_rank 3: it stays, B is not added.
	got, rows := Rank(daily, cfg(), []string{"CUSDT", "AUSDT"})
	if !reflect.DeepEqual(got, []string{"AUSDT", "CUSDT"}) {
		t.Fatalf("got %v", got)
	}
	for _, r := range rows {
		if r.Symbol == "CUSDT" && (!r.Selected || !r.Kept) {
			t.Errorf("C row %+v", r)
		}
	}
	// D was selected before but is rank 4 > keep_rank: replaced by B.
	if got, _ := Rank(daily, cfg(), []string{"DUSDT", "AUSDT"}); !reflect.DeepEqual(got, []string{"AUSDT", "BUSDT"}) {
		t.Fatalf("got %v", got)
	}
}

type fakeSource struct {
	cs     []binance.Contract
	ts     []binance.Ticker24h
	daily  map[string][]market.Kline
	fail   map[string]bool
	starts map[string]time.Time
	limits map[string]int
}

func (f *fakeSource) Contracts(context.Context) ([]binance.Contract, error)   { return f.cs, nil }
func (f *fakeSource) Tickers24h(context.Context) ([]binance.Ticker24h, error) { return f.ts, nil }
func (f *fakeSource) Klines(_ context.Context, sym string, iv market.Interval, start time.Time, limit int) ([]market.Kline, error) {
	if iv != market.I1d {
		return nil, errors.New("want 1d")
	}
	f.starts[sym], f.limits[sym] = start, limit
	if f.fail[sym] {
		return nil, errors.New("boom")
	}
	return f.daily[sym], nil
}

func TestSelect(t *testing.T) {
	f := &fakeSource{
		cs: []binance.Contract{{Symbol: "AUSDT", BaseAsset: "A", UnderlyingType: "COIN"}, {Symbol: "BUSDT", BaseAsset: "B", UnderlyingType: "COIN"},
			{Symbol: "CUSDT", BaseAsset: "C", UnderlyingType: "COIN"}},
		ts: []binance.Ticker24h{{Symbol: "AUSDT", QuoteVolume: 1e3}, {Symbol: "BUSDT", QuoteVolume: 1e3}, {Symbol: "CUSDT", QuoteVolume: 1e3}},
		daily: map[string][]market.Kline{
			"AUSDT": bars("AUSDT", 1, 100, 100, 100, 100, 500, 500, 500),
			"CUSDT": bars("CUSDT", 1, 100, 100, 100, 100, 300, 300, 300),
		},
		fail:   map[string]bool{"BUSDT": true},
		starts: map[string]time.Time{}, limits: map[string]int{},
	}
	sel, _, err := Select(context.Background(), f, cfg(), nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sel, []string{"AUSDT", "CUSDT"}) {
		t.Fatalf("sel %v", sel)
	}
	// 3+4 closed days plus today's bar, starting 7 days before today 00:00.
	if f.limits["AUSDT"] != 8 || !f.starts["AUSDT"].Equal(time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("request start %v limit %d", f.starts["AUSDT"], f.limits["AUSDT"])
	}
	f.fail = map[string]bool{"AUSDT": true, "BUSDT": true, "CUSDT": true}
	if _, _, err := Select(context.Background(), f, cfg(), nil, nil, now); err == nil {
		t.Fatal("all candidates failing must be an error")
	}
}
