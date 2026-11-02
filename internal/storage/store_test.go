//go:build integration

package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

// Run with:
//   TEST_DATABASE_URL=postgres://postgres:pg@localhost:5433/postgres go test -tags integration ./internal/storage/...
// Add TB_SKIP_TIMESCALE=1 when the server has no TimescaleDB extension.

func openTest(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(ctx, `TRUNCATE klines, mark_prices, open_interest, long_short, liquidations, signals`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func count(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func waitCount(t *testing.T, s *Store, table string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		n := count(t, s, table)
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s rows = %d, want %d", table, n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runWriter(t *testing.T, w *Writer) (chan any, func()) {
	t.Helper()
	in := make(chan any)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, in) }()
	stop := func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
	return in, stop
}

func bar(i int, closeP float64) market.KlineEvent {
	return market.KlineEvent{Kline: market.Kline{
		Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: t0.Add(time.Duration(i) * time.Minute),
		Open: 1, High: 2, Low: 0.5, Close: closeP, Volume: 10, QuoteVolume: 100, Closed: true,
	}}
}

func TestWriter_FlushOnSize(t *testing.T) {
	s := openTest(t)
	w := NewWriter(s, 3, time.Hour)
	in, stop := runWriter(t, w)
	defer stop()
	in <- bar(0, 1)
	in <- market.OpenInterestEvent{Symbol: "BTCUSDT", Time: t0, OI: 5}
	in <- market.LiquidationEvent{Symbol: "BTCUSDT", Time: t0, Side: "SELL", Price: 1, Qty: 2}
	waitCount(t, s, "klines", 1)
	waitCount(t, s, "open_interest", 1)
	waitCount(t, s, "liquidations", 1)
}

func TestWriter_FlushOnInterval(t *testing.T) {
	s := openTest(t)
	w := NewWriter(s, 500, 2*time.Second)
	clk := clock.NewFake(t0)
	w.Clock = clk
	in, stop := runWriter(t, w)
	defer stop()
	in <- market.MarkPriceEvent{Symbol: "BTCUSDT", Time: t0, MarkPrice: 100, FundingRate: 0.0001}
	in <- market.LongShortEvent{Symbol: "BTCUSDT", Time: t0, Kind: "global", Ratio: 1.2}
	time.Sleep(50 * time.Millisecond)
	if n := count(t, s, "mark_prices"); n != 0 {
		t.Fatalf("flushed before interval: %d", n)
	}
	clk.Advance(2 * time.Second)
	waitCount(t, s, "mark_prices", 1)
	waitCount(t, s, "long_short", 1)
}

func TestWriter_KlineUpsert(t *testing.T) {
	s := openTest(t)
	w := NewWriter(s, 1, time.Hour)
	in, stop := runWriter(t, w)
	in <- bar(0, 1)
	waitCount(t, s, "klines", 1)
	in <- bar(0, 42)
	stop()
	if n := count(t, s, "klines"); n != 1 {
		t.Fatalf("klines rows = %d, want 1", n)
	}
	var c float64
	if err := s.pool.QueryRow(context.Background(), `SELECT close FROM klines`).Scan(&c); err != nil {
		t.Fatal(err)
	}
	if c != 42 {
		t.Fatalf("close = %v, want 42 (the later write)", c)
	}
}

func TestWriter_SignalsIncludingSuppressed(t *testing.T) {
	s := openTest(t)
	w := NewWriter(s, 500, time.Hour)
	in, stop := runWriter(t, w)
	in <- signal.Signal{RuleID: "rsi", Symbol: "BTCUSDT", Interval: market.I1h, Level: config.Level("warn"), Time: t0,
		Price: 1, Detail: "fired", Channels: []string{"tg", "mail"}}
	in <- signal.Signal{RuleID: "rsi", Symbol: "BTCUSDT", Interval: market.I1h, Level: config.Level("warn"), Time: t0.Add(time.Minute),
		Price: 2, Detail: "held", Channels: []string{"tg"}, Suppressed: "cooldown"}
	stop()
	rows, err := s.pool.Query(context.Background(), `SELECT rule_id, interval, level, detail, channels, suppressed FROM signals ORDER BY time`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type row struct {
		rule, iv, lvl, detail, supp string
		ch                          []string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.rule, &r.iv, &r.lvl, &r.detail, &r.ch, &r.supp); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("signals = %+v, want 2", got)
	}
	if got[0].supp != "" || got[1].supp != "cooldown" || len(got[0].ch) != 2 || got[1].iv != "1h" || got[0].lvl != "warn" {
		t.Fatalf("signals = %+v", got)
	}
}

func TestStore_DuplicateSamplesHarmless(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	mp := []market.MarkPriceEvent{{Symbol: "BTCUSDT", Time: t0, MarkPrice: 1}}
	oi := []market.OpenInterestEvent{{Symbol: "BTCUSDT", Time: t0, OI: 1}}
	ls := []market.LongShortEvent{{Symbol: "BTCUSDT", Time: t0, Kind: "global", Ratio: 1}, {Symbol: "BTCUSDT", Time: t0, Kind: "top", Ratio: 2}}
	for i := 0; i < 2; i++ {
		if err := s.WriteMarkPrices(ctx, mp); err != nil {
			t.Fatal(err)
		}
		if err := s.WriteOI(ctx, oi); err != nil {
			t.Fatal(err)
		}
		if err := s.WriteLongShort(ctx, ls); err != nil {
			t.Fatal(err)
		}
	}
	if count(t, s, "mark_prices") != 1 || count(t, s, "open_interest") != 1 || count(t, s, "long_short") != 2 {
		t.Fatal("duplicate samples were not ignored")
	}
}

func TestStore_RecentKlines(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	var ks []market.Kline
	for i := 0; i < 5; i++ {
		ks = append(ks, bar(i, float64(i)).Kline)
	}
	other := bar(9, 99).Kline
	other.Interval = market.I5m
	ks = append(ks, other)
	if err := s.WriteKlines(ctx, ks); err != nil {
		t.Fatal(err)
	}
	got, err := s.RecentKlines(ctx, "BTCUSDT", market.I1m, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, k := range got {
		if k.Close != float64(i+2) || !k.Closed || k.Interval != market.I1m || k.Symbol != "BTCUSDT" ||
			!k.OpenTime.Equal(t0.Add(time.Duration(i+2)*time.Minute)) || k.QuoteVolume != 100 {
			t.Fatalf("got[%d] = %+v", i, k)
		}
	}
	all, err := s.RecentKlines(ctx, "BTCUSDT", market.I1m, 500)
	if err != nil || len(all) != 5 {
		t.Fatalf("all = %d, %v", len(all), err)
	}
}

func TestStore_BigKlineBatch(t *testing.T) {
	s := openTest(t)
	var ks []market.Kline
	for i := 0; i < 8000; i++ { // 8000*9 params > 65535
		ks = append(ks, bar(i, 1).Kline)
	}
	if err := s.WriteKlines(context.Background(), ks); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "klines"); n != 8000 {
		t.Fatalf("rows = %d", n)
	}
}

func TestStore_RecentKlinesNonPositive(t *testing.T) {
	s := openTest(t)
	for _, n := range []int{0, -1} {
		got, err := s.RecentKlines(context.Background(), "BTCUSDT", market.I1m, n)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("n=%d: got %v, %v; want empty, nil", n, got, err)
		}
	}
}

func TestStore_LastKlineTime(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if _, ok, err := s.LastKlineTime(ctx, "LKTUSDT", market.I1h); err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var ks []market.Kline
	for i := 0; i < 3; i++ {
		ks = append(ks, market.Kline{Symbol: "LKTUSDT", Interval: market.I1h, OpenTime: base.Add(time.Duration(i) * time.Hour),
			Open: 1, High: 1, Low: 1, Close: 1, Closed: true})
	}
	ks = append(ks, market.Kline{Symbol: "LKTUSDT", Interval: market.I5m, OpenTime: base.Add(10 * time.Hour), Closed: true})
	if err := s.WriteKlines(ctx, ks); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LastKlineTime(ctx, "LKTUSDT", market.I1h)
	if err != nil || !ok || !got.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("got %v ok=%v err=%v, want +2h", got, ok, err)
	}
}
