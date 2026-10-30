package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

// pumpSpacing separates the pump bars on the fake WS; it exceeds the
// configured 200ms merge window.
const pumpSpacing = 300 * time.Millisecond

// recordedClosedOpen is the OpenTime of testdata/ws_kline_closed.json.
var recordedClosedOpen = time.UnixMilli(1793383020000).UTC()

// shiftedClock is real time moved so that "now" starts 90s after the
// recorded closed bar opened: durations stay real, timestamps line up with
// the recorded messages.
type shiftedClock struct{ off time.Duration }

func (c shiftedClock) Now() time.Time                         { return time.Now().Add(c.off) }
func (c shiftedClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// memSink is an in-memory storage.Sink.
type memSink struct {
	mu      sync.Mutex
	klines  int
	signals []signal.Signal
	closed  bool
}

func (m *memSink) WriteKlines(_ context.Context, ks []market.Kline) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.klines += len(ks)
	return nil
}
func (m *memSink) WriteMarkPrices(context.Context, []market.MarkPriceEvent) error     { return nil }
func (m *memSink) WriteOI(context.Context, []market.OpenInterestEvent) error          { return nil }
func (m *memSink) WriteLongShort(context.Context, []market.LongShortEvent) error      { return nil }
func (m *memSink) WriteLiquidations(context.Context, []market.LiquidationEvent) error { return nil }
func (m *memSink) WriteSignals(_ context.Context, ss []signal.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.signals = append(m.signals, ss...)
	return nil
}
func (m *memSink) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *memSink) signalRows(ruleID string) []signal.Signal {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []signal.Signal
	for _, s := range m.signals {
		if s.RuleID == ruleID {
			out = append(out, s)
		}
	}
	return out
}

// bar is one synthetic 1m bar.
type bar struct {
	open          time.Time
	o, h, l, c, v float64
}

// history is 1000 quiet 1m bars ending right before the recorded closed bar,
// priced like it (≈77090) so that bar itself is unremarkable.
func history() []bar {
	const n = 1000
	out := make([]bar, 0, n)
	start := recordedClosedOpen.Add(-n * time.Minute)
	for i := 0; i < n; i++ {
		c := 77090 + float64(i%5) - 2
		out = append(out, bar{open: start.Add(time.Duration(i) * time.Minute), o: c, h: c + 3, l: c - 3, c: c, v: 78 + float64(i%5)})
	}
	return out
}

func f(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func wsKline(b bar) []byte {
	msg := map[string]any{
		"stream": "btcusdt@kline_1m",
		"data": map[string]any{
			"e": "kline", "E": b.open.Add(time.Minute).UnixMilli(), "s": "BTCUSDT",
			"k": map[string]any{
				"t": b.open.UnixMilli(), "T": b.open.Add(time.Minute).UnixMilli() - 1, "s": "BTCUSDT", "i": "1m",
				"o": f(b.o), "c": f(b.c), "h": f(b.h), "l": f(b.l), "v": f(b.v), "q": f(b.v * b.c), "x": true,
			},
		},
	}
	out, _ := json.Marshal(msg)
	return out
}

// fakeBinance serves the REST endpoints the bot uses and the combined stream
// at /market/stream, which replays the recorded messages followed by a pump
// (three closed 1m bars, each +5% on ~10x volume).
func fakeBinance(t *testing.T) *httptest.Server {
	t.Helper()
	hist := history()
	var recorded [][]byte
	for _, name := range []string{"ws_kline.json", "ws_markprice.json", "ws_forceorder.json", "ws_kline_closed.json"} {
		b, err := os.ReadFile(filepath.Join("..", "..", "internal", "exchange", "binance", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		recorded = append(recorded, []byte(strings.TrimSpace(string(b))))
	}
	pump := []bar{
		{open: recordedClosedOpen.Add(1 * time.Minute), o: 77095.7, h: 80960, l: 77090, c: 80950, v: 800},
		{open: recordedClosedOpen.Add(2 * time.Minute), o: 80950, h: 85010, l: 80940, c: 85000, v: 1000},
		{open: recordedClosedOpen.Add(3 * time.Minute), o: 85000, h: 89260, l: 84990, c: 89250, v: 1200},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/exchangeInfo", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"TRADING"}]}`)
	})
	mux.HandleFunc("/fapi/v1/klines", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		rows := [][]any{}
		if q.Get("symbol") == "BTCUSDT" && q.Get("interval") == "1m" {
			for _, b := range hist {
				if b.open.UnixMilli() < start || len(rows) >= limit {
					continue
				}
				rows = append(rows, []any{b.open.UnixMilli(), f(b.o), f(b.h), f(b.l), f(b.c), f(b.v),
					b.open.Add(time.Minute).UnixMilli() - 1, f(b.v * b.c), 100, "0", "0", "0"})
			}
		}
		_ = json.NewEncoder(w).Encode(rows)
	})
	mux.HandleFunc("/fapi/v1/openInterest", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"symbol":"BTCUSDT","openInterest":"80000.5","time":%d}`, recordedClosedOpen.UnixMilli())
	})
	ls := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"symbol":"BTCUSDT","longShortRatio":"1.25","timestamp":%d}]`, recordedClosedOpen.UnixMilli())
	}
	mux.HandleFunc("/futures/data/globalLongShortAccountRatio", ls)
	mux.HandleFunc("/futures/data/topLongShortPositionRatio", ls)
	mux.HandleFunc("/market/stream", func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Query().Get("streams"), "btcusdt@kline_1m") {
			http.Error(w, "unexpected streams "+r.URL.RawQuery, http.StatusBadRequest)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := c.CloseRead(r.Context())
		send := func(m []byte, pause time.Duration) bool {
			if err := c.Write(ctx, websocket.MessageText, m); err != nil {
				return false
			}
			time.Sleep(pause)
			return true
		}
		for _, m := range recorded {
			if !send(m, 20*time.Millisecond) {
				return
			}
		}
		// Pump bars are spaced wider than the 200ms merge window, so a repeat
		// fire would land in a separate Telegram message.
		for _, b := range pump {
			if !send(wsKline(b), pumpSpacing) {
				return
			}
		}
		<-ctx.Done() // keep the connection open like Binance does
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// fakeTelegram records the text of every sendMessage call.
type fakeTelegram struct {
	mu    sync.Mutex
	texts []string
}

func (f *fakeTelegram) handler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.texts = append(f.texts, body.Text)
	f.mu.Unlock()
	fmt.Fprint(w, `{"ok":true,"result":{}}`)
}

func (f *fakeTelegram) matching(sub string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, s := range f.texts {
		if strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestE2E_ReplayProducesOneNotification(t *testing.T) {
	binanceSrv := fakeBinance(t)
	tg := &fakeTelegram{}
	tgSrv := httptest.NewServer(http.HandlerFunc(tg.handler))
	t.Cleanup(tgSrv.Close)

	cfg := fmt.Sprintf(`
symbols: [BTCUSDT]
rules:
  - id: pump_with_volume
    interval: 1m
    trigger: on_close
    when:
      all:
        - change_pct(1) > 3
        - volume_ratio(20) > 2
    cooldown: 30m
    channels: [tg]
channels:
  tg:
    type: telegram
    params:
      token: TESTTOKEN
      chat_id: "42"
      base_url: %s
notify:
  merge_window: 200ms
  rate_per_minute: 20
binance:
  rest_url: %s
  ws_url: ws://%s/market
`, tgSrv.URL, binanceSrv.URL, strings.TrimPrefix(binanceSrv.URL, "http://"))
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	sink := &memSink{}
	clk := shiftedClock{off: recordedClosedOpen.Add(90 * time.Second).Sub(time.Now())}
	deps := Deps{
		Clock: clk,
		OpenStore: func(context.Context, string) (Store, error) {
			return sink, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, "127.0.0.1:0", deps) }()

	waitFor(t, 10*time.Second, "a Telegram message with pump_with_volume", func() bool {
		return len(tg.matching("pump_with_volume")) >= 1
	})
	// The continued pump bars arrive pumpSpacing apart, each beyond the merge
	// window; wait for all of them plus several merge windows.
	time.Sleep(3*pumpSpacing + time.Second)
	total := 0
	for _, txt := range tg.matching("") {
		total += strings.Count(txt, "pump_with_volume")
	}
	if total != 1 {
		t.Fatalf("pump_with_volume appears %d times across Telegram messages, want 1: %q", total, tg.matching(""))
	}
	if !strings.Contains(tg.matching("pump_with_volume")[0], "BTCUSDT") {
		t.Errorf("message lacks the symbol: %q", tg.matching("pump_with_volume")[0])
	}

	waitFor(t, 5*time.Second, "the signal row in storage", func() bool {
		return len(sink.signalRows("pump_with_volume")) >= 1
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return within 10s of cancel")
	}
	// After run returns the Writer has done its final flush: every recorded
	// signal (delivered or suppressed) is in the sink.
	rows := sink.signalRows("pump_with_volume")
	if len(rows) != 1 || rows[0].Symbol != "BTCUSDT" || rows[0].Suppressed != "" {
		t.Errorf("signal rows = %+v, want exactly one delivered BTCUSDT row", rows)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if !sink.closed {
		t.Error("store not closed on shutdown")
	}
	if sink.klines == 0 {
		t.Error("no klines persisted")
	}
}

// A startup failure after the notifiers exist is pushed as a critical system
// message before run returns.
func TestRun_StartupFailureNotifies(t *testing.T) {
	binanceDown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "maintenance", http.StatusServiceUnavailable)
	}))
	t.Cleanup(binanceDown.Close)
	tg := &fakeTelegram{}
	tgSrv := httptest.NewServer(http.HandlerFunc(tg.handler))
	t.Cleanup(tgSrv.Close)
	cfg := fmt.Sprintf(`
symbols: [BTCUSDT]
rules:
  - {id: r, interval: 1m, when: close > 1, channels: [tg]}
channels:
  tg: {type: telegram, params: {token: T, chat_id: "1", base_url: %s}}
binance:
  rest_url: %s
`, tgSrv.URL, binanceDown.URL)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	start := time.Now()
	err := run(context.Background(), cfgPath, "127.0.0.1:0", Deps{
		OpenStore: func(context.Context, string) (Store, error) { return sink, nil },
	})
	if err == nil {
		t.Fatal("run succeeded against a failing exchangeInfo")
	}
	if el := time.Since(start); el > 7*time.Second {
		t.Errorf("run took %v", el)
	}
	if got := tg.matching("启动失败"); len(got) != 1 || !strings.Contains(got[0], "exchange") {
		t.Errorf("telegram = %q, want one 启动失败 message naming the error", tg.matching(""))
	}
}
