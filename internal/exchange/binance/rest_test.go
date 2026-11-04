package binance

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
)

var t0 = time.Date(2026, 10, 31, 12, 0, 30, 0, time.UTC)

func newTestREST(t *testing.T, h http.HandlerFunc, weight int, clk clock.Clock) *REST {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	r := NewREST(srv.URL, weight)
	r.Clock = clk
	r.Logger = quietLogger(t)
	return r
}

// quietLogger routes logs to t.Log so they show only on failure.
func quietLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(tWriter{t}, nil))
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

// waitDone advances the fake clock in steps until done is closed (or fails
// after a real-time limit), so the tested goroutine can register its timer.
func waitDone(t *testing.T, clk *clock.Fake, step time.Duration, done <-chan struct{}) {
	t.Helper()
	limit := time.After(2 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-limit:
			t.Fatal("timed out waiting")
		case <-time.After(2 * time.Millisecond):
			clk.Advance(step)
		}
	}
}

func TestValidateSymbols_DropsUnknown(t *testing.T) {
	have := map[string]bool{"BTCUSDT": true, "ETHUSDT": true}
	ok, dropped := ValidateSymbols([]string{"BTCUSDT", "FOOUSDT", "ethusdt", "BTCUSDT"}, have)
	if !reflect.DeepEqual(ok, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("ok = %v", ok)
	}
	if !reflect.DeepEqual(dropped, []string{"FOOUSDT"}) {
		t.Fatalf("dropped = %v", dropped)
	}
}

func TestREST_Symbols_FiltersPerpetualUSDTTrading(t *testing.T) {
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/fapi/v1/exchangeInfo" {
			t.Errorf("path %s", req.URL.Path)
		}
		fmt.Fprint(w, `{"symbols":[
			{"symbol":"BTCUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"TRADING"},
			{"symbol":"BTCUSDT_261225","contractType":"CURRENT_QUARTER","quoteAsset":"USDT","status":"TRADING"},
			{"symbol":"ETHUSDC","contractType":"PERPETUAL","quoteAsset":"USDC","status":"TRADING"},
			{"symbol":"OLDUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"SETTLING"},
			{"symbol":"ETHUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"TRADING"}]}`)
	}, 1200, clock.NewFake(t0))
	got, err := r.Symbols(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]bool{"BTCUSDT": true, "ETHUSDT": true}) {
		t.Fatalf("got %v", got)
	}
}

func klineRow(open time.Time, px float64) string {
	ms := open.UnixMilli()
	return fmt.Sprintf(`[%d,"%g","%g","%g","%g","10.5",%d,"1000.25",5,"1","2","0"]`,
		ms, px, px+2, px-1, px+1, ms+59999)
}

func TestREST_Klines_ParsesAndMarksClosed(t *testing.T) {
	clk := clock.NewFake(t0) // 12:00:30, so the 12:00 bar is still open
	var q atomicQuery
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		q.set(req.URL.Query())
		if req.URL.Path != "/fapi/v1/klines" {
			t.Errorf("path %s", req.URL.Path)
		}
		fmt.Fprintf(w, "[%s,%s]", klineRow(t0.Add(-90*time.Second), 100), klineRow(t0.Add(-30*time.Second), 200))
	}, 1200, clk)
	start := t0.Add(-90 * time.Second)
	ks, err := r.Klines(context.Background(), "BTCUSDT", market.I1m, start, 2)
	if err != nil {
		t.Fatal(err)
	}
	got := q.get()
	if got.Get("symbol") != "BTCUSDT" || got.Get("interval") != "1m" || got.Get("limit") != "2" ||
		got.Get("startTime") != strconv.FormatInt(start.UnixMilli(), 10) {
		t.Fatalf("query %v", got)
	}
	want := []market.Kline{
		{Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: start, Open: 100, High: 102, Low: 99, Close: 101, Volume: 10.5, QuoteVolume: 1000.25, Closed: true},
		{Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: t0.Add(-30 * time.Second), Open: 200, High: 202, Low: 199, Close: 201, Volume: 10.5, QuoteVolume: 1000.25, Closed: false},
	}
	if !reflect.DeepEqual(ks, want) {
		t.Fatalf("got  %+v\nwant %+v", ks, want)
	}
}

func TestREST_Klines_PagesBeyond1500(t *testing.T) {
	clk := clock.NewFake(t0.Add(48 * time.Hour))
	var mu sync.Mutex
	var limits []string
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		limits = append(limits, req.URL.Query().Get("limit"))
		mu.Unlock()
		st, _ := strconv.ParseInt(req.URL.Query().Get("startTime"), 10, 64)
		n, _ := strconv.Atoi(req.URL.Query().Get("limit"))
		w.Write([]byte("["))
		for i := 0; i < n; i++ {
			if i > 0 {
				w.Write([]byte(","))
			}
			w.Write([]byte(klineRow(time.UnixMilli(st).Add(time.Duration(i)*time.Minute), 1)))
		}
		w.Write([]byte("]"))
	}, 100000, clk)
	start := t0.Truncate(time.Minute)
	ks, err := r.Klines(context.Background(), "BTCUSDT", market.I1m, start, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 2000 {
		t.Fatalf("len %d", len(ks))
	}
	for i, k := range ks {
		if !k.OpenTime.Equal(start.Add(time.Duration(i) * time.Minute)) {
			t.Fatalf("bar %d open %v", i, k.OpenTime)
		}
	}
	if !reflect.DeepEqual(limits, []string{"1500", "500"}) {
		t.Fatalf("limits %v", limits)
	}
}

type atomicQuery struct {
	mu sync.Mutex
	v  url.Values
}

func (a *atomicQuery) set(v url.Values) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicQuery) get() url.Values  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

func TestREST_OpenInterestAndLongShort(t *testing.T) {
	ms := t0.UnixMilli()
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		if q.Get("symbol") != "BTCUSDT" {
			t.Errorf("symbol %q", q.Get("symbol"))
		}
		switch req.URL.Path {
		case "/fapi/v1/openInterest":
			fmt.Fprintf(w, `{"symbol":"BTCUSDT","openInterest":"95231.055","time":%d}`, ms)
		case "/futures/data/globalLongShortAccountRatio", "/futures/data/topLongShortPositionRatio":
			if q.Get("period") != "5m" || q.Get("limit") != "1" {
				t.Errorf("query %v", q)
			}
			ratio := "2.6576"
			if req.URL.Path == "/futures/data/topLongShortPositionRatio" {
				ratio = "1.25"
			}
			fmt.Fprintf(w, `[{"symbol":"BTCUSDT","longShortRatio":"%s","timestamp":%d}]`, ratio, ms)
		default:
			t.Errorf("path %s", req.URL.Path)
			http.NotFound(w, req)
		}
	}, 1200, clock.NewFake(t0))
	ctx := context.Background()
	oi, err := r.OpenInterest(ctx, "BTCUSDT")
	if err != nil || oi != (market.OpenInterestEvent{Symbol: "BTCUSDT", Time: time.UnixMilli(ms).UTC(), OI: 95231.055}) {
		t.Fatalf("oi %+v %v", oi, err)
	}
	g, err := r.LongShort(ctx, "BTCUSDT", "global")
	if err != nil || g != (market.LongShortEvent{Symbol: "BTCUSDT", Time: time.UnixMilli(ms).UTC(), Kind: "global", Ratio: 2.6576}) {
		t.Fatalf("global %+v %v", g, err)
	}
	tp, err := r.LongShort(ctx, "BTCUSDT", "top")
	if err != nil || tp.Ratio != 1.25 || tp.Kind != "top" {
		t.Fatalf("top %+v %v", tp, err)
	}
	if _, err := r.LongShort(ctx, "BTCUSDT", "nope"); err == nil {
		t.Fatal("want error for unknown kind")
	}
}

func TestREST_RateLimited(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"openInterest":"1","time":1}`)
	}, 2, clk)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.OpenInterest(ctx, "BTCUSDT"); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		_, err = r.OpenInterest(ctx, "BTCUSDT")
	}()
	select {
	case <-done:
		t.Fatal("third call did not block although the weight budget is exhausted")
	case <-time.After(50 * time.Millisecond):
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d while blocked", calls.Load())
	}
	waitDone(t, clk, 5*time.Second, done)
	if err != nil {
		t.Fatalf("blocked call returned error: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}
	// Refill is 2 per minute, so one token takes 30s.
	if el := clk.Now().Sub(t0); el < 30*time.Second {
		t.Fatalf("unblocked after only %v", el)
	}
}

func TestREST_RateLimited_HonoursContext(t *testing.T) {
	clk := clock.NewFake(t0)
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, `{"openInterest":"1","time":1}`)
	}, 1, clk)
	if _, err := r.OpenInterest(context.Background(), "X"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := r.OpenInterest(ctx, "X"); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want ctx error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return on cancel")
	}
}

func TestREST_UsedWeightHeaderDrainsBucket(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "100") // 100% of capacity used elsewhere
		fmt.Fprint(w, `{"openInterest":"1","time":1}`)
	}, 100, clk)
	if _, err := r.OpenInterest(context.Background(), "X"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); r.OpenInterest(context.Background(), "X") }()
	select {
	case <-done:
		t.Fatal("call did not block after server reported the weight budget exhausted")
	case <-time.After(50 * time.Millisecond):
	}
	waitDone(t, clk, 100*time.Millisecond, done)
}

func TestREST_429RetriesAfterRetryAfter(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"openInterest":"3","time":1}`)
	}, 1200, clk)
	done := make(chan struct{})
	var oi market.OpenInterestEvent
	var err error
	go func() { defer close(done); oi, err = r.OpenInterest(context.Background(), "X") }()
	waitDone(t, clk, time.Second, done)
	if err != nil || oi.OI != 3 {
		t.Fatalf("oi %+v err %v", oi, err)
	}
	if el := clk.Now().Sub(t0); el < 7*time.Second {
		t.Fatalf("retried after only %v", el)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls %d", calls.Load())
	}
}

func TestREST_429TwiceFailsAndOtherErrorsNotRetried(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	var status atomic.Int32
	status.Store(http.StatusTooManyRequests)
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(int(status.Load()))
		fmt.Fprint(w, `{"code":-1,"msg":"x"}`)
	}, 1200, clk)
	done := make(chan struct{})
	var err error
	go func() { defer close(done); _, err = r.OpenInterest(context.Background(), "X") }()
	waitDone(t, clk, time.Second, done)
	if err == nil || calls.Load() != 2 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}

	calls.Store(0)
	status.Store(http.StatusBadRequest)
	clk.Advance(time.Second) // the second 429 paused all callers for Retry-After
	if _, err := r.OpenInterest(context.Background(), "X"); err == nil || calls.Load() != 1 {
		t.Fatalf("400: err %v calls %d", err, calls.Load())
	}
}

func TestPollContract_EmitsAndSkipsFailingSymbol(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if req.URL.Query().Get("symbol") == "BADUSDT" {
			http.Error(w, `{"code":-1121,"msg":"Invalid symbol."}`, http.StatusBadRequest)
			return
		}
		if req.URL.Path == "/fapi/v1/openInterest" {
			fmt.Fprint(w, `{"openInterest":"5","time":1}`)
			return
		}
		fmt.Fprint(w, `[{"longShortRatio":"1.5","timestamp":1}]`)
	}, 100000, clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan market.Event, 64)
	errc := make(chan error, 1)
	go func() {
		errc <- PollContract(ctx, r, func() []string { return []string{"BADUSDT", "BTCUSDT"} }, 5*time.Minute, out, clk)
	}()
	readRound := func() {
		t.Helper()
		var kinds []string
		for i := 0; i < 3; i++ {
			select {
			case ev := <-out:
				if ev.EventSymbol() != "BTCUSDT" {
					t.Fatalf("event for %s", ev.EventSymbol())
				}
				switch e := ev.(type) {
				case market.OpenInterestEvent:
					kinds = append(kinds, "oi")
				case market.LongShortEvent:
					kinds = append(kinds, e.Kind)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("round incomplete: %v", kinds)
			}
		}
		if !reflect.DeepEqual(kinds, []string{"oi", "global", "top"}) {
			t.Fatalf("kinds %v", kinds)
		}
	}
	readRound() // immediate first round
	select {
	case ev := <-out:
		t.Fatalf("unexpected event before interval: %+v", ev)
	case <-time.After(30 * time.Millisecond):
	}
	clk.Advance(5 * time.Minute)
	readRound()
	cancel()
	select {
	case <-errc:
	case <-time.After(2 * time.Second):
		t.Fatal("PollContract did not stop")
	}
}

func TestKlinesWeight_Table(t *testing.T) {
	for _, c := range []struct{ limit, want int }{{1, 1}, {99, 1}, {100, 2}, {499, 2}, {500, 5}, {1000, 5}, {1001, 10}, {1500, 10}} {
		if got := klinesWeight(c.limit); got != c.want {
			t.Errorf("klinesWeight(%d) = %d, want %d", c.limit, got, c.want)
		}
	}
}

func TestREST_429PausesAllCallers(t *testing.T) {
	clk := clock.NewFake(t0)
	var calls atomic.Int32
	first := make(chan struct{})
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "10")
			w.WriteHeader(http.StatusTeapot)
			close(first)
			return
		}
		fmt.Fprint(w, `{"openInterest":"1","time":1}`)
	}, 1200, clk)
	go r.OpenInterest(context.Background(), "A")
	<-first
	time.Sleep(20 * time.Millisecond) // let the first caller record the pause
	done := make(chan struct{})
	go func() { defer close(done); r.OpenInterest(context.Background(), "B") }()
	select {
	case <-done:
		t.Fatal("second caller not paused by the other caller's 418")
	case <-time.After(50 * time.Millisecond):
	}
	if calls.Load() != 1 {
		t.Fatalf("calls during pause = %d", calls.Load())
	}
	waitDone(t, clk, time.Second, done)
	if el := clk.Now().Sub(t0); el < 10*time.Second {
		t.Fatalf("second caller ran after only %v", el)
	}
}

func TestREST_Contracts(t *testing.T) {
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, `{"symbols":[
			{"symbol":"BTCUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"TRADING","baseAsset":"BTC","underlyingType":"COIN","onboardDate":1569398400000},
			{"symbol":"BTCDOMUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"TRADING","baseAsset":"BTCDOM","underlyingType":"INDEX"},
			{"symbol":"OLDUSDT","contractType":"PERPETUAL","quoteAsset":"USDT","status":"SETTLING","baseAsset":"OLD","underlyingType":"COIN"}]}`)
	}, 1200, clock.NewFake(t0))
	got, err := r.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Contract{
		{Symbol: "BTCUSDT", BaseAsset: "BTC", UnderlyingType: "COIN", OnboardDate: time.UnixMilli(1569398400000).UTC()},
		{Symbol: "BTCDOMUSDT", BaseAsset: "BTCDOM", UnderlyingType: "INDEX"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}

func TestREST_Tickers24h(t *testing.T) {
	r := newTestREST(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/fapi/v1/ticker/24hr" || req.URL.RawQuery != "" {
			t.Errorf("request %s?%s", req.URL.Path, req.URL.RawQuery)
		}
		fmt.Fprint(w, `[{"symbol":"BTCUSDT","priceChangePercent":"-1.25","quoteVolume":"12345678.9"},
			{"symbol":"ETHUSDT","priceChangePercent":"3","quoteVolume":"100"}]`)
	}, 1200, clock.NewFake(t0))
	got, err := r.Tickers24h(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Ticker24h{{"BTCUSDT", 12345678.9, -1.25}, {"ETHUSDT", 100, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}
