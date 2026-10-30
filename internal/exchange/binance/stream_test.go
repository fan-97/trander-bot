package binance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
)

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func ms(v int64) time.Time { return time.UnixMilli(v).UTC() }

func TestParseKlineMsg(t *testing.T) {
	ev, err := parseMessage(readSample(t, "ws_kline_closed.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := market.KlineEvent{Kline: market.Kline{
		Symbol: "BTCUSDT", Interval: market.I1m, OpenTime: ms(1793383020000),
		Open: 77086.90, High: 77114.80, Low: 77086.80, Close: 77095.70,
		Volume: 84.094, QuoteVolume: 6483378.05390, Closed: true,
	}}
	if ev != want {
		t.Fatalf("got  %+v\nwant %+v", ev, want)
	}
	ev, err = parseMessage(readSample(t, "ws_kline.json"))
	if err != nil {
		t.Fatal(err)
	}
	k, ok := ev.(market.KlineEvent)
	if !ok || k.Closed || !k.OpenTime.Equal(ms(1793382960000)) || k.Open != 77095.30 {
		t.Fatalf("open bar parsed as %+v", ev)
	}
}

func TestParseMarkPriceMsg(t *testing.T) {
	ev, err := parseMessage(readSample(t, "ws_markprice.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := market.MarkPriceEvent{Symbol: "BTCUSDT", Time: ms(1793382962000), MarkPrice: 77100.54753623,
		FundingRate: 0.0001, NextFunding: ms(1793404800000)}
	if ev != want {
		t.Fatalf("got  %+v\nwant %+v", ev, want)
	}
}

func TestParseForceOrderMsg(t *testing.T) {
	raw := readSample(t, "ws_forceorder.json")
	ev, err := parseMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := market.LiquidationEvent{Symbol: "QUSDT", Time: ms(1793383049281), Side: "SELL", Price: 0.0463017, Qty: 6298}
	if ev != want {
		t.Fatalf("got  %+v\nwant %+v", ev, want)
	}
	// Without an average price the order price is used.
	noAP := strings.Replace(string(raw), `"ap":"0.0463017"`, `"ap":"0"`, 1)
	ev, err = parseMessage([]byte(noAP))
	if err != nil {
		t.Fatal(err)
	}
	if ev.(market.LiquidationEvent).Price != 0.0453 {
		t.Fatalf("fallback price: %+v", ev)
	}
}

func TestParseMessage_IgnoresUnknownAndRejectsGarbage(t *testing.T) {
	if ev, err := parseMessage([]byte(`{"result":null,"id":1}`)); ev != nil || err != nil {
		t.Fatalf("subscribe ack: %v %v", ev, err)
	}
	if _, err := parseMessage([]byte(`{"stream":"x","data":{"e":"kline","k":{"t":1,"o":"abc"}}}`)); err == nil {
		t.Fatal("want error for bad number")
	}
}

func TestStreamURLs_SplitsLargePools(t *testing.T) {
	var syms []string
	for i := 0; i < 350; i++ {
		syms = append(syms, fmt.Sprintf("S%dUSDT", i))
	}
	urls := streamURLs("ws://h/market/", syms)
	if len(urls) < 2 {
		t.Fatalf("350 symbols in %d connection(s)", len(urls))
	}
	seen := map[string]bool{}
	for _, u := range urls {
		if !strings.HasPrefix(u, "ws://h/market/stream?streams=") {
			t.Fatalf("url %s", u)
		}
		streams := strings.Split(strings.TrimPrefix(u, "ws://h/market/stream?streams="), "/")
		if len(u) > maxURLLen {
			t.Fatalf("url length %d", len(u))
		}
		if len(streams) > 1024 {
			t.Fatalf("%d streams on one connection", len(streams))
		}
		for _, s := range streams {
			seen[s] = true
		}
	}
	for _, s := range syms {
		l := strings.ToLower(s)
		for _, st := range []string{l + "@kline_1m", l + "@markPrice@1s", l + "@forceOrder"} {
			if !seen[st] {
				t.Fatalf("missing stream %s", st)
			}
		}
	}
}

// ---- fake websocket server ----

type srvConn struct {
	c      *websocket.Conn
	query  string
	closed chan struct{} // closed when the client side goes away
}

func (sc *srvConn) send(t *testing.T, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sc.c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		t.Fatalf("server write: %v", err)
	}
}

type wsServer struct {
	conns chan *srvConn
	url   string
}

func newWSServer(t *testing.T) *wsServer {
	ws := &wsServer{conns: make(chan *srvConn, 16)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		sc := &srvConn{c: c, query: r.URL.RawQuery, closed: make(chan struct{})}
		ws.conns <- sc
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				close(sc.closed)
				c.CloseNow()
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	ws.url = "ws" + strings.TrimPrefix(srv.URL, "http") + "/market"
	return ws
}

func (ws *wsServer) next(t *testing.T) *srvConn {
	t.Helper()
	select {
	case sc := <-ws.conns:
		return sc
	case <-time.After(2 * time.Second):
		t.Fatal("no websocket connection")
		return nil
	}
}

func (ws *wsServer) none(t *testing.T) {
	t.Helper()
	select {
	case <-ws.conns:
		t.Fatal("unexpected websocket connection")
	case <-time.After(50 * time.Millisecond):
	}
}

type harness struct {
	t      *testing.T
	clk    *clock.Fake
	ws     *wsServer
	s      *Stream
	states chan bool
	out    chan market.Event
	mu     sync.Mutex
	syms   []string
	change chan struct{}
}

func newHarness(t *testing.T, restHandler http.HandlerFunc, syms ...string) *harness {
	h := &harness{t: t, clk: clock.NewFake(t0), ws: newWSServer(t), states: make(chan bool, 16),
		out: make(chan market.Event, 64), syms: syms, change: make(chan struct{}, 1)}
	if restHandler == nil {
		restHandler = func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "[]") }
	}
	rest := newTestREST(t, restHandler, 100000, h.clk)
	rest.Logger = quietLogger(t)
	h.s = NewStream(h.ws.url, rest, h.clk, func(up bool) { h.states <- up })
	h.s.Logger = rest.Logger
	return h
}

func (h *harness) symbols() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.syms...)
}

func (h *harness) run() {
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- h.s.Run(ctx, h.symbols, h.change, h.out) }()
	h.t.Cleanup(func() {
		cancel()
		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				h.t.Errorf("Run returned %v", err)
			}
		case <-time.After(2 * time.Second):
			h.t.Error("Run did not return after cancel")
		}
	})
}

func (h *harness) state(want bool) {
	h.t.Helper()
	select {
	case got := <-h.states:
		if got != want {
			h.t.Fatalf("onState(%v), want onState(%v)", got, want)
		}
	case <-time.After(2 * time.Second):
		h.t.Fatalf("no onState(%v)", want)
	}
}

func (h *harness) noState() {
	h.t.Helper()
	select {
	case got := <-h.states:
		h.t.Fatalf("unexpected onState(%v)", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func (h *harness) event() market.Event {
	h.t.Helper()
	select {
	case ev := <-h.out:
		return ev
	case <-time.After(2 * time.Second):
		h.t.Fatal("no event")
		return nil
	}
}

func (h *harness) noEvent() {
	h.t.Helper()
	select {
	case ev := <-h.out:
		h.t.Fatalf("unexpected event %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func klineMsg(sym string, open time.Time, closed bool, px float64) string {
	return fmt.Sprintf(`{"stream":"%s@kline_1m","data":{"e":"kline","E":%d,"s":"%s","k":{"t":%d,"T":%d,"s":"%s","i":"1m","o":"%g","c":"%g","h":"%g","l":"%g","v":"1","x":%v,"q":"2"}}}`,
		strings.ToLower(sym), open.UnixMilli()+1000, sym, open.UnixMilli(), open.UnixMilli()+59999, sym, px, px, px, px, closed)
}

func wantKline(t *testing.T, ev market.Event, open time.Time, closed bool) {
	t.Helper()
	k, ok := ev.(market.KlineEvent)
	if !ok || !k.OpenTime.Equal(open) || k.Closed != closed {
		t.Fatalf("got %+v, want kline open=%v closed=%v", ev, open.Format("15:04"), closed)
	}
}

func TestStream_ReconnectsAfterDrop(t *testing.T) {
	h := newHarness(t, nil, "BTCUSDT")
	h.run()
	sc1 := h.ws.next(t)
	if sc1.query != "streams=btcusdt@kline_1m/btcusdt@markPrice@1s/btcusdt@forceOrder" {
		t.Fatalf("query %q", sc1.query)
	}
	h.state(true)
	sc1.send(t, string(readSample(t, "ws_markprice.json")))
	if _, ok := h.event().(market.MarkPriceEvent); !ok {
		t.Fatal("want mark price event")
	}

	sc1.c.CloseNow()
	h.state(false)
	h.ws.none(t) // waits for backoff
	h.clk.Advance(time.Second)
	h.ws.next(t).c.CloseNow() // second connection dies immediately
	h.state(true)
	h.state(false)

	// Backoff doubled to 2s.
	h.clk.Advance(time.Second)
	h.ws.none(t)
	h.clk.Advance(time.Second)
	h.ws.next(t)
	h.state(true)
}

func TestStream_BackoffResetsAfterStableMinute(t *testing.T) {
	h := newHarness(t, nil, "BTCUSDT")
	h.run()
	h.ws.next(t).c.CloseNow()
	h.state(true)
	h.state(false)
	h.clk.Advance(time.Second)
	sc := h.ws.next(t)
	h.state(true)
	h.clk.Advance(time.Minute) // stays up for a minute (keep it alive with a message)
	sc.send(t, klineMsg("BTCUSDT", t0, false, 1))
	h.event()
	sc.c.CloseNow()
	h.state(false)
	h.clk.Advance(time.Second) // back to 1s, not 2s
	h.ws.next(t)
	h.state(true)
}

func TestStream_ReadTimeoutReconnects(t *testing.T) {
	h := newHarness(t, nil, "BTCUSDT")
	h.run()
	h.ws.next(t)
	h.state(true)
	h.clk.Advance(2 * time.Minute)
	h.noState()
	h.clk.Advance(time.Minute)
	h.state(false)
	h.clk.Advance(time.Second)
	h.ws.next(t)
	h.state(true)
}

func TestStream_GapFill(t *testing.T) {
	m := t0.Truncate(time.Minute) // 12:00; clock starts at 12:00:30
	var restCalls int
	var mu sync.Mutex
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		restCalls++
		mu.Unlock()
		q := r.URL.Query()
		if r.URL.Path != "/fapi/v1/klines" || q.Get("symbol") != "BTCUSDT" || q.Get("interval") != "1m" ||
			q.Get("startTime") != fmt.Sprint(m.UnixMilli()) {
			t.Errorf("rest %s %v", r.URL.Path, q)
		}
		// 12:00..12:03; at 12:03:30 the last one is still open.
		fmt.Fprintf(w, "[%s,%s,%s,%s]", klineRow(m, 1), klineRow(m.Add(time.Minute), 2),
			klineRow(m.Add(2*time.Minute), 3), klineRow(m.Add(3*time.Minute), 4))
	}, "BTCUSDT")
	h.run()
	sc1 := h.ws.next(t)
	h.state(true)
	sc1.send(t, klineMsg("BTCUSDT", m.Add(-time.Minute), true, 0))
	wantKline(t, h.event(), m.Add(-time.Minute), true)

	sc1.c.CloseNow()
	h.state(false)
	h.clk.Advance(3 * time.Minute) // missed 12:00, 12:01, 12:02
	sc2 := h.ws.next(t)
	h.state(true)
	sc2.send(t, klineMsg("BTCUSDT", m.Add(-2*time.Minute), true, 0)) // stale
	sc2.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))  // duplicate of gap fill
	sc2.send(t, klineMsg("BTCUSDT", m.Add(3*time.Minute), false, 4)) // live, open

	wantKline(t, h.event(), m, true)
	wantKline(t, h.event(), m.Add(time.Minute), true)
	wantKline(t, h.event(), m.Add(2*time.Minute), true)
	wantKline(t, h.event(), m.Add(3*time.Minute), false)
	h.noEvent()
	mu.Lock()
	defer mu.Unlock()
	if restCalls != 1 {
		t.Fatalf("rest calls %d", restCalls)
	}
}

func TestStream_RotatesWithoutGap(t *testing.T) {
	m := t0.Truncate(time.Minute)
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected REST call during rotation: %s", r.URL)
		fmt.Fprint(w, "[]")
	}, "BTCUSDT")
	h.s.readTimeout = 48 * time.Hour // keep the watchdog out of this test
	h.run()
	sc1 := h.ws.next(t)
	h.state(true)
	sc1.send(t, klineMsg("BTCUSDT", m, true, 1))
	wantKline(t, h.event(), m, true)

	h.clk.Advance(23 * time.Hour)
	sc2 := h.ws.next(t)
	sc2.send(t, klineMsg("BTCUSDT", m, true, 1)) // overlap duplicate
	sc2.send(t, klineMsg("BTCUSDT", m.Add(time.Minute), false, 2))
	wantKline(t, h.event(), m.Add(time.Minute), false)
	select {
	case <-sc1.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old connection not closed after rotation")
	}
	h.noState()
	h.noEvent()
}

func TestStream_ResubscribesOnChange(t *testing.T) {
	h := newHarness(t, nil, "BTCUSDT")
	h.run()
	sc1 := h.ws.next(t)
	h.state(true)
	h.mu.Lock()
	h.syms = []string{"BTCUSDT", "ethusdt"}
	h.mu.Unlock()
	h.change <- struct{}{}
	sc2 := h.ws.next(t)
	if !strings.Contains(sc2.query, "ethusdt@kline_1m") || !strings.Contains(sc2.query, "btcusdt@kline_1m") {
		t.Fatalf("query %q", sc2.query)
	}
	select {
	case <-sc1.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old connection not closed")
	}
	h.state(false)
	h.state(true)
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// eventAdvancing returns the next event, advancing the fake clock meanwhile
// so retry waits elapse.
func (h *harness) eventAdvancing(step time.Duration) market.Event {
	h.t.Helper()
	limit := time.After(2 * time.Second)
	for {
		select {
		case ev := <-h.out:
			return ev
		case <-limit:
			h.t.Fatal("no event")
			return nil
		case <-time.After(2 * time.Millisecond):
			h.clk.Advance(step)
		}
	}
}

// holeHarness: connected, last closed bar 11:59 emitted, clock then moved to
// 12:03:30 with the connection kept up (watchdog disabled).
func holeHarness(t *testing.T, rest http.HandlerFunc, seed bool) (*harness, *srvConn) {
	m := t0.Truncate(time.Minute)
	h := newHarness(t, rest, "BTCUSDT")
	h.s.readTimeout = 48 * time.Hour
	if seed {
		h.s.Seed("BTCUSDT", m.Add(-time.Minute))
	}
	h.run()
	sc := h.ws.next(t)
	h.state(true)
	if !seed {
		sc.send(t, klineMsg("BTCUSDT", m.Add(-time.Minute), true, 0))
		wantKline(t, h.event(), m.Add(-time.Minute), true)
	}
	h.clk.Advance(3 * time.Minute)
	return h, sc
}

func holeRows(w http.ResponseWriter) {
	m := t0.Truncate(time.Minute)
	fmt.Fprintf(w, "[%s,%s]", klineRow(m, 1), klineRow(m.Add(time.Minute), 2))
}

func TestStream_HoleFillRetriesThenSucceeds(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if q := r.URL.Query(); q.Get("startTime") != fmt.Sprint(m.UnixMilli()) {
			t.Errorf("startTime %s", q.Get("startTime"))
		}
		holeRows(w)
	}, false)
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3)) // 12:00, 12:01 missing
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m, true)
	wantKline(t, h.event(), m.Add(time.Minute), true)
	wantKline(t, h.event(), m.Add(2*time.Minute), true)
	h.noEvent()
	if calls.Load() != 2 {
		t.Fatalf("rest calls %d", calls.Load())
	}
}

func TestStream_HoleFillGivesUpLogsErrorEmitsLive(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}, false)
	var logs syncBuf
	h.s.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	start := h.clk.Now()
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m.Add(2*time.Minute), true)
	if calls.Load() != 4 {
		t.Fatalf("attempts %d, want 4 (1 + 3 retries)", calls.Load())
	}
	if el := h.clk.Now().Sub(start); el < 7*time.Second {
		t.Fatalf("retries waited only %v", el)
	}
	if l := logs.String(); !strings.Contains(l, "level=ERROR") || !strings.Contains(l, "symbol=BTCUSDT") {
		t.Fatalf("no error log: %s", l)
	}
	// The hole is accepted; the next bar is not blocked.
	sc.send(t, klineMsg("BTCUSDT", m.Add(3*time.Minute), true, 4))
	wantKline(t, h.event(), m.Add(3*time.Minute), true)
}

func TestStream_SeedThenLiveBarFillsHole(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		holeRows(w)
	}, true)
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	wantKline(t, h.event(), m, true)
	wantKline(t, h.event(), m.Add(time.Minute), true)
	wantKline(t, h.event(), m.Add(2*time.Minute), true)
	h.noEvent()
	if calls.Load() != 1 {
		t.Fatalf("rest calls %d", calls.Load())
	}
}

func TestStream_GapFillSkipsBarsNotSafelyClosed(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// REST itself would mark 12:02 closed at 12:03:01.
		fmt.Fprintf(w, "[%s,%s,%s]", klineRow(m, 1), klineRow(m.Add(time.Minute), 2), klineRow(m.Add(2*time.Minute), 3))
	}, "BTCUSDT")
	h.run()
	sc1 := h.ws.next(t)
	h.state(true)
	sc1.send(t, klineMsg("BTCUSDT", m.Add(-time.Minute), true, 0))
	wantKline(t, h.event(), m.Add(-time.Minute), true)
	sc1.c.CloseNow()
	h.state(false)
	h.clk.Advance(2*time.Minute + 31*time.Second) // now 12:03:01
	sc2 := h.ws.next(t)
	h.state(true)
	wantKline(t, h.event(), m, true)
	wantKline(t, h.event(), m.Add(time.Minute), true)
	h.noEvent() // 12:02 ended only 1s ago: left to the live stream
	sc2.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	wantKline(t, h.event(), m.Add(2*time.Minute), true)
	if calls.Load() != 1 {
		t.Fatalf("rest calls %d", calls.Load())
	}
}

func TestNewStream_WarnsOnEmptyPath(t *testing.T) {
	var logs syncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)
	NewStream("wss://fstream.binance.com", nil, nil, nil)
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("no warning for empty path: %q", logs.String())
	}
	logs = syncBuf{}
	NewStream(DefaultWSURL, nil, nil, nil)
	if logs.String() != "" {
		t.Fatalf("unexpected log: %q", logs.String())
	}
}

func TestStream_HoleFillUnderLongPauseFailsFast(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		holeRows(w)
	}, false)
	var logs syncBuf
	h.s.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	h.s.rest.bucket.pause(h.clk.Now().Add(600 * time.Second)) // e.g. a 429 with Retry-After: 600
	before := h.clk.Now()
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	wantKline(t, h.event(), m.Add(2*time.Minute), true) // no clock advance needed
	sc.send(t, klineMsg("BTCUSDT", m.Add(3*time.Minute), false, 4))
	wantKline(t, h.event(), m.Add(3*time.Minute), false)
	if !h.clk.Now().Equal(before) || calls.Load() != 0 {
		t.Fatalf("clock moved %v, rest calls %d", h.clk.Now().Sub(before), calls.Load())
	}
	l := logs.String()
	if !strings.Contains(l, "level=ERROR") || strings.Contains(l, "retrying") {
		t.Fatalf("want immediate give-up with an error log, got: %s", l)
	}
}

func TestStream_HoleFillAttemptTimesOut(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done() // hangs until the client gives up
	}, false)
	if h.s.fillTimeout != 10*time.Second {
		t.Fatalf("fillTimeout = %v, want 10s", h.s.fillTimeout)
	}
	start := h.clk.Now()
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	waitCalls := func(n int32) {
		t.Helper()
		limit := time.After(2 * time.Second)
		for calls.Load() < n {
			select {
			case <-limit:
				t.Fatalf("rest calls %d, want %d", calls.Load(), n)
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitCalls(1)
	h.clk.Advance(9 * time.Second)
	h.noEvent()
	if calls.Load() != 1 {
		t.Fatalf("attempt ended before 10s: calls %d", calls.Load())
	}
	h.clk.Advance(time.Second) // attempt 1 times out, then a 1s back-off
	waitCalls(1)
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m.Add(2*time.Minute), true)
	if calls.Load() != 4 {
		t.Fatalf("attempts %d", calls.Load())
	}
	if el := h.clk.Now().Sub(start); el < 47*time.Second {
		t.Fatalf("4 timed-out attempts + back-off took only %v", el)
	}
}

func TestStream_HolePartlyFilledLogsWarnNotError(t *testing.T) {
	m := t0.Truncate(time.Minute)
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "[%s]", klineRow(m, 1)) // 12:01 missing on the exchange
	}, false)
	var logs syncBuf
	h.s.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	sc.send(t, klineMsg("BTCUSDT", m.Add(2*time.Minute), true, 3))
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m, true)
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m.Add(2*time.Minute), true)
	if l := logs.String(); strings.Contains(l, "level=ERROR") || !strings.Contains(l, "partly filled") {
		t.Fatalf("logs: %s", l)
	}
}

// A closed frame lost around a reconnect is recovered from the next bar's
// first unclosed frame once the missing bar is safely closed (closeMargin),
// instead of waiting a whole minute for the next closed frame.
func TestStream_UnclosedFrameAfterHoleTriggersFill(t *testing.T) {
	m := t0.Truncate(time.Minute) // 12:00; clock starts at 12:00:30
	var calls atomic.Int32
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if q := r.URL.Query(); q.Get("startTime") != fmt.Sprint(m.UnixMilli()) {
			t.Errorf("startTime %s", q.Get("startTime"))
		}
		fmt.Fprintf(w, "[%s]", klineRow(m, 1))
	}, "BTCUSDT")
	h.s.readTimeout = 48 * time.Hour
	h.run()
	sc1 := h.ws.next(t)
	h.state(true)
	sc1.send(t, klineMsg("BTCUSDT", m.Add(-time.Minute), true, 0))
	wantKline(t, h.event(), m.Add(-time.Minute), true)

	sc1.c.CloseNow() // the closed 12:00 frame is lost while reconnecting
	h.state(false)
	h.clk.Advance(31 * time.Second) // 12:01:01
	sc2 := h.ws.next(t)
	h.state(true) // reconnect gap fill: 12:00 ended only 1s ago, not fetched
	h.noEvent()

	// Within closeMargin of the hole's end: forwarded, no fill yet.
	sc2.send(t, klineMsg("BTCUSDT", m.Add(time.Minute), false, 2))
	wantKline(t, h.event(), m.Add(time.Minute), false)
	if c := calls.Load(); c != 0 {
		t.Fatalf("rest calls %d before closeMargin", c)
	}

	h.clk.Advance(2 * time.Second) // 12:01:03
	sc2.send(t, klineMsg("BTCUSDT", m.Add(time.Minute), false, 2))
	wantKline(t, h.event(), m, true)
	wantKline(t, h.event(), m.Add(time.Minute), false)
	sc2.send(t, klineMsg("BTCUSDT", m.Add(time.Minute), false, 2))
	wantKline(t, h.event(), m.Add(time.Minute), false)
	h.noEvent()
	if c := calls.Load(); c != 1 {
		t.Fatalf("rest calls %d, want 1", c)
	}
}

// A failed fill from an unclosed frame is not retried on every following
// unclosed frame of the same bar (each would hold emitMu for seconds).
func TestStream_UnclosedFrameFillTriedOncePerBar(t *testing.T) {
	m := t0.Truncate(time.Minute)
	var calls atomic.Int32
	h, sc := holeHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, "[]") // REST does not have the bars
	}, false) // last closed 11:59, now 12:03:30
	sc.send(t, klineMsg("BTCUSDT", m.Add(3*time.Minute), false, 4))
	wantKline(t, h.eventAdvancing(500*time.Millisecond), m.Add(3*time.Minute), false)
	tried := calls.Load()
	if tried == 0 {
		t.Fatal("no fill attempted")
	}
	sc.send(t, klineMsg("BTCUSDT", m.Add(3*time.Minute), false, 4))
	wantKline(t, h.event(), m.Add(3*time.Minute), false)
	if c := calls.Load(); c != tried {
		t.Fatalf("rest calls %d after a second unclosed frame, want %d", c, tried)
	}
}
