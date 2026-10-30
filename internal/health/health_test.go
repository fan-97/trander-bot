package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/notify"
)

type sink struct {
	mu   sync.Mutex
	msgs []notify.Message
}

func (s *sink) send(m notify.Message) {
	s.mu.Lock()
	s.msgs = append(s.msgs, m)
	s.mu.Unlock()
}

func (s *sink) all() []notify.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]notify.Message(nil), s.msgs...)
}

func newTest(t *testing.T) (*Monitor, *clock.Fake, *sink) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s := &sink{}
	return NewMonitor(clk, s.send, time.Minute), clk, s
}

// step advances the fake clock in 10s increments, running a check each time.
func step(m *Monitor, clk *clock.Fake, d time.Duration) {
	for el := time.Duration(0); el < d; el += 10 * time.Second {
		clk.Advance(10 * time.Second)
		m.check()
	}
}

func TestMonitor_DisconnectAlertOnceAfter1m(t *testing.T) {
	m, clk, s := newTest(t)
	m.OnConn(true)
	m.OnConn(false)
	step(m, clk, 50*time.Second)
	if n := len(s.all()); n != 0 {
		t.Fatalf("alert before 1m: %d", n)
	}
	step(m, clk, 10*time.Second)
	got := s.all()
	if len(got) != 1 || got[0].Title != "WS 断开" || got[0].Level != config.LevelCritical {
		t.Fatalf("want one critical WS 断开, got %+v", got)
	}
	step(m, clk, 5*time.Minute)
	if n := len(s.all()); n != 1 {
		t.Fatalf("repeated alert: %d", n)
	}
}

func TestMonitor_RecoveryAlert(t *testing.T) {
	m, clk, s := newTest(t)
	m.OnConn(true)
	m.OnConn(false)
	step(m, clk, 90*time.Second)
	m.OnConn(true)
	m.check()
	got := s.all()
	if len(got) != 2 || got[1].Title != "WS 已恢复" || got[1].Level != config.LevelInfo {
		t.Fatalf("got %+v", got)
	}
	if len(got[1].Lines) == 0 || !strings.Contains(got[1].Lines[0], "1m30s") {
		t.Fatalf("recovery should include duration: %+v", got[1].Lines)
	}
	m.check()
	if len(s.all()) != 2 {
		t.Fatal("duplicate recovery")
	}
}

func TestMonitor_ShortFlapNoAlerts(t *testing.T) {
	m, clk, s := newTest(t)
	m.OnConn(true)
	m.OnConn(false)
	step(m, clk, 20*time.Second)
	m.OnConn(true)
	step(m, clk, 20*time.Second)
	if n := len(s.all()); n != 0 {
		t.Fatalf("flap alerted: %+v", s.all())
	}
}

func TestMonitor_StaleSymbol(t *testing.T) {
	m, clk, s := newTest(t)
	m.SetSymbols([]string{"BTCUSDT", "ETHUSDT"})
	m.OnConn(true)
	for i := 0; i < 8; i++ {
		clk.Advance(10 * time.Second)
		m.OnEvent("BTCUSDT")
		m.check()
	}
	got := s.all()
	if len(got) != 1 || got[0].Title != "数据停更: ETHUSDT" || got[0].Level != config.LevelCritical {
		t.Fatalf("got %+v", got)
	}
	st := m.Snapshot()
	if len(st.Stale) != 1 || st.Stale[0] != "ETHUSDT" {
		t.Fatalf("snapshot %+v", st)
	}
	m.OnEvent("ETHUSDT")
	m.check()
	got = s.all()
	if len(got) != 2 || got[1].Title != "数据恢复: ETHUSDT" || got[1].Level != config.LevelInfo {
		t.Fatalf("got %+v", got)
	}
	m.check()
	if len(s.all()) != 2 || len(m.Snapshot().Stale) != 0 {
		t.Fatal("duplicate recovery or still stale")
	}
}

func TestMonitor_AggregatedStale(t *testing.T) {
	m, clk, s := newTest(t)
	syms := []string{"A", "B", "C", "D", "E", "F"}
	m.SetSymbols(syms)
	m.OnConn(true)
	step(m, clk, 70*time.Second)
	got := s.all()
	if len(got) != 1 {
		t.Fatalf("want 1 aggregated alert, got %d: %+v", len(got), got)
	}
	body := got[0].Title + strings.Join(got[0].Lines, "\n")
	for _, sym := range syms {
		if !strings.Contains(body, sym) {
			t.Fatalf("missing %s in %q", sym, body)
		}
	}
	for _, sym := range syms {
		m.OnEvent(sym)
	}
	m.check()
	got = s.all()
	if len(got) != 2 || got[1].Level != config.LevelInfo {
		t.Fatalf("want one aggregated recovery, got %+v", got)
	}
}

func TestMonitor_NoStaleAlertWhileDisconnected(t *testing.T) {
	m, clk, s := newTest(t)
	m.SetSymbols([]string{"BTCUSDT"})
	m.OnConn(true)
	m.OnConn(false)
	step(m, clk, 3*time.Minute)
	got := s.all()
	if len(got) != 1 || got[0].Title != "WS 断开" {
		t.Fatalf("got %+v", got)
	}
	// after reconnect every symbol gets a fresh grace period
	m.OnConn(true)
	step(m, clk, 50*time.Second)
	for _, g := range s.all() {
		if strings.HasPrefix(g.Title, "数据停更") {
			t.Fatalf("stale alert within grace: %+v", g)
		}
	}
	step(m, clk, 20*time.Second)
	last := s.all()[len(s.all())-1]
	if last.Title != "数据停更: BTCUSDT" {
		t.Fatalf("expected stale after grace, got %+v", s.all())
	}
}

func TestMonitor_NoTrackingBeforeFirstConnect(t *testing.T) {
	m, clk, s := newTest(t)
	m.SetSymbols([]string{"BTCUSDT"})
	step(m, clk, 5*time.Minute)
	if len(s.all()) != 0 {
		t.Fatalf("alert before first connect: %+v", s.all())
	}
	m.OnConn(true)
	step(m, clk, 50*time.Second)
	if len(s.all()) != 0 {
		t.Fatal("cold start alerted")
	}
}

func TestMonitor_NewSymbolGrace(t *testing.T) {
	m, clk, s := newTest(t)
	m.SetSymbols([]string{"BTCUSDT"})
	m.OnConn(true)
	for i := 0; i < 12; i++ {
		clk.Advance(10 * time.Second)
		m.OnEvent("BTCUSDT")
		m.check()
	}
	m.SetSymbols([]string{"BTCUSDT", "NEWUSDT"})
	for i := 0; i < 5; i++ {
		clk.Advance(10 * time.Second)
		m.OnEvent("BTCUSDT")
		m.check()
	}
	if len(s.all()) != 0 {
		t.Fatalf("new symbol alerted inside grace: %+v", s.all())
	}
	for i := 0; i < 2; i++ {
		clk.Advance(10 * time.Second)
		m.OnEvent("BTCUSDT")
		m.check()
	}
	if got := s.all(); len(got) != 1 || got[0].Title != "数据停更: NEWUSDT" {
		t.Fatalf("got %+v", got)
	}
	// removed symbols are forgotten
	m.SetSymbols([]string{"BTCUSDT"})
	if st := m.Snapshot(); len(st.Stale) != 0 || len(st.LastEvent) != 1 {
		t.Fatalf("snapshot after removal: %+v", st)
	}
}

func TestMonitor_AlertRateLimit(t *testing.T) {
	m, clk, s := newTest(t)
	msg := notify.Message{Title: "storage drop", Level: config.LevelWarn}
	m.Alert(msg)
	m.Alert(msg)
	clk.Advance(9 * time.Minute)
	m.Alert(msg)
	if n := len(s.all()); n != 1 {
		t.Fatalf("want 1, got %d", n)
	}
	m.Alert(notify.Message{Title: "other"})
	if n := len(s.all()); n != 2 {
		t.Fatalf("different title should pass, got %d", n)
	}
	clk.Advance(2 * time.Minute)
	m.Alert(msg)
	if n := len(s.all()); n != 3 {
		t.Fatalf("want 3 after 10m, got %d", n)
	}
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestHealthz_503WhenStale(t *testing.T) {
	m, clk, _ := newTest(t)
	h := m.Handler()

	rec := get(h, "/healthz")
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "starting") {
		t.Fatalf("before connect: %d %s", rec.Code, rec.Body)
	}

	m.SetSymbols([]string{"BTCUSDT"})
	m.OnConn(true)
	rec = get(h, "/healthz")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("healthy: %d %s", rec.Code, rec.Body)
	}

	clk.Advance(2 * time.Minute)
	rec = get(h, "/healthz")
	var body struct {
		Status    string   `json:"status"`
		Connected bool     `json:"connected"`
		Stale     []string `json:"stale"`
	}
	if rec.Code != 503 {
		t.Fatalf("stale: %d", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "degraded" || !body.Connected || len(body.Stale) != 1 || body.Stale[0] != "BTCUSDT" {
		t.Fatalf("body %+v", body)
	}

	m.OnEvent("BTCUSDT")
	if rec = get(h, "/healthz"); rec.Code != 200 {
		t.Fatalf("recovered: %d %s", rec.Code, rec.Body)
	}

	m.OnConn(false)
	rec = get(h, "/healthz")
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), `"connected":false`) {
		t.Fatalf("disconnected: %d %s", rec.Code, rec.Body)
	}
}

func TestMetrics_ExposesGauges(t *testing.T) {
	m, clk, _ := newTest(t)
	m.SetSymbols([]string{"BTCUSDT"})
	m.OnConn(true)
	clk.Advance(30 * time.Second)
	m.OnEvent("BTCUSDT")
	clk.Advance(5 * time.Second)
	m.RegisterCounterFunc("traderbot_test_drops_total", "test", func() float64 { return 7 })

	rec := get(m.Handler(), "/metrics")
	if rec.Code != 200 {
		t.Fatalf("code %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"traderbot_ws_connected 1",
		"traderbot_symbols_stale 0",
		`traderbot_symbol_last_event_seconds{symbol="BTCUSDT"} 5`,
		"traderbot_test_drops_total 7",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "go_goroutines") {
		t.Error("should use a private registry without default collectors")
	}
}

func TestMonitor_ConcurrentOnEvent(t *testing.T) {
	m, _, _ := newTest(t)
	syms := make([]string, 20)
	for i := range syms {
		syms[i] = fmt.Sprintf("S%dUSDT", i)
	}
	m.SetSymbols(syms)
	m.OnConn(true)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				m.OnEvent(syms[i%len(syms)])
				if i%500 == 0 {
					m.Snapshot()
					m.check()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			m.SetSymbols(syms[:10+i%10])
			m.OnConn(i%2 == 0)
		}
	}()
	wg.Wait()
}

func TestRun_ReturnsNilOnCancelAndChecks(t *testing.T) {
	m, clk, s := newTest(t)
	m.OnConn(true)
	m.OnConn(false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(s.all()) == 0 && time.Now().Before(deadline) {
		clk.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	if len(s.all()) == 0 {
		t.Fatal("Run never produced the disconnect alert")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}
