// Package health watches WS connectivity and per-symbol data freshness, sends
// system alerts on state changes, and serves /healthz and /metrics.
//
// It deliberately knows nothing about engine, storage or the exchange: it only
// receives OnEvent, OnConn and SetSymbols calls.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/notify"
)

const (
	checkInterval  = 10 * time.Second
	aggregateAbove = 5 // more than this many symbols in one check -> one aggregated alert
	alertCooldown  = 10 * time.Minute
)

// Status is a point-in-time view of the monitor.
type Status struct {
	Started   bool // true once the first connection has been seen
	Connected bool
	Stale     []string
	LastEvent map[string]time.Time // zero time: no event seen yet
	// ChannelsFailing are notification channels that keep failing (I4).
	ChannelsFailing []string
}

type symState struct {
	last      time.Time // last event; zero if none
	grace     time.Time // start of the current grace period
	alerted   bool      // a stale alert was sent and not yet recovered
	alertedAt time.Time
	gapStart  time.Time // reference time when the alert fired
}

func (s *symState) ref() time.Time {
	if s.last.After(s.grace) {
		return s.last
	}
	return s.grace
}

// Monitor tracks health and emits alerts. It is safe for concurrent use.
type Monitor struct {
	clk        clock.Clock
	alert      func(notify.Message)
	staleAfter time.Duration
	reg        *prometheus.Registry

	mu           sync.Mutex
	started      bool
	connected    bool
	discSince    time.Time
	discAlerted  bool
	symbols      map[string]*symState
	symbolsFixed bool // SetSymbols called: unknown symbols in OnEvent are ignored
	failing      func() []string

	rlMu     sync.Mutex
	lastSent map[string]time.Time
}

// NewMonitor builds a Monitor. alert is typically Dispatcher.SendSystem.
func NewMonitor(clk clock.Clock, alert func(notify.Message), staleAfter time.Duration) *Monitor {
	m := &Monitor{
		clk:        clk,
		alert:      alert,
		staleAfter: staleAfter,
		reg:        prometheus.NewRegistry(),
		symbols:    map[string]*symState{},
		lastSent:   map[string]time.Time{},
	}
	m.reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "traderbot_ws_connected", Help: "1 if all websocket shards are connected.",
		}, func() float64 {
			if m.Snapshot().Connected {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "traderbot_symbols_stale", Help: "Number of symbols with stale data.",
		}, func() float64 { return float64(len(m.Snapshot().Stale)) }),
		ageCollector{m},
	)
	return m
}

// Registry returns the private Prometheus registry so other components can add metrics.
func (m *Monitor) Registry() *prometheus.Registry { return m.reg }

// RegisterCounterFunc registers a counter whose value is read from f at scrape time.
// It panics on duplicate registration (a wiring bug at startup).
func (m *Monitor) RegisterCounterFunc(name, help string, f func() float64) {
	m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, f))
}

type ageCollector struct{ m *Monitor }

var ageDesc = prometheus.NewDesc("traderbot_symbol_last_event_seconds",
	"Seconds since the last event per symbol.", []string{"symbol"}, nil)

func (c ageCollector) Describe(ch chan<- *prometheus.Desc) { ch <- ageDesc }

func (c ageCollector) Collect(ch chan<- prometheus.Metric) {
	now := c.m.clk.Now()
	for sym, t := range c.m.Snapshot().LastEvent {
		if t.IsZero() {
			continue
		}
		ch <- prometheus.MustNewConstMetric(ageDesc, prometheus.GaugeValue, now.Sub(t).Seconds(), sym)
	}
}

// OnEvent records that data arrived for symbol. Cheap: one mutex-guarded map write.
func (m *Monitor) OnEvent(symbol string) {
	now := m.clk.Now()
	m.mu.Lock()
	s := m.symbols[symbol]
	if s == nil {
		if m.symbolsFixed {
			m.mu.Unlock()
			return
		}
		s = &symState{grace: now}
		m.symbols[symbol] = s
	}
	s.last = now
	m.mu.Unlock()
}

// SetSymbols replaces the tracked symbol set. Removed symbols are forgotten;
// added ones get a fresh grace period starting now.
func (m *Monitor) SetSymbols(symbols []string) {
	now := m.clk.Now()
	want := make(map[string]struct{}, len(symbols))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.symbolsFixed = true
	for _, sym := range symbols {
		want[sym] = struct{}{}
		if _, ok := m.symbols[sym]; !ok {
			m.symbols[sym] = &symState{grace: now}
		}
	}
	for sym := range m.symbols {
		if _, ok := want[sym]; !ok {
			delete(m.symbols, sym)
		}
	}
}

// OnConn reports the aggregated WS connection state. Brief flaps are absorbed
// by the staleAfter threshold in check.
func (m *Monitor) OnConn(connected bool) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if connected == m.connected && m.started {
		return
	}
	if connected {
		m.started = true
		for _, s := range m.symbols {
			s.grace = now // fresh grace after (re)connect
		}
	} else if m.connected {
		m.discSince = now
	}
	m.connected = connected
}

// Run checks every 10s until ctx is cancelled. It returns nil on cancel.
func (m *Monitor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-m.clk.After(checkInterval):
			m.check()
		}
	}
}

func (m *Monitor) isStale(s *symState, now time.Time) bool {
	if s.alerted {
		return !s.last.After(s.alertedAt)
	}
	return m.started && m.connected && now.Sub(s.ref()) >= m.staleAfter
}

// check evaluates state and sends any due alerts (outside the lock).
func (m *Monitor) check() {
	now := m.clk.Now()
	var out []notify.Message

	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	if !m.connected {
		// The disconnect alert covers every symbol; suppress per-symbol alerts.
		if !m.discAlerted && now.Sub(m.discSince) >= m.staleAfter {
			m.discAlerted = true
			out = append(out, notify.Message{
				Title: "WS 断开", Level: config.LevelCritical,
				Lines: []string{"已断开 " + fmtDur(now.Sub(m.discSince))},
			})
		}
		m.mu.Unlock()
		m.send(out)
		return
	}
	if m.discAlerted {
		m.discAlerted = false
		out = append(out, notify.Message{
			Title: "WS 已恢复", Level: config.LevelInfo,
			Lines: []string{"断线持续 " + fmtDur(now.Sub(m.discSince))},
		})
	}

	var newStale, recovered []string
	var staleGap, recGap = map[string]time.Duration{}, map[string]time.Duration{}
	for sym, s := range m.symbols {
		switch {
		case s.alerted && s.last.After(s.alertedAt):
			recovered = append(recovered, sym)
			recGap[sym] = s.last.Sub(s.gapStart)
			s.alerted = false
		case !s.alerted && now.Sub(s.ref()) >= m.staleAfter:
			newStale = append(newStale, sym)
			staleGap[sym] = now.Sub(s.ref())
			s.alerted, s.alertedAt, s.gapStart = true, now, s.ref()
		}
	}
	m.mu.Unlock()

	sort.Strings(newStale)
	sort.Strings(recovered)
	out = append(out, build("数据停更", config.LevelCritical, newStale, staleGap, "已停更 ")...)
	out = append(out, build("数据恢复", config.LevelInfo, recovered, recGap, "停更持续 ")...)
	m.send(out)
}

// build makes one message per symbol, or one aggregated message when there are
// more than aggregateAbove symbols.
func build(title string, lvl config.Level, syms []string, gap map[string]time.Duration, prefix string) []notify.Message {
	if len(syms) == 0 {
		return nil
	}
	if len(syms) > aggregateAbove {
		lines := make([]string, 0, len(syms)+1)
		lines = append(lines, strconv.Itoa(len(syms))+" 个币种: "+strings.Join(syms, ", "))
		var max time.Duration
		for _, d := range gap {
			if d > max {
				max = d
			}
		}
		lines = append(lines, prefix+"最长 "+fmtDur(max))
		return []notify.Message{{Title: title + ": " + strconv.Itoa(len(syms)) + " 个币种", Level: lvl, Lines: lines}}
	}
	out := make([]notify.Message, 0, len(syms))
	for _, s := range syms {
		out = append(out, notify.Message{Title: title + ": " + s, Level: lvl, Lines: []string{prefix + fmtDur(gap[s])}})
	}
	return out
}

func (m *Monitor) send(ms []notify.Message) {
	for _, msg := range ms {
		m.alert(msg)
	}
}

func fmtDur(d time.Duration) string { return d.Round(time.Second).String() }

// Alert forwards a system alert, allowing at most one per title per 10 minutes.
func (m *Monitor) Alert(msg notify.Message) {
	now := m.clk.Now()
	m.rlMu.Lock()
	if t, ok := m.lastSent[msg.Title]; ok && now.Sub(t) < alertCooldown {
		m.rlMu.Unlock()
		return
	}
	m.lastSent[msg.Title] = now
	m.rlMu.Unlock()
	m.alert(msg)
}

// Snapshot returns the current status.
func (m *Monitor) Snapshot() Status {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	st := Status{
		Started:   m.started,
		Connected: m.started && m.connected,
		Stale:     []string{},
		LastEvent: make(map[string]time.Time, len(m.symbols)),
	}
	for sym, s := range m.symbols {
		st.LastEvent[sym] = s.last
		if m.isStale(s, now) {
			st.Stale = append(st.Stale, sym)
		}
	}
	sort.Strings(st.Stale)
	st.ChannelsFailing = []string{}
	if m.failing != nil {
		st.ChannelsFailing = append(st.ChannelsFailing, m.failing()...)
	}
	return st
}

// SetFailingChannels sets the source of failing notification channels
// reported by /healthz (e.g. Dispatcher.Failing). Call it before serving;
// f must be safe for concurrent use.
func (m *Monitor) SetFailingChannels(f func() []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failing = f
}

// Handler serves /healthz and /metrics.
func (m *Monitor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", m.healthz)
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	return mux
}

func (m *Monitor) healthz(w http.ResponseWriter, _ *http.Request) {
	st := m.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case !st.Started:
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "starting"})
	case !st.Connected || len(st.Stale) > 0 || len(st.ChannelsFailing) > 0:
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "degraded", "connected": st.Connected, "stale": st.Stale,
			"channels_failing": st.ChannelsFailing})
	default:
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}
