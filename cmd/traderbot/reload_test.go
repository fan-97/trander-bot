package main

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/health"
	"trader-bot/internal/notify"
)

const reloadBase = `
rules:
  - id: r
    symbols: [btcusdt]
    interval: 5m
    when: close > %s
    channels: [w]
channels:
  w: {type: webhook, params: {url: "http://x"}}
`

func mustParse(t *testing.T, symbols, threshold string) *config.Config {
	t.Helper()
	c, err := config.Parse([]byte("symbols: " + symbols + "\n" + strings.Replace(reloadBase, "%s", threshold, 1)))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type reloadHarness struct {
	raw, store *config.Store
	changed    chan struct{}
	auto       chan []string
	mon        *health.Monitor
	mu         sync.Mutex
	alerts     []notify.Message
}

func (h *reloadHarness) alertTitles() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, a := range h.alerts {
		switch {
		case a.Title == "配置已生效":
			continue // the success confirmation is checked on its own
		case strings.HasPrefix(a.Title, "配置无效 #"):
			out = append(out, "配置无效") // the hash suffix varies with the reason
		default:
			out = append(out, a.Title)
		}
	}
	return out
}

func (h *reloadHarness) applied() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, a := range h.alerts {
		if a.Title == "配置已生效" {
			n++
		}
	}
	return n
}

func startReload(t *testing.T, initial *config.Config) *reloadHarness {
	t.Helper()
	have := map[string]bool{"BTCUSDT": true, "ETHUSDT": true}
	h := &reloadHarness{changed: make(chan struct{}, 1), auto: make(chan []string)}
	h.mon = health.NewMonitor(clock.NewFake(time.Unix(0, 0)), func(m notify.Message) {
		h.mu.Lock()
		h.alerts = append(h.alerts, m)
		h.mu.Unlock()
	}, time.Minute)
	active, _ := filterSymbols(initial, have, nil)
	h.raw, h.store = config.NewStore(initial), config.NewStore(active)
	h.mon.SetSymbols(active.Symbols)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	refresh := func(context.Context) (map[string]bool, error) { return have, nil }
	sub := h.raw.Subscribe()
	go func() {
		defer close(done)
		reloadLoop(ctx, sub, h.auto, initial, h.store, refresh, have, h.mon, h.changed, slog.Default())
	}()
	t.Cleanup(func() { cancel(); <-done })
	return h
}

// publish sets c on the raw store and waits until the loop has handled it
// (store snapshot replaced or an alert raised).
func (h *reloadHarness) publish(t *testing.T, c *config.Config) {
	t.Helper()
	before, nAlerts := h.store.Current(), len(h.alertTitles())
	h.raw.Set(c)
	deadline := time.Now().Add(2 * time.Second)
	for h.store.Current() == before && len(h.alertTitles()) == nAlerts {
		if time.Now().After(deadline) {
			t.Fatal("reload not handled")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *reloadHarness) changedFired() bool {
	select {
	case <-h.changed:
		return true
	default:
		return false
	}
}

func symbolsTracked(m *health.Monitor) []string {
	var out []string
	for s := range m.Snapshot().LastEvent {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func TestReloadLoop_PoolChangeIsFilteredAndSignalled(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	h.publish(t, mustParse(t, "[btcusdt, ethusdt]", "1"))
	got := h.store.Current()
	if !reflect.DeepEqual(got.Symbols, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Errorf("store symbols = %v", got.Symbols)
	}
	if !reflect.DeepEqual(got.Rules[0].Symbols, []string{"BTCUSDT"}) {
		t.Errorf("rule symbols = %v", got.Rules[0].Symbols)
	}
	if !h.changedFired() {
		t.Error("changed not signalled on a pool change")
	}
	if tr := symbolsTracked(h.mon); !reflect.DeepEqual(tr, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Errorf("monitor tracks %v", tr)
	}
	if a := h.alertTitles(); len(a) != 0 {
		t.Errorf("unexpected alerts %v", a)
	}
}

func TestReloadLoop_RulesOnlyEditUpdatesStoreWithoutResubscribe(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	h.publish(t, mustParse(t, "[btcusdt]", "2"))
	if got := h.store.Current().Rules[0].When; reflect.DeepEqual(got, mustParse(t, "[btcusdt]", "1").Rules[0].When) {
		t.Error("store still has the old rule")
	}
	if h.changedFired() {
		t.Error("changed signalled although the pool is unchanged")
	}
}

func TestReloadLoop_AllDroppedKeepsPreviousAndAlerts(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	prev := h.store.Current()
	c, err := config.Parse([]byte(`
symbols: [NOPEUSDT]
rules:
  - {id: r, interval: 5m, when: close > 1, channels: [w]}
channels:
  w: {type: webhook, params: {url: "http://x"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	h.publish(t, c)
	if h.store.Current() != prev {
		t.Error("store replaced although no symbol is tradable")
	}
	if a := h.alertTitles(); !reflect.DeepEqual(a, []string{"配置无效"}) {
		t.Errorf("alerts = %v, want [配置无效]", a)
	}
	if h.changedFired() {
		t.Error("changed signalled for a rejected config")
	}
}

func TestReloadLoop_DroppedSymbolsAlert(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	h.publish(t, mustParse(t, "[btcusdt, NOPEUSDT]", "1"))
	deadline := time.Now().Add(time.Second)
	for len(h.alertTitles()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a := h.alertTitles(); !reflect.DeepEqual(a, []string{"币种已忽略"}) {
		t.Errorf("alerts = %v, want [币种已忽略]", a)
	}
	if got := h.store.Current().Symbols; !reflect.DeepEqual(got, []string{"BTCUSDT"}) {
		t.Errorf("store symbols = %v", got)
	}
	if h.changedFired() {
		t.Error("changed signalled although the validated pool is unchanged")
	}
}

// I5: a reload whose rules point at a channel the running dispatcher does
// not have is rejected (signals to it would be silently dropped).
func TestReloadLoop_RuleOnNewChannelRejected(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	prev := h.store.Current()
	c, err := config.Parse([]byte(`
symbols: [btcusdt]
rules:
  - {id: r, interval: 5m, when: close > 1, channels: [w, x]}
channels:
  w: {type: webhook, params: {url: "http://x"}}
  x: {type: webhook, params: {url: "http://y"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	h.publish(t, c)
	if h.store.Current() != prev {
		t.Error("store replaced although a rule uses a channel that is not running")
	}
	if a := h.alertTitles(); !reflect.DeepEqual(a, []string{"配置无效"}) {
		t.Errorf("alerts = %v, want [配置无效]", a)
	}
}

// I5: changing a running channel's params is applied to the store but warns
// that a restart is needed; a successful reload is confirmed.
func TestReloadLoop_ChannelChangeWarnsRestart(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	c, err := config.Parse([]byte(`
symbols: [btcusdt]
rules:
  - {id: r, interval: 5m, when: close > 1, channels: [w]}
channels:
  w: {type: webhook, params: {url: "http://changed"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	h.publish(t, c)
	deadline := time.Now().Add(time.Second)
	for h.applied() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a := h.alertTitles(); !reflect.DeepEqual(a, []string{"通知渠道变更需重启"}) {
		t.Errorf("alerts = %v, want [通知渠道变更需重启]", a)
	}
	if h.applied() != 1 {
		t.Errorf("配置已生效 sent %d times, want 1", h.applied())
	}
}

// sendAuto hands an auto selection to the loop and waits until it is applied.
func (h *reloadHarness) sendAuto(t *testing.T, auto []string) {
	t.Helper()
	before := h.store.Current()
	h.auto <- auto
	deadline := time.Now().Add(2 * time.Second)
	for h.store.Current() == before {
		if time.Now().After(deadline) {
			t.Fatal("auto selection not applied")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReloadLoop_AutoSymbolsMergeWithManual(t *testing.T) {
	h := startReload(t, mustParse(t, "[btcusdt]", "1"))
	h.sendAuto(t, []string{"ETHUSDT"})
	got := h.store.Current()
	if !reflect.DeepEqual(got.Symbols, []string{"BTCUSDT", "ETHUSDT"}) || !reflect.DeepEqual(got.Auto, []string{"ETHUSDT"}) {
		t.Fatalf("pool %v auto %v", got.Symbols, got.Auto)
	}
	if !h.changedFired() {
		t.Error("stream not resubscribed for the auto symbol")
	}
	if h.applied() != 0 {
		t.Error("an auto update must not announce 配置已生效")
	}
	if !reflect.DeepEqual(symbolsTracked(h.mon), []string{"BTCUSDT", "ETHUSDT"}) {
		t.Errorf("health tracks %v", symbolsTracked(h.mon))
	}

	// A manual edit keeps the auto symbols.
	h.publish(t, mustParse(t, "[btcusdt]", "2"))
	if got := h.store.Current(); !reflect.DeepEqual(got.Auto, []string{"ETHUSDT"}) || len(got.Symbols) != 2 {
		t.Fatalf("after manual edit: pool %v auto %v", got.Symbols, got.Auto)
	}

	// Dropping every auto symbol never touches the manual ones.
	h.sendAuto(t, nil)
	if got := h.store.Current(); !reflect.DeepEqual(got.Symbols, []string{"BTCUSDT"}) || len(got.Auto) != 0 {
		t.Fatalf("after auto cleared: pool %v auto %v", got.Symbols, got.Auto)
	}

	// A symbol added by hand that is also auto-selected counts as manual.
	h.publish(t, mustParse(t, "[btcusdt, ethusdt]", "2"))
	h.sendAuto(t, []string{"ETHUSDT"})
	if got := h.store.Current(); len(got.Symbols) != 2 || len(got.Auto) != 0 {
		t.Fatalf("manual+auto overlap: pool %v auto %v", got.Symbols, got.Auto)
	}
}
