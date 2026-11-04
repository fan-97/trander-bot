// Package signal holds the signal type and the edge-trigger/cooldown gate.
package signal

import (
	"sync"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/market"
	"trader-bot/internal/rule"
)

// Signal is one rule hit for one symbol. Suppressed is ""|"cooldown"|"quiet"|"rate".
type Signal struct {
	RuleID, Symbol string
	Interval       market.Interval
	Level          config.Level
	Time           time.Time
	Price          float64
	Detail         string
	Channels       []string
	Suppressed     string

	// BarTime is the open time of the evaluated bar of Interval; zero when the
	// signal came from a contract event rather than a bar. BarLive reports
	// that the bar was still open (realtime rule).
	BarTime time.Time
	BarLive bool
	// ChangePct is the bar's change from its open, in %; valid if HasChange.
	ChangePct float64
	HasChange bool
	// Targets are ATR-based reference stop/take-profit prices; nil when the
	// rule has none or its ATR was not ready.
	Targets *Targets
}

// Targets holds reference prices derived from the signal price and ATR.
type Targets struct {
	Side        config.Side
	ATRPeriod   int
	ATRInterval market.Interval
	ATR         float64
	Stop        float64 // valid if HasStop
	HasStop     bool
	Takes       []float64
}

type key struct{ rule, symbol string }

type state struct {
	last      bool // previous result was True
	lastFired time.Time
	fired     bool
}

// Gate turns a stream of rule results into edge-triggered, cooled-down fires.
// It is safe for concurrent use.
type Gate struct {
	clk clock.Clock
	mu  sync.Mutex
	st  map[key]*state
}

func NewGate(clk clock.Clock) *Gate {
	return &Gate{clk: clk, st: map[key]*state{}}
}

// Observe records r for (ruleID, symbol). fire is true only on a False->True
// edge outside the cooldown, which runs from the last fired signal. An edge
// inside the cooldown returns (false, "cooldown") but still marks the state
// True. Unknown leaves the state unchanged. cooldown <= 0 means none.
func (g *Gate) Observe(ruleID, symbol string, r rule.Result, cooldown time.Duration) (fire bool, suppressed string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key{ruleID, symbol}
	s := g.st[k]
	if s == nil {
		s = &state{}
		g.st[k] = s
	}
	switch r {
	case rule.True:
		wasTrue := s.last
		s.last = true
		if wasTrue {
			return false, ""
		}
		now := g.clk.Now()
		if cooldown > 0 && s.fired && now.Sub(s.lastFired) < cooldown {
			return false, "cooldown"
		}
		s.fired, s.lastFired = true, now
		return true, ""
	case rule.False:
		s.last = false
	}
	return false, ""
}

// Prime seeds the edge state of (ruleID, symbol) from a result computed
// before going live (warmup), without firing or touching the cooldown. A
// condition already True at boot then counts as seen and fires only after it
// goes False->True again, so a restart does not re-announce it (R23).
// Unknown leaves the state unchanged.
func (g *Gate) Prime(ruleID, symbol string, r rule.Result) {
	if r == rule.Unknown {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	k := key{ruleID, symbol}
	s := g.st[k]
	if s == nil {
		s = &state{}
		g.st[k] = s
	}
	s.last = r == rule.True
}

// LastFired returns, per symbol, when ruleID last fired (cooldown-suppressed
// edges do not count); symbols that never fired are absent.
func (g *Gate) LastFired(ruleID string) map[string]time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := map[string]time.Time{}
	for k, s := range g.st {
		if k.rule == ruleID && s.fired {
			out[k.symbol] = s.lastFired
		}
	}
	return out
}

// ResetEdge clears the edge state of (ruleID, symbol) but keeps its cooldown,
// so a rule edited on reload re-arms without bypassing the cooldown.
func (g *Gate) ResetEdge(ruleID, symbol string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if s := g.st[key{ruleID, symbol}]; s != nil {
		s.last = false
	}
}

// Forget drops all state for a rule (every symbol).
func (g *Gate) Forget(ruleID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.st {
		if k.rule == ruleID {
			delete(g.st, k)
		}
	}
}

// ForgetSymbol drops all state for a symbol (every rule).
func (g *Gate) ForgetSymbol(symbol string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.st {
		if k.symbol == symbol {
			delete(g.st, k)
		}
	}
}

// ForgetRuleSymbol drops the state of one (rule, symbol) pair. A worker
// reloading its own rules uses it so it never clears another symbol's state
// that the other worker has already re-established under the new config.
func (g *Gate) ForgetRuleSymbol(ruleID, symbol string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.st, key{ruleID, symbol})
}
