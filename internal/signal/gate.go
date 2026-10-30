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
