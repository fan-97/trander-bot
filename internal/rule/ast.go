// Package rule parses rule condition expressions into a tree.
package rule

import (
	"slices"
	"strconv"
	"strings"

	"trader-bot/internal/market"
)

// Node is a condition tree node: All, Any, Not or Leaf.
type Node interface{ isNode() }

type All struct{ Children []Node }
type Any struct{ Children []Node }
type Not struct{ Child Node }

// Leaf is a single comparison or crossover.
type Leaf struct {
	Left  Operand
	Op    Op
	Right Operand
}

func (All) isNode()  {}
func (Any) isNode()  {}
func (Not) isNode()  {}
func (Leaf) isNode() {}

// Op is a comparison or crossover operator.
type Op string

const (
	OpGT        Op = ">"
	OpGTE       Op = ">="
	OpLT        Op = "<"
	OpLTE       Op = "<="
	OpEQ        Op = "=="
	OpCrossUp   Op = "cross_up"
	OpCrossDown Op = "cross_down"
)

// Operand is a function call (Func != "") or a numeric constant (Const != nil).
// Interval, set by an "@4h" suffix, makes a kline function read another
// interval's last closed bar; empty means the rule's own interval.
type Operand struct {
	Func     string
	Args     []float64
	Const    *float64
	Interval market.Interval
}

// Local returns o without its interval suffix.
func (o Operand) Local() Operand {
	o.Interval = ""
	return o
}

// On returns the interval o reads: its own suffix, else def.
func (o Operand) On(def market.Interval) market.Interval {
	if o.Interval != "" {
		return o.Interval
	}
	return def
}

// contractArity lists contract-data functions (provided by the worker, not
// kline indicators) with their argument counts.
var contractArity = map[string]int{
	"funding_rate":         0,
	"mark_price":           0,
	"oi":                   0,
	"oi_change_pct":        1,
	"long_short_ratio":     0,
	"top_long_short_ratio": 0,
	"liq_volume":           1,
}

// IsContract reports whether the operand is a contract-data function.
func (o Operand) IsContract() bool {
	if o.Func == "" {
		return false
	}
	_, ok := contractArity[o.Func]
	return ok
}

func fmtNum(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// String returns the canonical text, e.g. "rsi(14)", "close", "-0.0005",
// "ema(20)@4h".
func (o Operand) String() string {
	if o.Interval != "" {
		return o.Local().String() + "@" + string(o.Interval)
	}
	if o.Func == "" {
		if o.Const == nil {
			return ""
		}
		return fmtNum(*o.Const)
	}
	if len(o.Args) == 0 {
		return o.Func
	}
	parts := make([]string, len(o.Args))
	for i, a := range o.Args {
		parts[i] = fmtNum(a)
	}
	return o.Func + "(" + strings.Join(parts, ",") + ")"
}

// Intervals returns the unique "@" intervals referenced by ns, in first-seen
// order.
func Intervals(ns ...Node) []market.Interval {
	var out []market.Interval
	for _, o := range Funcs(ns...) {
		if o.Interval != "" && !slices.Contains(out, o.Interval) {
			out = append(out, o.Interval)
		}
	}
	return out
}

// Funcs returns the unique function operands used by ns, in first-seen order.
func Funcs(ns ...Node) []Operand {
	var out []Operand
	seen := map[string]bool{}
	add := func(o Operand) {
		if o.Func == "" {
			return
		}
		if k := o.String(); !seen[k] {
			seen[k] = true
			out = append(out, o)
		}
	}
	var walk func(Node)
	walk = func(n Node) {
		switch v := n.(type) {
		case All:
			for _, c := range v.Children {
				walk(c)
			}
		case Any:
			for _, c := range v.Children {
				walk(c)
			}
		case Not:
			walk(v.Child)
		case Leaf:
			add(v.Left)
			add(v.Right)
		}
	}
	for _, n := range ns {
		walk(n)
	}
	return out
}

// String renders the leaf as written, e.g. "rsi(14) < 30" or
// "cross_up(ema(12), ema(26))".
func (l Leaf) String() string {
	if l.Op == OpCrossUp || l.Op == OpCrossDown {
		return string(l.Op) + "(" + l.Left.String() + ", " + l.Right.String() + ")"
	}
	return l.Left.String() + " " + string(l.Op) + " " + l.Right.String()
}

// Format renders n on one line: leaves as written, groups as
// "all(a; b)", "any(a; b)" and "not(a)". Nil renders as "".
func Format(n Node) string {
	group := func(name string, kids []Node) string {
		parts := make([]string, len(kids))
		for i, k := range kids {
			parts[i] = Format(k)
		}
		return name + "(" + strings.Join(parts, "; ") + ")"
	}
	switch v := n.(type) {
	case Leaf:
		return v.String()
	case All:
		return group("all", v.Children)
	case Any:
		return group("any", v.Children)
	case Not:
		return "not(" + Format(v.Child) + ")"
	}
	return ""
}
