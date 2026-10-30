// Package rule parses rule condition expressions into a tree.
package rule

import (
	"strconv"
	"strings"
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
type Operand struct {
	Func  string
	Args  []float64
	Const *float64
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

// String returns the canonical text, e.g. "rsi(14)", "close", "-0.0005".
func (o Operand) String() string {
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

// Funcs returns the unique function operands used by n, in first-seen order.
func Funcs(n Node) []Operand {
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
	walk(n)
	return out
}
