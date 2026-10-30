package rule

import (
	"strconv"
	"strings"
)

// Values supplies operand values. cur is the latest value, prev the value one
// step earlier (needed by cross_up/cross_down). ok=false means unavailable.
// Eval resolves constant operands itself and only calls Get for functions.
type Values interface {
	Get(o Operand) (cur, prev float64, ok bool)
}

// Result is a three-valued logic outcome.
type Result int

const (
	False Result = iota
	True
	Unknown
)

func (r Result) String() string {
	switch r {
	case False:
		return "false"
	case True:
		return "true"
	}
	return "unknown"
}

func operandValue(o Operand, v Values) (cur, prev float64, ok bool) {
	if o.Func == "" {
		if o.Const == nil {
			return 0, 0, false
		}
		return *o.Const, *o.Const, true
	}
	return v.Get(o)
}

// Eval evaluates n with Kleene three-valued logic. A leaf with any needed
// operand unavailable is Unknown. Equality (==) is exact float equality.
func Eval(n Node, v Values) Result {
	switch t := n.(type) {
	case Leaf:
		return evalLeaf(t, v)
	case All:
		res := True
		for _, c := range t.Children {
			switch Eval(c, v) {
			case False:
				return False
			case Unknown:
				res = Unknown
			}
		}
		return res
	case Any:
		res := False
		for _, c := range t.Children {
			switch Eval(c, v) {
			case True:
				return True
			case Unknown:
				res = Unknown
			}
		}
		return res
	case Not:
		switch Eval(t.Child, v) {
		case True:
			return False
		case False:
			return True
		}
		return Unknown
	}
	return Unknown
}

func b2r(b bool) Result {
	if b {
		return True
	}
	return False
}

func evalLeaf(l Leaf, v Values) Result {
	ca, pa, ok1 := operandValue(l.Left, v)
	cb, pb, ok2 := operandValue(l.Right, v)
	if !ok1 || !ok2 {
		return Unknown
	}
	switch l.Op {
	case OpGT:
		return b2r(ca > cb)
	case OpGTE:
		return b2r(ca >= cb)
	case OpLT:
		return b2r(ca < cb)
	case OpLTE:
		return b2r(ca <= cb)
	case OpEQ:
		return b2r(ca == cb)
	case OpCrossUp:
		return b2r(pa <= pb && ca > cb)
	case OpCrossDown:
		return b2r(pa >= pb && ca < cb)
	}
	return Unknown
}

// fmtDesc formats a number for messages: shortest representation, rounded to
// at most 6 decimal places with trailing zeros trimmed ("-0" becomes "0").
func fmtDesc(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if i := strings.IndexByte(s, '.'); i >= 0 && len(s)-i-1 > 6 {
		s = strconv.FormatFloat(v, 'f', 6, 64)
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		s = "0"
	}
	return s
}

func describeOperand(o Operand, cur float64) string {
	if o.Func == "" {
		return fmtDesc(cur)
	}
	return o.String() + "=" + fmtDesc(cur)
}

// Describe renders every leaf that evaluates True, in tree order, joined by
// "; " (e.g. "rsi(14)=27.31 < 30"). Constants render as the bare number.
// Leaves under Not, and False/Unknown leaves, are omitted.
func Describe(n Node, v Values) string {
	var parts []string
	var walk func(Node)
	walk = func(n Node) {
		switch t := n.(type) {
		case All:
			for _, c := range t.Children {
				walk(c)
			}
		case Any:
			for _, c := range t.Children {
				walk(c)
			}
		case Leaf:
			if evalLeaf(t, v) != True {
				return
			}
			ca, _, _ := operandValue(t.Left, v)
			cb, _, _ := operandValue(t.Right, v)
			parts = append(parts, describeOperand(t.Left, ca)+" "+string(t.Op)+" "+describeOperand(t.Right, cb))
		}
	}
	walk(n)
	return strings.Join(parts, "; ")
}
