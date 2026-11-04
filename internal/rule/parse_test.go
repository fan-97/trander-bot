package rule

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"trader-bot/internal/market"
)

func f(v float64) *float64 { return &v }

func fn(name string, args ...float64) Operand { return Operand{Func: name, Args: args} }
func cn(v float64) Operand                    { return Operand{Const: f(v)} }

func at(o Operand, iv market.Interval) Operand { o.Interval = iv; return o }

func TestParseLeaf(t *testing.T) {
	tests := []struct {
		in   string
		want Leaf
	}{
		{"rsi(14) < 30", Leaf{fn("rsi", 14), OpLT, cn(30)}},
		{"change_pct(1) > 3", Leaf{fn("change_pct", 1), OpGT, cn(3)}},
		{"funding_rate < -0.0005", Leaf{fn("funding_rate"), OpLT, cn(-0.0005)}},
		{"funding_rate < 1e-4", Leaf{fn("funding_rate"), OpLT, cn(1e-4)}},
		{"cross_up(ema(12), ema(26))", Leaf{fn("ema", 12), OpCrossUp, fn("ema", 26)}},
		{"cross_down(close, ema(20))", Leaf{fn("close"), OpCrossDown, fn("ema", 20)}},
		{"cross_up(rsi(14), 30)", Leaf{fn("rsi", 14), OpCrossUp, cn(30)}},
		{"close > highest(20)", Leaf{fn("close"), OpGT, fn("highest", 20)}},
		{"close() >= 5.5", Leaf{fn("close"), OpGTE, cn(5.5)}},
		{"3 <= volume", Leaf{cn(3), OpLTE, fn("volume")}},
		{"oi_change_pct(4) == 0", Leaf{fn("oi_change_pct", 4), OpEQ, cn(0)}},
		{"liq_volume(15) > 1000000", Leaf{fn("liq_volume", 15), OpGT, cn(1000000)}},
		{"macd(12,26,9) > 0", Leaf{fn("macd", 12, 26, 9), OpGT, cn(0)}},
		{"ema(20)@4h > ema(50)@4h", Leaf{at(fn("ema", 20), "4h"), OpGT, at(fn("ema", 50), "4h")}},
		{"low <= ema(20)@4h", Leaf{fn("low"), OpLTE, at(fn("ema", 20), "4h")}},
		{"close@1d > 0", Leaf{at(fn("close"), "1d"), OpGT, cn(0)}},
		{"cross_up(close, ema(20)@4h)", Leaf{fn("close"), OpCrossUp, at(fn("ema", 20), "4h")}},
	}
	for _, tt := range tests {
		got, err := ParseLeaf(tt.in)
		if err != nil {
			t.Errorf("%q: %v", tt.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q: got %+v want %+v", tt.in, got, tt.want)
		}
	}
}

func TestParseLeaf_Errors(t *testing.T) {
	for _, in := range []string{
		"rsi(14) <", "foo(1) > 2", "rsi(1,2) > 3", "3 > 2", "cross_up(1, 2)",
		"cross_up(ema(12))", "cross_up ema(12), ema(26)", "rsi(14) 30", "rsi(14) < 30 extra",
		"rsi(0) < 30", "rsi(-1) < 30", "rsi(1.5) < 30", "funding_rate(1) < 0", "oi_change_pct > 1",
		"liq_volume(0) > 1", "", "rsi(14) = 30", "rsi(14 < 30", "rsi(x) < 30", "1e > 3",
		"ema(20)@2h > 1", "ema(20)@ > 1", "close > 3@4h", "funding_rate@4h < 0", "ema(20)@4h@1h > 1",
	} {
		_, err := ParseLeaf(in)
		if err == nil {
			t.Errorf("%q: expected error", in)
			continue
		}
		if !strings.Contains(err.Error(), in) {
			t.Errorf("%q: error %q lacks expression", in, err)
		}
	}
}

func mustYAML(t *testing.T, s string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(s), &n); err != nil {
		t.Fatal(err)
	}
	return &n
}

func TestParse_Tree(t *testing.T) {
	n, err := Parse(mustYAML(t, `
all:
  - rsi(14) < 30
  - any:
      - funding_rate < -0.0005
      - oi_change_pct(4) > 10
`))
	if err != nil {
		t.Fatal(err)
	}
	want := All{Children: []Node{
		Leaf{fn("rsi", 14), OpLT, cn(30)},
		Any{Children: []Node{
			Leaf{fn("funding_rate"), OpLT, cn(-0.0005)},
			Leaf{fn("oi_change_pct", 4), OpGT, cn(10)},
		}},
	}}
	if !reflect.DeepEqual(n, want) {
		t.Fatalf("got %+v\nwant %+v", n, want)
	}
}

func TestParse_NotAndScalar(t *testing.T) {
	n, err := Parse(mustYAML(t, "not: rsi(14) > 70"))
	if err != nil || !reflect.DeepEqual(n, Not{Child: Leaf{fn("rsi", 14), OpGT, cn(70)}}) {
		t.Fatalf("n=%+v err=%v", n, err)
	}
	n, err = Parse(mustYAML(t, "not: {any: [\"rsi(14) > 70\"]}"))
	if err != nil || !reflect.DeepEqual(n, Not{Child: Any{Children: []Node{Leaf{fn("rsi", 14), OpGT, cn(70)}}}}) {
		t.Fatalf("n=%+v err=%v", n, err)
	}
	n, err = Parse(mustYAML(t, `"rsi(14) < 30"`))
	if err != nil || !reflect.DeepEqual(n, Leaf{fn("rsi", 14), OpLT, cn(30)}) {
		t.Fatalf("n=%+v err=%v", n, err)
	}
}

func TestParse_Errors(t *testing.T) {
	tests := []struct{ in, want string }{
		{"all: []", "at least one"},
		{"any: []", "at least one"},
		{"all: rsi(14) < 30", "sequence"},
		{"not: [\"rsi(14) < 30\"]", "single"},
		{"all: [\"rsi(14) < 30\"]\nany: [\"rsi(14) < 30\"]", "exactly one key"},
		{"xor: [\"rsi(14) < 30\"]", "unknown key"},
		{"all:\n  - rsi(14) < 30\n  - foo(1) > 2", "line 3"},
		{"all:\n  - rsi(14) < 30\n  - foo(1) > 2", "foo(1) > 2"},
		{"[a, b]", "mapping or string"},
		{"{}", "exactly one key"},
	}
	for _, tt := range tests {
		_, err := Parse(mustYAML(t, tt.in))
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%q: err=%v want containing %q", tt.in, err, tt.want)
		}
	}
	if _, err := Parse(nil); err == nil {
		t.Error("nil node: expected error")
	}
}

func TestOperand_StringAndIsContract(t *testing.T) {
	tests := []struct {
		o        Operand
		s        string
		contract bool
	}{
		{fn("rsi", 14), "rsi(14)", false},
		{fn("close"), "close", false},
		{fn("macd", 12, 26, 9), "macd(12,26,9)", false},
		{cn(-0.0005), "-0.0005", false},
		{cn(30), "30", false},
		{fn("funding_rate"), "funding_rate", true},
		{fn("oi_change_pct", 4), "oi_change_pct(4)", true},
		{fn("liq_volume", 15), "liq_volume(15)", true},
	}
	for _, tt := range tests {
		if got := tt.o.String(); got != tt.s {
			t.Errorf("String=%q want %q", got, tt.s)
		}
		if got := tt.o.IsContract(); got != tt.contract {
			t.Errorf("%s IsContract=%v", tt.s, got)
		}
	}
}

func TestFuncs(t *testing.T) {
	n, err := Parse(mustYAML(t, `
all:
  - rsi(14) < 30
  - cross_up(ema(12), ema(26))
  - not: rsi(14) > 70
  - close > ema(12)
  - any: [funding_rate < 0, "5 < rsi(14)"]
`))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range Funcs(n) {
		got = append(got, o.String())
	}
	want := []string{"rsi(14)", "ema(12)", "ema(26)", "close", "funding_rate"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestOperandIntervalString(t *testing.T) {
	l, err := ParseLeaf("ema(20)@4h > close@1h")
	if err != nil {
		t.Fatal(err)
	}
	if got := l.Left.String(); got != "ema(20)@4h" {
		t.Errorf("left %q", got)
	}
	if got := l.Right.String(); got != "close@1h" {
		t.Errorf("right %q", got)
	}
	if got := l.Left.Local().String(); got != "ema(20)" {
		t.Errorf("local %q", got)
	}
	if l.Left.On("1h") != "4h" || fn("close").On("1h") != "1h" {
		t.Error("On")
	}
}

func TestIntervals(t *testing.T) {
	n, err := Parse(mustYAML(t, `
all:
  - ema(20)@4h > ema(50)@4h
  - low <= ema(20)@4h
  - close@1d > ema(20)
`))
	if err != nil {
		t.Fatal(err)
	}
	got := Intervals(n)
	if !reflect.DeepEqual(got, []market.Interval{"4h", "1d"}) {
		t.Errorf("got %v", got)
	}
	// ema(20) and ema(20)@4h are distinct operands.
	if fs := Funcs(n); len(fs) != 5 {
		t.Errorf("funcs %v", fs)
	}
}
