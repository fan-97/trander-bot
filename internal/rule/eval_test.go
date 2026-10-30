package rule

import "testing"

// mapValues is a test Values keyed by Operand.String(); value is {cur, prev}.
// A missing key yields ok=false.
type mapValues map[string][2]float64

func (m mapValues) Get(o Operand) (float64, float64, bool) {
	v, ok := m[o.String()]
	return v[0], v[1], ok
}

func leaf(t *testing.T, s string) Leaf {
	t.Helper()
	l, err := ParseLeaf(s)
	if err != nil {
		t.Fatalf("ParseLeaf(%q): %v", s, err)
	}
	return l
}

func TestEval_Comparisons(t *testing.T) {
	v := mapValues{"rsi(14)": {30, 30}}
	cases := []struct {
		expr string
		want Result
	}{
		{"rsi(14) > 29", True}, {"rsi(14) > 30", False},
		{"rsi(14) >= 30", True}, {"rsi(14) >= 31", False},
		{"rsi(14) < 31", True}, {"rsi(14) < 30", False},
		{"rsi(14) <= 30", True}, {"rsi(14) <= 29", False},
		{"rsi(14) == 30", True}, {"rsi(14) == 30.5", False},
	}
	for _, c := range cases {
		l := leaf(t, c.expr)
		if got := Eval(l, v); got != c.want {
			t.Errorf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
	if Eval(leaf(t, "rsi(14) > 29"), v) != True {
		t.Fatal("sanity")
	}
	// constant on the left is resolved by Eval, not Values.
	if got := Eval(leaf(t, "20 < rsi(14)"), v); got != True {
		t.Errorf("const-left = %v", got)
	}
}

func TestEval_CrossUp(t *testing.T) {
	l := leaf(t, "cross_up(ema(12), ema(26))")
	cases := []struct {
		name string
		v    mapValues
		want Result
	}{
		{"crosses", mapValues{"ema(12)": {5, 3}, "ema(26)": {4, 4}}, True},
		{"already above", mapValues{"ema(12)": {5, 4.5}, "ema(26)": {4, 4}}, False},
		{"prev equal counts", mapValues{"ema(12)": {5, 4}, "ema(26)": {4, 4}}, True},
		{"cur equal no", mapValues{"ema(12)": {4, 3}, "ema(26)": {4, 4}}, False},
		{"missing", mapValues{"ema(12)": {5, 3}}, Unknown},
	}
	for _, c := range cases {
		if got := Eval(l, c.v); got != c.want {
			t.Errorf("%s: %v want %v", c.name, got, c.want)
		}
	}
}

func TestEval_CrossDown(t *testing.T) {
	l := leaf(t, "cross_down(ema(12), ema(26))")
	if got := Eval(l, mapValues{"ema(12)": {3, 5}, "ema(26)": {4, 4}}); got != True {
		t.Errorf("crosses: %v", got)
	}
	if got := Eval(l, mapValues{"ema(12)": {3, 3.5}, "ema(26)": {4, 4}}); got != False {
		t.Errorf("already below: %v", got)
	}
	if got := Eval(l, mapValues{"ema(26)": {4, 4}}); got != Unknown {
		t.Errorf("missing: %v", got)
	}
}

func TestEval_CrossAgainstConstant(t *testing.T) {
	l := leaf(t, "cross_up(rsi(14), 30)")
	if got := Eval(l, mapValues{"rsi(14)": {31, 29}}); got != True {
		t.Errorf("got %v", got)
	}
	if got := Eval(l, mapValues{"rsi(14)": {31, 30.5}}); got != False {
		t.Errorf("got %v", got)
	}
}

func TestEval_UnknownNeverFires(t *testing.T) {
	v := mapValues{"close": {10, 9}}
	unk := leaf(t, "rsi(14) < 30")
	tru := leaf(t, "close > 0")
	fls := leaf(t, "close < 0")
	cases := []struct {
		name string
		n    Node
		want Result
	}{
		{"all unknown+true", All{[]Node{unk, tru}}, Unknown},
		{"all unknown+false", All{[]Node{unk, fls}}, False},
		{"any unknown+false", Any{[]Node{unk, fls}}, Unknown},
		{"any unknown+true", Any{[]Node{unk, tru}}, True},
		{"not unknown", Not{unk}, Unknown},
		{"not true", Not{tru}, False},
		{"not false", Not{fls}, True},
		{"all true", All{[]Node{tru, tru}}, True},
		{"any false", Any{[]Node{fls, fls}}, False},
		{"empty all", All{}, True},
		{"empty any", Any{}, False},
	}
	for _, c := range cases {
		if got := Eval(c.n, v); got != c.want {
			t.Errorf("%s: %v want %v", c.name, got, c.want)
		}
	}
}

func TestEval_ConstNotQueried(t *testing.T) {
	// Values has nothing; a const-vs-const leaf still evaluates.
	if got := Eval(Leaf{Left: Operand{Const: fp(2)}, Op: OpGT, Right: Operand{Const: fp(1)}}, mapValues{}); got != True {

		t.Errorf("got %v", got)
	}
}

func TestResultString(t *testing.T) {
	if False.String() != "false" || True.String() != "true" || Unknown.String() != "unknown" {
		t.Fatal("Result.String")
	}
}

func TestFmtDesc(t *testing.T) {
	cases := map[float64]string{
		27.3123456:  "27.312346",
		27.31:       "27.31",
		-0.0007:     "-0.0007",
		30:          "30",
		1e-9:        "0",
		-1e-9:       "0",
		101.5:       "101.5",
		0.1234564:   "0.123456",
		123456789.5: "123456789.5",
	}
	for in, want := range cases {
		if got := fmtDesc(in); got != want {
			t.Errorf("fmtDesc(%v) = %q want %q", in, got, want)
		}
	}
}

func TestDescribe(t *testing.T) {
	v := mapValues{
		"rsi(14)":      {27.3123456, 31},
		"funding_rate": {-0.0007, -0.0001},
		"ema(12)":      {101.5, 100},
		"ema(26)":      {100.9, 101},
		"close":        {10, 9},
	}
	n := All{[]Node{
		leaf(t, "rsi(14) < 30"),
		leaf(t, "funding_rate < -0.0005"),
	}}
	if got, want := Describe(n, v), "rsi(14)=27.312346 < 30; funding_rate=-0.0007 < -0.0005"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	if got, want := Describe(leaf(t, "cross_up(ema(12), ema(26))"), v), "ema(12)=101.5 cross_up ema(26)=100.9"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	// false / unknown leaves omitted
	n2 := Any{[]Node{
		leaf(t, "close < 0"),
		leaf(t, "rsi(7) < 1"), // unknown
		leaf(t, "close > 5"),
	}}
	if got, want := Describe(n2, v), "close=10 > 5"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	// Not describes nothing
	if got := Describe(Not{leaf(t, "close < 0")}, v); got != "" {
		t.Errorf("not: %q", got)
	}
	n3 := All{[]Node{Not{leaf(t, "close < 0")}, leaf(t, "close > 5")}}
	if got, want := Describe(n3, v), "close=10 > 5"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	// nothing true
	if got := Describe(leaf(t, "close < 0"), v); got != "" {
		t.Errorf("false leaf: %q", got)
	}
}

func fp(f float64) *float64 { return &f }
