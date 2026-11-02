package indicator

import (
	"encoding/csv"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"trader-bot/internal/market"
)

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows[1:]
}

func pf(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func loadKlines(t *testing.T) []market.Kline {
	var ks []market.Kline
	for _, r := range readCSV(t, "testdata/btcusdt_1h.csv") {
		ms, _ := strconv.ParseInt(r[0], 10, 64)
		ks = append(ks, market.Kline{
			Symbol: "BTCUSDT", Interval: market.I1h, OpenTime: time.UnixMilli(ms),
			Open: pf(t, r[1]), High: pf(t, r[2]), Low: pf(t, r[3]), Close: pf(t, r[4]),
			Volume: pf(t, r[5]), Closed: true,
		})
	}
	return ks
}

func TestIndicators_MatchTradingView(t *testing.T) {
	ks := loadKlines(t)
	exp := readCSV(t, "testdata/tv_expected.csv")
	if len(exp) != len(ks) {
		t.Fatalf("rows: %d klines vs %d expected", len(ks), len(exp))
	}
	cases := []struct {
		name string
		col  int
		ind  Indicator
	}{
		{"rsi14", 1, NewRSI(14)},
		{"ema12", 2, NewEMA(12)},
		{"ema26", 3, NewEMA(26)},
		{"macd", 4, NewMACD(12, 26, 9, MACDLine)},
		{"macd_signal", 5, NewMACD(12, 26, 9, MACDSignal)},
		{"macd_hist", 6, NewMACD(12, 26, 9, MACDHist)},
		{"bb_upper", 7, NewBoll(20, 2, BollUpper)},
		{"bb_mid", 8, NewBoll(20, 2, BollMid)},
		{"bb_lower", 9, NewBoll(20, 2, BollLower)},
		{"atr14", 10, NewATR(14)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			compared := 0
			for i, k := range ks {
				c.ind.Update(k)
				cell := exp[i][c.col]
				got, ok := c.ind.Value()
				if cell == "" {
					if ok {
						t.Fatalf("row %d: defined (%v) but expected undefined", i, got)
					}
					continue
				}
				if !ok {
					t.Fatalf("row %d: not ready, want %s", i, cell)
				}
				want := pf(t, cell)
				if math.Abs(got-want) >= 1e-6 {
					t.Fatalf("row %d: got %.10f want %.10f", i, got, want)
				}
				compared++
			}
			if compared < 200 {
				t.Fatalf("only %d rows compared", compared)
			}
		})
	}
}

func bar(c float64) market.Kline {
	return market.Kline{Close: c, High: c, Low: c, Volume: 1, Closed: true}
}

func TestRSI_NotReadyBeforeN(t *testing.T) {
	r := NewRSI(14)
	for i := 0; i < 14; i++ { // 14 bars give only 13 changes
		r.Update(bar(float64(100 + i%3)))
		if _, ok := r.Value(); ok {
			t.Fatalf("ready after %d bars", i+1)
		}
	}
	r.Update(bar(101))
	if _, ok := r.Value(); !ok {
		t.Fatal("not ready after 15 bars")
	}
}

func TestNew_UnknownName(t *testing.T) {
	if _, err := New("nope", nil); err == nil {
		t.Fatal("want error")
	}
}

func TestNew_ArgValidation(t *testing.T) {
	bad := []struct {
		name string
		args []float64
	}{
		{"rsi", nil}, {"rsi", []float64{14, 1}}, {"rsi", []float64{0}}, {"rsi", []float64{-3}},
		{"rsi", []float64{2.5}}, {"macd", []float64{12, 26}}, {"macd_hist", []float64{12, 26, 0}},
		{"boll_upper", []float64{20}}, {"boll_mid", []float64{20.5, 2}}, {"boll_lower", []float64{20, 0}},
		{"close", []float64{1}}, {"volume", []float64{1}}, {"highest", []float64{}},
	}
	for _, b := range bad {
		if _, err := New(b.name, b.args); err == nil {
			t.Errorf("New(%s,%v): want error", b.name, b.args)
		}
	}
	good := []struct {
		name string
		args []float64
	}{
		{"rsi", []float64{14}}, {"macd", []float64{12, 26, 9}}, {"boll_upper", []float64{20, 2.5}},
		{"close", nil}, {"volume", nil}, {"volume_ratio", []float64{20}}, {"sma", []float64{5}},
		{"ema", []float64{5}}, {"macd_signal", []float64{12, 26, 9}}, {"boll_mid", []float64{20, 2}},
		{"boll_lower", []float64{20, 2}}, {"macd_hist", []float64{12, 26, 9}}, {"atr", []float64{14}},
		{"change_pct", []float64{3}}, {"highest", []float64{3}}, {"lowest", []float64{3}},
	}
	for _, g := range good {
		if _, err := New(g.name, g.args); err != nil {
			t.Errorf("New(%s,%v): %v", g.name, g.args, err)
		}
	}
}

func feed(ind Indicator, ks ...market.Kline) {
	for _, k := range ks {
		ind.Update(k)
	}
}

func TestIgnoresUnclosed(t *testing.T) {
	s := NewSMA(2)
	feed(s, bar(1), bar(3))
	u := bar(100)
	u.Closed = false
	s.Update(u)
	if v, _ := s.Value(); v != 2 {
		t.Fatalf("got %v", v)
	}
}

func TestPrev(t *testing.T) {
	s := NewSMA(2)
	feed(s, bar(1), bar(3))
	if _, ok := s.Prev(); ok {
		t.Fatal("prev should be undefined after one defined value")
	}
	s.Update(bar(5))
	p, ok := s.Prev()
	v, _ := s.Value()
	if !ok || p != 2 || v != 4 {
		t.Fatalf("prev=%v ok=%v v=%v", p, ok, v)
	}
}

func TestSimpleIndicators(t *testing.T) {
	mk := func(c, h, l, v float64) market.Kline {
		return market.Kline{Close: c, High: h, Low: l, Volume: v, Closed: true}
	}
	ks := []market.Kline{mk(10, 12, 8, 100), mk(11, 15, 9, 200), mk(12, 13, 7, 300), mk(15, 14, 10, 600)}

	vr := NewVolumeRatio(3)
	feed(vr, ks[:3]...)
	if _, ok := vr.Value(); ok {
		t.Fatal("volume_ratio ready too early")
	}
	vr.Update(ks[3])
	if v, ok := vr.Value(); !ok || v != 600.0/200 {
		t.Fatalf("volume_ratio %v %v", v, ok)
	}

	cp := NewChangePct(3)
	feed(cp, ks[:3]...)
	if _, ok := cp.Value(); ok {
		t.Fatal("change_pct ready too early")
	}
	cp.Update(ks[3])
	if v, _ := cp.Value(); math.Abs(v-50) > 1e-9 {
		t.Fatalf("change_pct %v", v)
	}

	hi, lo := NewHighest(3), NewLowest(3)
	feed(hi, ks[:3]...)
	feed(lo, ks[:3]...)
	if _, ok := hi.Value(); ok {
		t.Fatal("highest ready too early")
	}
	feed(hi, ks[3])
	feed(lo, ks[3])
	if v, _ := hi.Value(); v != 15 {
		t.Fatalf("highest %v", v) // excludes current high 14
	}
	if v, _ := lo.Value(); v != 7 {
		t.Fatalf("lowest %v", v)
	}

	c, vol := NewClose(), NewVolume()
	feed(c, ks...)
	feed(vol, ks...)
	if v, _ := c.Value(); v != 15 {
		t.Fatal(v)
	}
	if v, _ := vol.Value(); v != 600 {
		t.Fatal(v)
	}
}

func TestRing(t *testing.T) {
	r := NewRing(3)
	for i := 1; i <= 5; i++ {
		r.Push(float64(i))
	}
	if !r.Full() || r.At(0) != 3 || r.Last() != 5 || r.Len() != 3 {
		t.Fatal("ring wrong")
	}
}
