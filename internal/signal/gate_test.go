package signal

import (
	"sync"
	"testing"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/rule"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newGate() (*Gate, *clock.Fake) {
	f := clock.NewFake(t0)
	return NewGate(f), f
}

func obs(g *Gate, r rule.Result, cd time.Duration) (bool, string) {
	return g.Observe("r1", "BTCUSDT", r, cd)
}

func TestGate_EdgeOnly(t *testing.T) {
	g, _ := newGate()
	for i, want := range []bool{true, false, false} {
		if fire, s := obs(g, rule.True, 0); fire != want || s != "" {
			t.Fatalf("i=%d fire=%v s=%q", i, fire, s)
		}
	}
}

func TestGate_RearmAfterFalse(t *testing.T) {
	g, f := newGate()
	n := 0
	for _, r := range []rule.Result{rule.True, rule.False, rule.True} {
		if fire, _ := obs(g, r, time.Minute); fire {
			n++
		}
		f.Advance(2 * time.Minute)
	}
	if n != 2 {
		t.Fatalf("fires=%d", n)
	}
}

func TestGate_Cooldown(t *testing.T) {
	g, f := newGate()
	if fire, _ := obs(g, rule.True, 30*time.Minute); !fire {
		t.Fatal("first")
	}
	obs(g, rule.False, 30*time.Minute)
	if fire, s := obs(g, rule.True, 30*time.Minute); fire || s != "cooldown" {
		t.Fatalf("fire=%v s=%q", fire, s)
	}
	// suppressed edge left state True; still True does not edge
	if fire, s := obs(g, rule.True, 30*time.Minute); fire || s != "" {
		t.Fatalf("fire=%v s=%q", fire, s)
	}
	f.Advance(30 * time.Minute)
	obs(g, rule.False, 30*time.Minute)
	if fire, _ := obs(g, rule.True, 30*time.Minute); !fire {
		t.Fatal("after cooldown")
	}
}

func TestGate_CooldownStartsAtFire(t *testing.T) {
	g, f := newGate()
	obs(g, rule.True, 30*time.Minute) // fire at t0
	f.Advance(20 * time.Minute)
	obs(g, rule.False, 30*time.Minute)
	obs(g, rule.True, 30*time.Minute) // suppressed at t0+20m
	f.Advance(11 * time.Minute)       // t0+31m: past fire+30m
	obs(g, rule.False, 30*time.Minute)
	if fire, _ := obs(g, rule.True, 30*time.Minute); !fire {
		t.Fatal("cooldown must run from the fired signal, not the suppressed edge")
	}
}

func TestGate_UnknownKeepsState(t *testing.T) {
	g, _ := newGate()
	n := 0
	for _, r := range []rule.Result{rule.True, rule.Unknown, rule.True} {
		if fire, _ := obs(g, r, 0); fire {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("fires=%d", n)
	}
}

func TestGate_NoCooldownWhenNonPositive(t *testing.T) {
	g, _ := newGate()
	for _, cd := range []time.Duration{0, -time.Hour} {
		obs(g, rule.False, cd)
		if fire, s := obs(g, rule.True, cd); !fire || s != "" {
			t.Fatalf("cd=%v fire=%v s=%q", cd, fire, s)
		}
	}
}

func TestGate_KeysIndependent(t *testing.T) {
	g, _ := newGate()
	a, _ := g.Observe("r1", "BTCUSDT", rule.True, time.Hour)
	b, _ := g.Observe("r1", "ETHUSDT", rule.True, time.Hour)
	c, _ := g.Observe("r2", "BTCUSDT", rule.True, time.Hour)
	if !a || !b || !c {
		t.Fatal(a, b, c)
	}
}

func TestGate_Forget(t *testing.T) {
	g, _ := newGate()
	g.Observe("r1", "BTCUSDT", rule.True, time.Hour)
	g.Observe("r1", "ETHUSDT", rule.True, time.Hour)
	g.Observe("r2", "BTCUSDT", rule.True, time.Hour)
	g.Forget("r1")
	if f, _ := g.Observe("r1", "BTCUSDT", rule.True, time.Hour); !f {
		t.Fatal("r1 BTC not forgotten")
	}
	if f, _ := g.Observe("r1", "ETHUSDT", rule.True, time.Hour); !f {
		t.Fatal("r1 ETH not forgotten")
	}
	if f, _ := g.Observe("r2", "BTCUSDT", rule.True, time.Hour); f {
		t.Fatal("r2 wrongly forgotten")
	}
}

func TestGate_ForgetSymbol(t *testing.T) {
	g, _ := newGate()
	g.Observe("r1", "BTCUSDT", rule.True, time.Hour)
	g.Observe("r2", "BTCUSDT", rule.True, time.Hour)
	g.Observe("r1", "ETHUSDT", rule.True, time.Hour)
	g.ForgetSymbol("BTCUSDT")
	if f, _ := g.Observe("r1", "BTCUSDT", rule.True, time.Hour); !f {
		t.Fatal("r1 BTC not forgotten")
	}
	if f, _ := g.Observe("r2", "BTCUSDT", rule.True, time.Hour); !f {
		t.Fatal("r2 BTC not forgotten")
	}
	if f, _ := g.Observe("r1", "ETHUSDT", rule.True, time.Hour); f {
		t.Fatal("ETH wrongly forgotten")
	}
}

func TestGate_Concurrent(t *testing.T) {
	g, _ := newGate()
	var wg sync.WaitGroup
	var mu sync.Mutex
	fires := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f, _ := g.Observe("r1", "BTCUSDT", rule.True, time.Hour); f {
				mu.Lock()
				fires++
				mu.Unlock()
			}
			g.Observe("r2", "ETHUSDT", rule.False, time.Hour)
			g.Forget("r3")
			g.ForgetSymbol("XRPUSDT")
		}()
	}
	wg.Wait()
	if fires != 1 {
		t.Fatalf("fires=%d", fires)
	}
}

func TestGate_ForgetRuleSymbol(t *testing.T) {
	g, _ := newGate()
	g.Observe("r1", "BTCUSDT", rule.True, time.Hour)
	g.Observe("r1", "ETHUSDT", rule.True, time.Hour)
	g.ForgetRuleSymbol("r1", "BTCUSDT")
	if f, _ := g.Observe("r1", "BTCUSDT", rule.True, time.Hour); !f {
		t.Fatal("r1 BTC not forgotten")
	}
	if f, _ := g.Observe("r1", "ETHUSDT", rule.True, time.Hour); f {
		t.Fatal("r1 ETH wrongly forgotten")
	}
}

// R23: Prime marks a condition already true at boot as seen without firing,
// so it fires only after going False->True again.
func TestGate_PrimeTrueSuppressesFirstEdge(t *testing.T) {
	g := NewGate(clock.NewFake(time.Unix(0, 0)))
	g.Prime("r", "BTC", rule.True)
	if f, _ := g.Observe("r", "BTC", rule.True, 0); f {
		t.Fatal("primed-true condition fired")
	}
	g.Observe("r", "BTC", rule.False, 0)
	if f, _ := g.Observe("r", "BTC", rule.True, 0); !f {
		t.Fatal("did not fire after False->True")
	}
	// Priming False (or Unknown) leaves the first edge alive.
	g.Prime("q", "BTC", rule.False)
	g.Prime("u", "BTC", rule.Unknown)
	for _, id := range []string{"q", "u"} {
		if f, _ := g.Observe(id, "BTC", rule.True, 0); !f {
			t.Fatalf("%s: first edge suppressed", id)
		}
	}
}

// ResetEdge clears the edge state but keeps the cooldown, so editing a rule
// cannot be used to bypass it.
func TestGate_ResetEdgeKeepsCooldown(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	g := NewGate(clk)
	if f, _ := g.Observe("r", "BTC", rule.True, time.Hour); !f {
		t.Fatal("first edge")
	}
	g.ResetEdge("r", "BTC")
	if f, s := g.Observe("r", "BTC", rule.True, time.Hour); f || s != "cooldown" {
		t.Fatalf("after reset: fire=%v suppressed=%q, want cooldown", f, s)
	}
	clk.Advance(time.Hour)
	g.ResetEdge("r", "BTC")
	if f, _ := g.Observe("r", "BTC", rule.True, time.Hour); !f {
		t.Fatal("did not fire after cooldown")
	}
}

func TestGate_LastFired(t *testing.T) {
	g, clk := newGate()
	g.Observe("r", "BTC", rule.True, time.Hour)
	clk.Advance(time.Minute)
	g.Observe("r", "BTC", rule.False, time.Hour)
	g.Observe("r", "BTC", rule.True, time.Hour) // inside cooldown: not a fire
	g.Observe("r", "ETH", rule.False, time.Hour)
	g.Observe("other", "BTC", rule.True, time.Hour)
	got := g.LastFired("r")
	if len(got) != 1 || !got["BTC"].Equal(t0) {
		t.Fatalf("got %v", got)
	}
}
