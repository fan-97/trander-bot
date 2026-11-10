package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"trader-bot/internal/config"
)

const adminCfg = `# my comment
symbols: [BTCUSDT, ETHUSDT]
rules:
  - id: r
    symbols: [ETHUSDT]
    interval: 5m
    when: close > 1
    channels: [w]
  - id: all
    interval: 5m
    when: close > 1
    channels: [w]
channels:
  w: {type: webhook, params: {url: "http://x"}}
`

func TestSymbolAdminHotReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(adminCfg), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{"BTCUSDT": true, "ETHUSDT": true, "SOLUSDT": true}
	raw, store := config.NewStore(cfg), config.NewStore(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := raw.Subscribe()
	go config.Watch(ctx, path, raw, func(err error) { t.Log("watch:", err) })
	go func() { // stand-in for reloadLoop
		for {
			select {
			case <-ctx.Done():
				return
			case c := <-sub:
				next, _ := filterSymbols(c, have, nil)
				store.Set(next)
			}
		}
	}()
	a := newSymbolAdmin(path, store, have, nil, slog.Default())

	res, err := a.Add(ctx, []string{"SOLUSDT", "BTCUSDT", "DOGEUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Changed, []string{"SOLUSDT"}) || !res.Applied || len(res.Notes) != 2 {
		t.Fatalf("add %+v", res)
	}
	if !slices.Contains(store.Current().Symbols, "SOLUSDT") {
		t.Fatal("not hot reloaded")
	}
	data, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(data), "# my comment\nsymbols: [BTCUSDT, ETHUSDT, SOLUSDT]\nrules:") {
		t.Fatalf("file:\n%s", data)
	}

	// ETHUSDT is referenced by rule r: refused with a readable reason.
	if _, err := a.Remove(ctx, []string{"ETHUSDT"}); err == nil || !strings.Contains(err.Error(), "规则 r") {
		t.Fatalf("remove referenced: %v", err)
	}
	res, err = a.Remove(ctx, []string{"btcusdt", "XRPUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Changed, []string{"BTCUSDT"}) || !res.Applied || len(res.Notes) != 1 {
		t.Fatalf("remove %+v", res)
	}
	if slices.Contains(store.Current().Symbols, "BTCUSDT") {
		t.Fatal("removal not hot reloaded")
	}
	v, err := a.View()
	if err != nil || !slices.Equal(v.Manual, []string{"ETHUSDT", "SOLUSDT"}) {
		t.Fatalf("view %+v %v", v, err)
	}
}
