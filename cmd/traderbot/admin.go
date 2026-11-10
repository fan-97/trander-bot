package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"trader-bot/internal/config"
	"trader-bot/internal/tgadmin"
)

// adminApplyWait bounds how long an edit waits for the hot reload to reach
// the running config before replying "已写入配置，等待热加载". A var for tests.
var adminApplyWait = 20 * time.Second

// symbolAdmin is the tgadmin.Backend: it edits the symbols list in the config
// file (config.EditSymbols) and waits until config.Watch -> reloadLoop has
// published the change to store. The file stays the single source of truth,
// so a hand edit and a Telegram edit never fight.
type symbolAdmin struct {
	path  string
	store *config.Store // the running config (validated symbols + auto)
	// tradable returns the current USDT perpetuals; cached is used when it fails.
	tradable func(ctx context.Context) (map[string]bool, error)
	log      *slog.Logger

	mu     sync.Mutex
	cached map[string]bool
}

func newSymbolAdmin(path string, store *config.Store, have map[string]bool,
	tradable func(context.Context) (map[string]bool, error), log *slog.Logger) *symbolAdmin {
	return &symbolAdmin{path: path, store: store, tradable: tradable, cached: have, log: log}
}

func (a *symbolAdmin) View() (tgadmin.View, error) {
	manual, err := config.FileSymbols(a.path)
	if err != nil {
		return tgadmin.View{}, err
	}
	cur := a.store.Current()
	running := map[string]bool{}
	for _, s := range cur.Symbols {
		running[s] = true
	}
	v := tgadmin.View{Inactive: map[string]bool{}, Auto: cur.Auto, AutoOn: autoOn(cur)}
	for _, s := range manual {
		v.Manual = append(v.Manual, s)
		if !running[strings.ToUpper(strings.TrimSpace(s))] {
			v.Inactive[s] = true
		}
	}
	return v, nil
}

func (a *symbolAdmin) exchange(ctx context.Context) map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tradable != nil {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		have, err := a.tradable(cctx)
		cancel()
		if err == nil {
			a.cached = have
		} else {
			a.log.Warn("telegram admin: exchangeInfo failed, using the cached symbol list", "err", err)
		}
	}
	return a.cached
}

func (a *symbolAdmin) Add(ctx context.Context, syms []string) (tgadmin.Result, error) {
	have := a.exchange(ctx)
	var res tgadmin.Result
	var want []string
	for _, s := range syms {
		if have != nil && !have[s] {
			res.Notes = append(res.Notes, s+" 不是可交易的 U 本位永续合约，已跳过")
			continue
		}
		want = append(want, s)
	}
	if len(want) == 0 {
		return res, nil
	}
	_, err := config.EditSymbols(a.path, func(cur []string) ([]string, error) {
		next := slices.Clone(cur)
		for _, s := range want {
			if containsFold(next, s) {
				res.Notes = append(res.Notes, s+" 已在列表中")
				continue
			}
			next = append(next, s)
			res.Changed = append(res.Changed, s)
		}
		return next, nil
	})
	if err != nil || len(res.Changed) == 0 {
		res.Changed = nil
		return res, err
	}
	a.log.Info("telegram admin: symbols added", "symbols", res.Changed)
	res.Applied = a.waitApplied(ctx, res.Changed, nil)
	return res, nil
}

func (a *symbolAdmin) Remove(ctx context.Context, syms []string) (tgadmin.Result, error) {
	var res tgadmin.Result
	auto := a.store.Current().Auto
	_, err := config.EditSymbols(a.path, func(cur []string) ([]string, error) {
		next := slices.Clone(cur)
		for _, s := range syms {
			s = strings.ToUpper(strings.TrimSpace(s))
			n := len(next)
			next = slices.DeleteFunc(next, func(x string) bool { return strings.EqualFold(strings.TrimSpace(x), s) })
			switch {
			case len(next) < n:
				res.Changed = append(res.Changed, s)
			case slices.Contains(auto, s):
				res.Notes = append(res.Notes, s+" 是自动选币选中的，不能在这里删除（可加到 auto_symbols.exclude）")
			default:
				res.Notes = append(res.Notes, s+" 不在手动币种列表中")
			}
		}
		if len(res.Changed) == 0 {
			return cur, nil
		}
		if err := checkRuleRefs(a.path, res.Changed); err != nil {
			return nil, err
		}
		return next, nil
	})
	if err != nil || len(res.Changed) == 0 {
		res.Changed = nil
		return res, err
	}
	a.log.Info("telegram admin: symbols removed", "symbols", res.Changed)
	// A removed symbol that auto_symbols also selected stays in the pool.
	var gone []string
	for _, s := range res.Changed {
		if !slices.Contains(auto, s) {
			gone = append(gone, s)
		}
	}
	res.Applied = a.waitApplied(ctx, nil, gone)
	return res, nil
}

// checkRuleRefs returns a readable error when a rule lists one of removed
// explicitly (validation would reject the edit with a less helpful message).
func checkRuleRefs(path string, removed []string) error {
	c, err := config.Load(path)
	if err != nil {
		return nil // let EditSymbols report the real validation error
	}
	var refs []string
	for _, r := range c.Rules {
		for _, s := range r.Symbols {
			if !config.IsSymbolGroup(s) && containsFold(removed, s) {
				refs = append(refs, fmt.Sprintf("规则 %s 引用了 %s", r.ID, strings.ToUpper(s)))
			}
		}
	}
	if len(refs) > 0 {
		return fmt.Errorf("%s，请先修改规则的 symbols", strings.Join(refs, "；"))
	}
	return nil
}

func containsFold(list []string, s string) bool {
	return slices.ContainsFunc(list, func(x string) bool { return strings.EqualFold(strings.TrimSpace(x), s) })
}

// waitApplied waits until every symbol of in is in the running pool and none
// of out is, or adminApplyWait passes.
func (a *symbolAdmin) waitApplied(ctx context.Context, in, out []string) bool {
	done := func() bool {
		pool := a.store.Current().Symbols
		for _, s := range in {
			if !slices.Contains(pool, s) {
				return false
			}
		}
		for _, s := range out {
			if slices.Contains(pool, s) {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(adminApplyWait)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if done() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}
