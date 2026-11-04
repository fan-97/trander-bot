package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/notify"
	"trader-bot/internal/universe"
)

const (
	// autoSelectTimeout bounds one auto_symbols selection (≈200 REST calls).
	autoSelectTimeout = 3 * time.Minute
	// autoRetryAfter is the wait after a failed selection.
	autoRetryAfter = 10 * time.Minute
)

// autoState is what /status reports about auto selection. Methods are safe
// for concurrent use and on a nil receiver.
type autoState struct {
	mu      sync.Mutex
	lastRun time.Time // last successful selection
	nextRun time.Time
	lastErr string
	rows    []universe.Row // selected symbols' rows, by rank
}

type autoSnapshot struct {
	LastRun, NextRun time.Time
	LastErr          string
	Rows             []universe.Row
}

func (a *autoState) snapshot() autoSnapshot {
	if a == nil {
		return autoSnapshot{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return autoSnapshot{a.lastRun, a.nextRun, a.lastErr, slices.Clone(a.rows)}
}

func (a *autoState) ran(at, next time.Time, rows []universe.Row) {
	if a == nil {
		return
	}
	var sel []universe.Row
	for _, r := range rows {
		if r.Selected {
			sel = append(sel, r)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastRun, a.nextRun, a.lastErr, a.rows = at, next, "", sel
}

func (a *autoState) failed(err error, next time.Time) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastErr, a.nextRun = err.Error(), next
}

func (a *autoState) off() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastRun, a.nextRun, a.lastErr, a.rows = time.Time{}, time.Time{}, "", nil
}

// manualSymbols returns c's manual watch pool, upper-cased and de-duplicated.
func manualSymbols(c *config.Config) []string {
	var out []string
	for _, s := range c.Symbols {
		if s = strings.ToUpper(strings.TrimSpace(s)); s != "" && !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func autoOn(c *config.Config) bool { return c.AutoSymbols != nil && c.AutoSymbols.Enabled }

// selectAuto runs one bounded selection for c.
func selectAuto(ctx context.Context, src universe.Source, c *config.Config, prev []string, now time.Time) ([]string, []universe.Row, error) {
	sctx, cancel := context.WithTimeout(ctx, autoSelectTimeout)
	defer cancel()
	return universe.Select(sctx, src, *c.AutoSymbols, manualSymbols(c), prev, now)
}

// autoLoop re-selects the auto symbols every auto_symbols.refresh, and at once
// when the auto_symbols section changes on a reload, sending each changed
// selection to out (reloadLoop merges it into the running pool). prev is the
// selection made at startup; ran reports whether that selection succeeded
// (if not, the first one runs immediately). Turning auto_symbols off sends an
// empty selection.
func autoLoop(ctx context.Context, clk clock.Clock, src universe.Source, raw *config.Store, sub <-chan *config.Config,
	prev []string, ran bool, out chan<- []string, st *autoState, alert func(notify.Message), log *slog.Logger) {
	cur := raw.Current()
	next := clk.Now()
	if ran {
		next = next.Add(cur.AutoSymbols.Refresh)
	}
	if !autoOn(cur) {
		st.off()
	}
	for {
		var wait <-chan time.Time
		if autoOn(cur) {
			wait = clk.After(max(0, next.Sub(clk.Now())))
		}
		select {
		case <-ctx.Done():
			return
		case c := <-sub:
			changed := !reflect.DeepEqual(c.AutoSymbols, cur.AutoSymbols)
			cur = c
			if !changed {
				continue
			}
			if !autoOn(cur) {
				st.off()
				if len(prev) > 0 {
					log.Info("auto_symbols disabled, removing the auto-selected symbols", "symbols", prev)
					alert(notify.Message{Title: "自动选币已关闭", Level: config.LevelInfo,
						Lines: []string{"已移除自动加入的币种: " + strings.Join(prev, ", ")}})
					prev = nil
					if !send(ctx, out, prev) {
						return
					}
				}
				continue
			}
		case <-wait:
		}

		sel, rows, err := selectAuto(ctx, src, cur, prev, clk.Now())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("auto_symbols selection failed, keeping the current selection", "err", err, "retry_in", autoRetryAfter)
			alert(notify.Message{Title: "自动选币失败", Level: config.LevelWarn,
				Lines: []string{"沿用当前自动币种，" + autoRetryAfter.String() + " 后重试", err.Error()}})
			next = clk.Now().Add(autoRetryAfter)
			st.failed(err, next)
			continue
		}
		next = clk.Now().Add(cur.AutoSymbols.Refresh)
		st.ran(clk.Now(), next, rows)
		if sameSet(sel, prev) {
			log.Info("auto_symbols selection unchanged", "symbols", sel)
			continue
		}
		log.Info("auto_symbols selection changed", "symbols", sel, "previous", prev)
		alert(autoMessage(prev, sel, rows, *cur.AutoSymbols))
		prev = sel
		if !send(ctx, out, sel) {
			return
		}
	}
}

func send(ctx context.Context, out chan<- []string, v []string) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

var rankLabels = map[config.RankBy]string{
	config.RankVolumeGrowth: "量增",
	config.RankQuoteVolume:  "日均成交额",
	config.RankAbsChange:    "波动",
	config.RankGain:         "涨幅",
}

// fmtUSDT renders a USDT amount compactly: 1.23亿, 4567万, 890.
func fmtUSDT(v float64) string {
	switch {
	case v >= 1e8:
		return fmt.Sprintf("%.2f亿", v/1e8)
	case v >= 1e4:
		return fmt.Sprintf("%.0f万", v/1e4)
	}
	return fmt.Sprintf("%.0f", v)
}

// rowSummary is e.g. "量增 ×3.40 日均 1.23亿 7天 +12.3%".
func rowSummary(r universe.Row, a config.AutoSymbols) string {
	s := ""
	if r.Growth > 0 {
		s = fmt.Sprintf("量增 ×%.2f ", r.Growth)
	}
	return s + fmt.Sprintf("日均 %s %d天 %+.1f%%", fmtUSDT(r.AvgVolume), a.Days, r.ChangePct)
}

// autoMessage announces a changed auto selection.
func autoMessage(prev, sel []string, rows []universe.Row, a config.AutoSymbols) notify.Message {
	byS := make(map[string]universe.Row, len(rows))
	for _, r := range rows {
		byS[r.Symbol] = r
	}
	var added, removed []string
	for _, s := range sel {
		if !slices.Contains(prev, s) {
			added = append(added, s)
		}
	}
	for _, s := range prev {
		if !slices.Contains(sel, s) {
			removed = append(removed, s)
		}
	}
	lines := []string{fmt.Sprintf("按%s（近 %d 天）选出 %d 个：新增 %d，移除 %d",
		rankLabels[a.RankBy], a.Days, len(sel), len(added), len(removed))}
	for _, s := range added {
		line := "+" + s
		if r, ok := byS[s]; ok {
			line += fmt.Sprintf(" #%d %s", r.Rank, rowSummary(r, a))
		}
		lines = append(lines, line)
	}
	for _, s := range removed {
		line := "-" + s
		switch r, ok := byS[s]; {
		case !ok:
			line += "（不再满足筛选条件）"
		case r.Reason != "":
			line += "（" + r.Reason + "）"
		default:
			line += fmt.Sprintf("（排名 %d，超出 keep_rank %d）", r.Rank, a.KeepRank)
		}
		lines = append(lines, line)
	}
	lines = append(lines, "当前自动币种: "+strings.Join(sel, ", "))
	return notify.Message{Title: "自动币种更新", Level: config.LevelInfo, Lines: lines}
}

// writeRank prints the ranking table for -rank.
func writeRank(w io.Writer, rows []universe.Row, a config.AutoSymbols, manual []string) {
	fmt.Fprintf(w, "rank_by=%s days=%d base_days=%d top=%d keep_rank=%d min_quote_volume=%s min_listing_days=%d\n",
		a.RankBy, a.Days, a.BaseDays, a.Top, a.KeepRank, fmtUSDT(a.MinQuoteVolume), a.MinListingDays)
	if len(manual) > 0 {
		fmt.Fprintf(w, "manual (never auto-selected): %s\n", strings.Join(manual, ", "))
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "#\tSYMBOL\tSCORE\tGROWTH\tAVG/DAY\tCHANGE\tSTATUS")
	rejected := 0
	for _, r := range rows {
		if r.Reason != "" {
			rejected++
			continue
		}
		status := ""
		switch {
		case r.Kept:
			status = "selected (kept)"
		case r.Selected:
			status = "selected"
		}
		growth := "-"
		if r.Growth > 0 {
			growth = fmt.Sprintf("×%.2f", r.Growth)
		}
		fmt.Fprintf(tw, "%d\t%s\t%.4g\t%s\t%s\t%+.1f%%\t%s\n", r.Rank, r.Symbol, r.Score, growth, fmtUSDT(r.AvgVolume), r.ChangePct, status)
	}
	_ = tw.Flush()
	if rejected > 0 {
		fmt.Fprintf(w, "\nrejected after fetching daily bars (%d):\n", rejected)
		for _, r := range rows {
			if r.Reason != "" {
				fmt.Fprintf(w, "  %s: %s\n", r.Symbol, r.Reason)
			}
		}
	}
}

// printRank loads cfgPath and prints its auto_symbols ranking (defaults when
// the section is absent) without starting the bot.
func printRank(ctx context.Context, cfgPath string, w io.Writer) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.AutoSymbols == nil {
		cfg.AutoSymbols = config.DefaultAutoSymbols()
	}
	rest := binance.NewREST(cfg.Binance.RESTURL, binance.DefaultWeightPerMin)
	_, rows, err := selectAuto(ctx, rest, cfg, nil, time.Now())
	if err != nil {
		return err
	}
	writeRank(w, rows, *cfg.AutoSymbols, manualSymbols(cfg))
	return nil
}
