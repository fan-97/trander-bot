// Package universe picks the auto-selected watch symbols: USDT perpetuals
// ranked by recent daily-bar activity (auto_symbols in the config).
package universe

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"trader-bot/internal/config"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/market"
)

// Source is the market data Select needs; *binance.REST implements it.
type Source interface {
	Contracts(ctx context.Context) ([]binance.Contract, error)
	Tickers24h(ctx context.Context) ([]binance.Ticker24h, error)
	Klines(ctx context.Context, symbol string, iv market.Interval, start time.Time, limit int) ([]market.Kline, error)
}

// peggedBases are base assets pegged to a fiat currency or to gold: they do
// not trend, so they are never selected.
var peggedBases = map[string]bool{
	"USDC": true, "FDUSD": true, "TUSD": true, "USDP": true, "DAI": true, "BUSD": true, "USDE": true,
	"PAXG": true, "XAUT": true,
}

// Row is one ranked (or rejected) candidate.
type Row struct {
	Symbol    string
	Rank      int     // 1-based among eligible candidates; 0 if rejected
	Score     float64 // by cfg.RankBy
	AvgVolume float64 // average daily quote volume over Days (USDT)
	Growth    float64 // AvgVolume / baseline average; 0 if no baseline
	ChangePct float64 // price change over Days, %
	Selected  bool
	Kept      bool   // selected only because it was selected before (hysteresis)
	Reason    string // why rejected; "" if eligible
}

// Candidates applies the hard filters and the 24h-volume pre-filter, returning
// the symbols worth fetching daily bars for (sorted). manual symbols are never
// auto-selected, so they are skipped.
func Candidates(cs []binance.Contract, ts []binance.Ticker24h, cfg config.AutoSymbols, manual []string, now time.Time) []string {
	vol := make(map[string]float64, len(ts))
	for _, t := range ts {
		vol[t.Symbol] = t.QuoteVolume
	}
	minListed := time.Duration(cfg.MinListingDays) * 24 * time.Hour
	var out []string
	for _, c := range cs {
		switch {
		case c.UnderlyingType != "" && c.UnderlyingType != "COIN":
		case peggedBases[c.BaseAsset]:
		case slices.Contains(cfg.Exclude, c.Symbol), slices.Contains(manual, c.Symbol):
		case !c.OnboardDate.IsZero() && now.Sub(c.OnboardDate) < minListed:
		case vol[c.Symbol] < cfg.MinQuoteVolume:
		default:
			out = append(out, c.Symbol)
		}
	}
	slices.Sort(out)
	return out
}

// Rank scores the candidates' daily bars and picks cfg.Top symbols. A symbol
// in prev stays selected while it ranks within cfg.KeepRank. selected is in
// rank order; rows lists eligible candidates by rank, then rejected ones.
func Rank(daily map[string][]market.Kline, cfg config.AutoSymbols, prev []string) (selected []string, rows []Row) {
	var ok, bad []Row
	for sym, ks := range daily {
		r := score(sym, ks, cfg)
		if r.Reason != "" {
			bad = append(bad, r)
		} else {
			ok = append(ok, r)
		}
	}
	slices.SortFunc(ok, func(a, b Row) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return strings.Compare(a.Symbol, b.Symbol)
	})
	slices.SortFunc(bad, func(a, b Row) int { return strings.Compare(a.Symbol, b.Symbol) })

	for i := range ok {
		ok[i].Rank = i + 1
	}
	// Previously selected symbols still within keep_rank go first (best
	// first), then the best of the rest fill up to top.
	for i := range ok {
		if len(selected) < cfg.Top && ok[i].Rank <= cfg.KeepRank && slices.Contains(prev, ok[i].Symbol) {
			ok[i].Selected, ok[i].Kept = true, ok[i].Rank > cfg.Top
			selected = append(selected, ok[i].Symbol)
		}
	}
	for i := range ok {
		if len(selected) >= cfg.Top {
			break
		}
		if !ok[i].Selected {
			ok[i].Selected = true
			selected = append(selected, ok[i].Symbol)
		}
	}
	slices.SortFunc(selected, func(a, b string) int { return cmp.Compare(rankOf(ok, a), rankOf(ok, b)) })
	return selected, append(ok, bad...)
}

func rankOf(rows []Row, sym string) int {
	for _, r := range rows {
		if r.Symbol == sym {
			return r.Rank
		}
	}
	return math.MaxInt
}

// score computes one candidate's metrics from its closed daily bars.
func score(sym string, ks []market.Kline, cfg config.AutoSymbols) Row {
	r := Row{Symbol: sym}
	var closed []market.Kline
	for _, k := range ks {
		if k.Closed {
			closed = append(closed, k)
		}
	}
	slices.SortFunc(closed, func(a, b market.Kline) int { return a.OpenTime.Compare(b.OpenTime) })
	if len(closed) < cfg.Days {
		r.Reason = fmt.Sprintf("history: %d of %d days", len(closed), cfg.Days)
		return r
	}
	recent := closed[len(closed)-cfg.Days:]
	base := closed[max(0, len(closed)-cfg.Days-cfg.BaseDays) : len(closed)-cfg.Days]

	r.AvgVolume = avgQuote(recent)
	if first := recent[0].Open; first > 0 {
		r.ChangePct = (recent[len(recent)-1].Close/first - 1) * 100
	}
	if len(base) >= max(1, cfg.BaseDays/2) {
		if b := avgQuote(base); b > 0 {
			r.Growth = r.AvgVolume / b
		}
	}
	if r.AvgVolume < cfg.MinQuoteVolume {
		r.Reason = "liquidity: average daily volume below min_quote_volume"
		return r
	}
	switch cfg.RankBy {
	case config.RankVolumeGrowth:
		if r.Growth == 0 {
			r.Reason = fmt.Sprintf("baseline: %d of %d days", len(base), cfg.BaseDays)
			return r
		}
		r.Score = r.Growth
	case config.RankQuoteVolume:
		r.Score = r.AvgVolume
	case config.RankAbsChange:
		r.Score = math.Abs(r.ChangePct)
	case config.RankGain:
		r.Score = r.ChangePct
	}
	return r
}

func avgQuote(ks []market.Kline) float64 {
	var sum float64
	for _, k := range ks {
		sum += k.QuoteVolume
	}
	return sum / float64(len(ks))
}

// Select fetches the data and ranks it. manual symbols are excluded; prev is
// the current auto selection (for hysteresis). A failure fetching one
// symbol's bars skips that symbol; failing to list contracts or tickers is an
// error.
func Select(ctx context.Context, src Source, cfg config.AutoSymbols, manual, prev []string, now time.Time) ([]string, []Row, error) {
	cs, err := src.Contracts(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("universe: contracts: %w", err)
	}
	ts, err := src.Tickers24h(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("universe: 24h tickers: %w", err)
	}
	cands := Candidates(cs, ts, cfg, manual, now)
	// Days+BaseDays closed bars plus today's open one, which score drops.
	n := cfg.Days + cfg.BaseDays + 1
	start := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -(n - 1))
	daily := make(map[string][]market.Kline, len(cands))
	var failed int
	for _, sym := range cands {
		ks, err := src.Klines(ctx, sym, market.I1d, start, n)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			failed++
			continue
		}
		daily[sym] = ks
	}
	if len(cands) > 0 && failed == len(cands) {
		return nil, nil, fmt.Errorf("universe: daily bars failed for all %d candidates", failed)
	}
	sel, rows := Rank(daily, cfg, prev)
	return sel, rows, nil
}
