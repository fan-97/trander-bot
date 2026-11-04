package config

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"trader-bot/internal/market"
	"trader-bot/internal/rule"

	_ "time/tzdata" // QuietHours TZ validation must work in distroless images
)

var channelTypes = map[string]bool{
	"telegram": true, "discord": true, "webhook": true, "email": true,
	"wecom": true, "dingtalk": true, "feishu": true,
}

// realtimeFuncs are the kline operands a realtime rule may use: they have a
// meaningful provisional value on an unclosed bar.
// Wick shapes are excluded: they are only meaningful once the bar has closed.
var realtimeFuncs = map[string]bool{
	"close": true, "change_pct": true, "volume": true,
	"open": true, "high": true, "low": true, "amplitude_pct": true, "volume_ratio": true,
	"highest": true, "lowest": true, // exclude the current bar: fixed while it is open
}

// Validate checks the whole config and returns every problem found, joined.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	autoOn := c.AutoSymbols != nil && c.AutoSymbols.Enabled
	if len(c.Symbols) == 0 && !autoOn {
		add("symbols: watch pool is empty (list symbols or enable auto_symbols)")
	}
	if a := c.AutoSymbols; a != nil {
		if a.Top < 1 || a.Top > 200 {
			add("auto_symbols.top must be 1..200, got %d", a.Top)
		}
		switch a.RankBy {
		case RankVolumeGrowth, RankQuoteVolume, RankAbsChange, RankGain:
		default:
			add("auto_symbols.rank_by: unknown %q (want volume_growth, quote_volume, abs_change or gain)", a.RankBy)
		}
		if a.Days < 1 || a.Days > 60 {
			add("auto_symbols.days must be 1..60, got %d", a.Days)
		}
		if a.BaseDays < 1 || a.BaseDays > 60 {
			add("auto_symbols.base_days must be 1..60, got %d", a.BaseDays)
		}
		if a.KeepRank < a.Top {
			add("auto_symbols.keep_rank (%d) must be >= top (%d)", a.KeepRank, a.Top)
		}
		if a.MinQuoteVolume < 0 || math.IsNaN(a.MinQuoteVolume) {
			add("auto_symbols.min_quote_volume must be >= 0")
		}
		if a.MinListingDays < 0 {
			add("auto_symbols.min_listing_days must be >= 0")
		}
		if a.Refresh < 10*time.Minute {
			add("auto_symbols.refresh must be at least 10m, got %v", a.Refresh)
		}
	}
	pool := map[string]bool{}
	for _, s := range c.Symbols {
		pool[s] = true
	}
	hasInterval := map[market.Interval]bool{}
	for _, i := range c.Intervals {
		hasInterval[i] = true
		if !i.Valid() {
			add("intervals: unknown interval %q", i)
		}
	}

	names := make([]string, 0, len(c.Channels))
	for name := range c.Channels {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		ch := c.Channels[name]
		if !channelTypes[ch.Type] {
			add("channels.%s: unknown type %q", name, ch.Type)
		}
		if ch.Fallback != "" {
			if _, ok := c.Channels[ch.Fallback]; !ok {
				add("channels.%s: unknown fallback channel %q", name, ch.Fallback)
			}
		}
	}

	ids := map[string]bool{}
	for i, r := range c.Rules {
		if r.ID == "" {
			add("rules[%d]: missing id", i)
		} else if ids[r.ID] {
			add("rules[%d]: duplicate rule id %q", i, r.ID)
		}
		ids[r.ID] = true

		if !r.Interval.Valid() {
			add("rule %q: unknown interval %q", r.ID, r.Interval)
		} else if c.explicitIntervals && !hasInterval[r.Interval] {
			add("rule %q: interval %q not in intervals list", r.ID, r.Interval)
		}
		if c.explicitIntervals {
			for _, iv := range rule.Intervals(r.Conditions()...) {
				if !hasInterval[iv] {
					add("rule %q: interval %q (referenced with @) not in intervals list", r.ID, iv)
				}
			}
		}
		if t := r.Targets; t != nil {
			if t.Side != SideLong && t.Side != SideShort {
				add("rule %q: targets.side must be long or short, got %q", r.ID, t.Side)
			}
			if t.ATR < 1 || t.ATR > 1e6 {
				add("rule %q: targets.atr must be a positive period, got %d", r.ID, t.ATR)
			}
			if !t.Interval.Valid() {
				add("rule %q: targets: unknown interval %q", r.ID, t.Interval)
			} else if c.explicitIntervals && !hasInterval[t.Interval] {
				add("rule %q: targets.interval %q not in intervals list", r.ID, t.Interval)
			}
			if t.Stop < 0 || math.IsNaN(t.Stop) || math.IsInf(t.Stop, 0) {
				add("rule %q: targets.stop must be a non-negative ATR multiple", r.ID)
			}
			for _, k := range t.Take {
				if !(k > 0) || math.IsInf(k, 0) {
					add("rule %q: targets.take values must be positive ATR multiples, got %v", r.ID, k)
				}
			}
			if t.Stop == 0 && len(t.Take) == 0 {
				add("rule %q: targets needs stop or take", r.ID)
			}
		}
		if r.Trigger != TriggerOnClose && r.Trigger != TriggerRealtime {
			add("rule %q: unknown trigger %q", r.ID, r.Trigger)
		}
		if r.Trigger == TriggerRealtime {
			for _, o := range rule.Funcs(r.Conditions()...) {
				// "@" operands read a closed bar, so any indicator works.
				if !o.IsContract() && o.Interval == "" && !realtimeFuncs[o.Func] {
					add("rule %q: %s is not supported with trigger realtime (only close, open, high, low, change_pct, amplitude_pct, volume, volume_ratio, highest, lowest, @ operands and contract data)", r.ID, o)
				}
			}
		}
		if r.Level.Rank() < 0 {
			add("rule %q: unknown level %q", r.ID, r.Level)
		}
		if r.Cooldown < 0 {
			add("rule %q: negative cooldown", r.ID)
		}
		if len(r.Channels) == 0 {
			add("rule %q: no channels", r.ID)
		}
		for _, ch := range r.Channels {
			if _, ok := c.Channels[ch]; !ok {
				add("rule %q: unknown channel %q", r.ID, ch)
			}
		}
		for _, s := range r.Symbols {
			if s == AutoSymbolsGroup && !autoOn {
				add("rule %q: symbols \"auto\" needs auto_symbols enabled", r.ID)
			} else if !IsSymbolGroup(s) && !pool[s] {
				add("rule %q: symbol %q not in watch pool", r.ID, s)
			}
		}
	}

	if c.Notify.QuietMinLevel.Rank() < 0 {
		add("notify.quiet_min_level: unknown level %q", c.Notify.QuietMinLevel)
	}
	if c.Notify.MergeWindow < 0 {
		add("notify.merge_window: negative")
	}
	if c.Notify.RatePerMinute <= 0 {
		add("notify.rate_per_minute: must be positive")
	}
	if q := c.Notify.QuietHours; q != nil {
		for _, t := range []string{q.Start, q.End} {
			if _, err := time.Parse("15:04", t); err != nil {
				add("notify.quiet_hours: invalid time %q (want HH:MM)", t)
			}
		}
		if _, err := time.LoadLocation(q.TZ); err != nil || q.TZ == "" {
			add("notify.quiet_hours: invalid tz %q", q.TZ)
		}
	}
	return errors.Join(errs...)
}
