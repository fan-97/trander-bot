package config

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"trader-bot/internal/market"

	_ "time/tzdata" // QuietHours TZ validation must work in distroless images
)

var channelTypes = map[string]bool{
	"telegram": true, "discord": true, "webhook": true, "email": true,
	"wecom": true, "dingtalk": true, "feishu": true,
}

// Validate checks the whole config and returns every problem found, joined.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if len(c.Symbols) == 0 {
		add("symbols: watch pool is empty")
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
		if r.Trigger != TriggerOnClose && r.Trigger != TriggerRealtime {
			add("rule %q: unknown trigger %q", r.ID, r.Trigger)
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
			if s != AllSymbols && !pool[s] {
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
