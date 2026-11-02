package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"trader-bot/internal/market"
	"trader-bot/internal/rule"
)

const (
	defaultMergeWindow   = 10 * time.Second
	defaultRatePerMinute = 20
	defaultNotifyTZ      = "Asia/Shanghai" // message times; tzdata is embedded (validate.go)
	defaultRESTURL       = "https://fapi.binance.com"
	defaultWSURL         = "wss://fstream.binance.com/market"
)

type rawConfig struct {
	Symbols   []string              `yaml:"symbols"`
	Intervals []string              `yaml:"intervals"`
	Rules     []rawRule             `yaml:"rules"`
	Channels  map[string]rawChannel `yaml:"channels"`
	Notify    rawNotify             `yaml:"notify"`
	Database  struct {
		DSN string `yaml:"dsn"`
	} `yaml:"database"`
	Binance struct {
		RESTURL string `yaml:"rest_url"`
		WSURL   string `yaml:"ws_url"`
	} `yaml:"binance"`
}

type rawRule struct {
	ID       string     `yaml:"id"`
	Symbols  stringList `yaml:"symbols"`
	Interval string     `yaml:"interval"`
	Trigger  string     `yaml:"trigger"`
	When     yaml.Node  `yaml:"when"`
	Cooldown string     `yaml:"cooldown"`
	Channels []string   `yaml:"channels"`
	Level    string     `yaml:"level"`
}

type rawChannel struct {
	Type     string            `yaml:"type"`
	Fallback string            `yaml:"fallback"`
	Params   map[string]string `yaml:"params"`
}

type rawNotify struct {
	MergeWindow   string `yaml:"merge_window"`
	RatePerMinute int    `yaml:"rate_per_minute"`
	QuietHours    *struct {
		Start string `yaml:"start"`
		End   string `yaml:"end"`
		TZ    string `yaml:"tz"`
	} `yaml:"quiet_hours"`
	QuietMinLevel string `yaml:"quiet_min_level"`
	TZ            string `yaml:"tz"`
}

// stringList accepts either a scalar ("all") or a list of strings.
type stringList []string

func (s *stringList) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		*s = stringList{n.Value}
		return nil
	case yaml.SequenceNode:
		var l []string
		if err := n.Decode(&l); err != nil {
			return err
		}
		*s = l
		return nil
	}
	return fmt.Errorf("line %d: expected string or list of strings", n.Line)
}

// Load reads the YAML file at path, applies defaults, expands ${ENV}
// references and validates the result.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse is Load for in-memory YAML.
func Parse(data []byte) (*Config, error) {
	var raw rawConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c, buildErr := raw.build()
	if err := errors.Join(buildErr, c.Validate()); err != nil {
		return nil, err
	}
	return c, nil
}

// expandEnv expands ${VAR} / $VAR in s and appends the names of variables
// that are not set to missing (a set-but-empty variable is fine).
func expandEnv(s string, missing *[]string) string {
	return os.Expand(s, func(name string) string {
		v, ok := os.LookupEnv(name)
		if !ok && !slices.Contains(*missing, name) {
			*missing = append(*missing, name)
		}
		return v
	})
}

func (r *rawConfig) build() (*Config, error) {
	var missing []string
	c := &Config{
		Symbols:  r.Symbols,
		Channels: map[string]ChannelConfig{},
		Database: DatabaseConfig{DSN: expandEnv(r.Database.DSN, &missing)},
		Binance:  BinanceConfig{RESTURL: r.Binance.RESTURL, WSURL: r.Binance.WSURL},
	}
	if c.Binance.RESTURL == "" {
		c.Binance.RESTURL = defaultRESTURL
	}
	if c.Binance.WSURL == "" {
		c.Binance.WSURL = defaultWSURL
	}

	for name, ch := range r.Channels {
		params := make(map[string]string, len(ch.Params))
		for k, v := range ch.Params {
			params[k] = expandEnv(v, &missing)
		}
		c.Channels[name] = ChannelConfig{Type: ch.Type, Fallback: ch.Fallback, Params: params}
	}

	var errs []error
	if len(missing) > 0 {
		sort.Strings(missing)
		errs = append(errs, fmt.Errorf("environment variables not set: %s", strings.Join(missing, ", ")))
	}
	for i, rr := range r.Rules {
		r2 := Rule{
			ID:       rr.ID,
			Symbols:  []string(rr.Symbols),
			Interval: market.Interval(rr.Interval),
			Trigger:  Trigger(rr.Trigger),
			Channels: rr.Channels,
			Level:    Level(rr.Level),
		}
		if len(r2.Symbols) == 0 {
			r2.Symbols = []string{AllSymbols}
		}
		if r2.Trigger == "" {
			r2.Trigger = TriggerOnClose
		}
		if r2.Level == "" {
			r2.Level = LevelWarn
		}
		if rr.Cooldown != "" {
			d, err := time.ParseDuration(rr.Cooldown)
			if err != nil {
				errs = append(errs, fmt.Errorf("rules[%d] %q: invalid cooldown %q: %w", i, rr.ID, rr.Cooldown, err))
			}
			r2.Cooldown = d
		} else {
			r2.Cooldown = defaultCooldown(r2.Interval)
		}
		if w := &rr.When; w.IsZero() || w.Tag == "!!null" || (w.Kind == yaml.ScalarNode && w.Value == "") {
			errs = append(errs, fmt.Errorf("rule %q: missing when", rr.ID))
		} else if n, err := rule.Parse(w); err != nil {
			errs = append(errs, fmt.Errorf("rule %q: when: %w", rr.ID, err))
		} else {
			r2.When = n
		}
		c.Rules = append(c.Rules, r2)
	}

	if len(r.Intervals) > 0 {
		seen := map[market.Interval]bool{}
		for _, s := range r.Intervals {
			i := market.Interval(s)
			if !seen[i] {
				seen[i] = true
				c.Intervals = append(c.Intervals, i)
			}
		}
		c.explicitIntervals = true
	} else {
		seen := map[market.Interval]bool{}
		for _, rule := range c.Rules {
			if rule.Interval.Valid() && !seen[rule.Interval] {
				seen[rule.Interval] = true
				c.Intervals = append(c.Intervals, rule.Interval)
			}
		}
	}

	c.Notify = NotifyConfig{
		MergeWindow:   defaultMergeWindow,
		RatePerMinute: defaultRatePerMinute,
		QuietMinLevel: Level(r.Notify.QuietMinLevel),
	}
	if r.Notify.MergeWindow != "" {
		d, err := time.ParseDuration(r.Notify.MergeWindow)
		if err != nil {
			errs = append(errs, fmt.Errorf("notify.merge_window: invalid duration %q: %w", r.Notify.MergeWindow, err))
		} else {
			c.Notify.MergeWindow = d
		}
	}
	if r.Notify.RatePerMinute != 0 {
		c.Notify.RatePerMinute = r.Notify.RatePerMinute
	}
	if c.Notify.QuietMinLevel == "" {
		c.Notify.QuietMinLevel = LevelCritical
	}
	if q := r.Notify.QuietHours; q != nil {
		c.Notify.QuietHours = &QuietHours{Start: q.Start, End: q.End, TZ: q.TZ}
	}
	c.Notify.TZ = r.Notify.TZ
	if c.Notify.TZ == "" {
		c.Notify.TZ = defaultNotifyTZ
	}
	if loc, err := time.LoadLocation(c.Notify.TZ); err != nil {
		errs = append(errs, fmt.Errorf("notify.tz: unknown time zone %q", c.Notify.TZ))
		c.Notify.Location = time.UTC
	} else {
		c.Notify.Location = loc
	}

	return c, errors.Join(errs...)
}

// minDefaultCooldown is the floor for a rule that omits cooldown, so a
// condition hovering around its threshold cannot re-fire on every crossing.
const minDefaultCooldown = 15 * time.Minute

// defaultCooldown is max(interval, 15m) (R21). Set cooldown: 0s explicitly
// to disable it.
func defaultCooldown(iv market.Interval) time.Duration {
	return max(iv.Duration(), minDefaultCooldown)
}

// RulesFor returns the rules that apply to symbol.
func (c *Config) RulesFor(symbol string) []Rule {
	var out []Rule
	for _, r := range c.Rules {
		for _, s := range r.Symbols {
			if s == AllSymbols || s == symbol {
				out = append(out, r)
				break
			}
		}
	}
	return out
}
