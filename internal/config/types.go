// Package config loads and validates the YAML configuration.
package config

import (
	"time"

	"trader-bot/internal/market"
	"trader-bot/internal/rule"
)

type Level string

const (
	LevelInfo     Level = "info"
	LevelWarn     Level = "warn"
	LevelCritical Level = "critical"
)

// Rank orders levels; unknown levels rank -1.
func (l Level) Rank() int {
	switch l {
	case LevelInfo:
		return 0
	case LevelWarn:
		return 1
	case LevelCritical:
		return 2
	}
	return -1
}

type Trigger string

const (
	TriggerOnClose  Trigger = "on_close"
	TriggerRealtime Trigger = "realtime"
)

// Group keywords in a rule's Symbols: AllSymbols is every symbol in the watch
// pool, ManualSymbols those listed under symbols, AutoSymbolsGroup those added
// by auto_symbols.
const (
	AllSymbols       = "all"
	ManualSymbols    = "manual"
	AutoSymbolsGroup = "auto"
)

// IsSymbolGroup reports whether s is a group keyword rather than a symbol.
func IsSymbolGroup(s string) bool {
	return s == AllSymbols || s == ManualSymbols || s == AutoSymbolsGroup
}

type Config struct {
	// Symbols is the watch pool. As loaded it holds the manual symbols; the
	// running config also includes the auto-selected ones, listed in Auto.
	Symbols     []string
	Auto        []string     // auto-selected members of Symbols (runtime only)
	AutoSymbols *AutoSymbols // nil: auto selection off
	Intervals   []market.Interval
	Rules       []Rule
	Channels    map[string]ChannelConfig
	Notify      NotifyConfig
	Database    DatabaseConfig
	Binance     BinanceConfig

	explicitIntervals bool // Intervals came from YAML, not derived from rules
}

type Rule struct {
	ID       string
	Symbols  []string // ["all"] means every watched symbol
	Interval market.Interval
	Trigger  Trigger
	When     rule.Node // parsed condition tree; nil when missing
	Cooldown time.Duration
	Channels []string
	Level    Level
	Targets  *Targets // ATR stop/take-profit reference shown in messages; nil: none
	Setup    *Setup   // precondition that must hold first; nil: When alone fires
}

// Setup makes a rule two-stage. Setup.When becoming True arms the rule; it
// stays armed until Within has passed since Setup.When was last True, until
// Invalidate is True, or until it fires. The rule fires only when its When
// goes False->True while armed, so the trigger must follow the setup; one
// arming yields at most one signal.
type Setup struct {
	When       rule.Node
	Invalidate rule.Node // optional
	Within     time.Duration
}

// Conditions returns the rule's non-nil condition trees: When, then the
// setup's When and Invalidate.
func (r Rule) Conditions() []rule.Node {
	var out []rule.Node
	if r.When != nil {
		out = append(out, r.When)
	}
	if s := r.Setup; s != nil {
		if s.When != nil {
			out = append(out, s.When)
		}
		if s.Invalidate != nil {
			out = append(out, s.Invalidate)
		}
	}
	return out
}

type Side string

const (
	SideLong  Side = "long"
	SideShort Side = "short"
)

// Targets derives reference stop and take-profit prices from the signal price
// and ATR: long stop = price - Stop*ATR, take = price + k*ATR (short mirrored).
// They are informational only and never affect whether a rule fires.
type Targets struct {
	Side     Side
	ATR      int             // ATR period
	Interval market.Interval // interval the ATR is computed on
	Stop     float64         // ATR multiple; 0: no stop
	Take     []float64       // ATR multiples
}

// AutoSymbols periodically adds the top-ranked USDT perpetuals to the watch
// pool, alongside (never replacing) the manual symbols.
type AutoSymbols struct {
	Enabled        bool
	Top            int
	RankBy         RankBy
	Days           int     // ranking window, in closed daily bars
	BaseDays       int     // volume_growth baseline window before Days
	MinQuoteVolume float64 // minimum average daily quote volume (USDT) over Days
	MinListingDays int
	KeepRank       int // an already-selected symbol stays while ranked within this
	Refresh        time.Duration
	Exclude        []string // upper-cased symbols never selected
}

type RankBy string

const (
	RankVolumeGrowth RankBy = "volume_growth"
	RankQuoteVolume  RankBy = "quote_volume"
	RankAbsChange    RankBy = "abs_change"
	RankGain         RankBy = "gain"
)

type ChannelConfig struct {
	Type     string // telegram|discord|webhook|email|wecom|dingtalk|feishu
	Fallback string
	Params   map[string]string
}

type NotifyConfig struct {
	MergeWindow   time.Duration
	RatePerMinute int
	QuietHours    *QuietHours
	QuietMinLevel Level
	// TZ names the time zone message times are shown in; Location is it
	// loaded (never nil after Parse).
	TZ       string
	Location *time.Location
}

type QuietHours struct {
	Start string // "23:00"
	End   string // "07:00"
	TZ    string // e.g. "Asia/Shanghai"
}

type DatabaseConfig struct {
	DSN string
}

type BinanceConfig struct {
	RESTURL string
	WSURL   string
}
