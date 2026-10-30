// Package config loads and validates the YAML configuration.
package config

import (
	"time"

	"gopkg.in/yaml.v3"

	"trader-bot/internal/market"
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

// AllSymbols in a rule's Symbols means every symbol in the watch pool.
const AllSymbols = "all"

type Config struct {
	Symbols   []string
	Intervals []market.Interval
	Rules     []Rule
	Channels  map[string]ChannelConfig
	Notify    NotifyConfig
	Database  DatabaseConfig
	Binance   BinanceConfig
}

type Rule struct {
	ID       string
	Symbols  []string // ["all"] means every watched symbol
	Interval market.Interval
	Trigger  Trigger
	When     yaml.Node // Task 5 replaces this with a parsed rule.Node
	Cooldown time.Duration
	Channels []string
	Level    Level
}

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
