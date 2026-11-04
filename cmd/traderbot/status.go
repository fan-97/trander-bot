package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/health"
	"trader-bot/internal/rule"
	"trader-bot/internal/signal"
)

// statusDoc is the /status response.
type statusDoc struct {
	Time        time.Time           `json:"time"`
	Symbols     statusSymbols       `json:"symbols"`
	AutoSymbols *statusAuto         `json:"auto_symbols"` // null: auto selection off
	Rules       []statusRule        `json:"rules"`
	Feeds       map[string]feedInfo `json:"feeds"`
}

type statusSymbols struct {
	Total  int      `json:"total"`
	Manual []string `json:"manual"`
	Auto   []string `json:"auto"`
}

type statusAuto struct {
	RankBy    config.RankBy `json:"rank_by"`
	Top       int           `json:"top"`
	Days      int           `json:"days"`
	Refresh   string        `json:"refresh"`
	LastRun   *time.Time    `json:"last_run"`
	NextRun   *time.Time    `json:"next_run"`
	LastError string        `json:"last_error,omitempty"`
	Selected  []autoPick    `json:"selected"`
}

type autoPick struct {
	Symbol    string  `json:"symbol"`
	Rank      int     `json:"rank"`
	Score     float64 `json:"score"`
	Growth    float64 `json:"volume_growth,omitempty"`
	AvgVolume float64 `json:"avg_daily_quote_volume"`
	ChangePct float64 `json:"change_pct"`
	Kept      bool    `json:"kept,omitempty"`
}

type statusRule struct {
	ID        string               `json:"id"`
	Enabled   bool                 `json:"enabled"`
	Interval  string               `json:"interval"`
	Trigger   config.Trigger       `json:"trigger"`
	Level     config.Level         `json:"level"`
	Cooldown  string               `json:"cooldown"`
	Channels  []string             `json:"channels"`
	Symbols   []string             `json:"symbols"`    // as configured
	AppliesTo []string             `json:"applies_to"` // watched symbols it runs on now
	When      string               `json:"when"`
	Setup     *statusSetup         `json:"setup,omitempty"`
	Targets   *config.Targets      `json:"targets,omitempty"`
	LastFired map[string]time.Time `json:"last_fired"` // symbol -> last delivered-or-recorded fire
}

type statusSetup struct {
	When       string `json:"when"`
	Invalidate string `json:"invalidate,omitempty"`
	Within     string `json:"within"`
}

type feedInfo struct {
	LastEvent  *time.Time `json:"last_event"`
	AgeSeconds *float64   `json:"age_seconds"`
}

func optTime(t time.Time, loc *time.Location) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.In(loc)
	return &t
}

// buildStatus assembles the document from the running config and state.
func buildStatus(cfg *config.Config, auto autoSnapshot, gate *signal.Gate, mon health.Status, now time.Time) statusDoc {
	loc := cfg.Notify.Location
	if loc == nil {
		loc = time.UTC
	}
	manual := manualOf(cfg)
	doc := statusDoc{
		Time:    now.In(loc),
		Symbols: statusSymbols{Total: len(cfg.Symbols), Manual: nonNil(manual), Auto: nonNil(slices.Clone(cfg.Auto))},
		Rules:   []statusRule{},
		Feeds:   map[string]feedInfo{},
	}
	if a := cfg.AutoSymbols; a != nil && a.Enabled {
		sa := &statusAuto{RankBy: a.RankBy, Top: a.Top, Days: a.Days, Refresh: a.Refresh.String(),
			LastRun: optTime(auto.LastRun, loc), NextRun: optTime(auto.NextRun, loc), LastError: auto.LastErr, Selected: []autoPick{}}
		for _, r := range auto.Rows {
			sa.Selected = append(sa.Selected, autoPick{Symbol: r.Symbol, Rank: r.Rank, Score: r.Score, Growth: r.Growth,
				AvgVolume: r.AvgVolume, ChangePct: r.ChangePct, Kept: r.Kept})
		}
		doc.AutoSymbols = sa
	}

	applies := map[string][]string{}
	for _, s := range cfg.Symbols {
		for _, r := range cfg.RulesFor(s) {
			applies[r.ID] = append(applies[r.ID], s)
		}
	}
	for _, r := range cfg.Rules {
		sr := statusRule{
			ID: r.ID, Enabled: !r.Disabled, Interval: string(r.Interval), Trigger: r.Trigger, Level: r.Level,
			Cooldown: r.Cooldown.String(), Channels: nonNil(r.Channels), Symbols: nonNil(r.Symbols),
			AppliesTo: nonNil(applies[r.ID]), When: rule.Format(r.When), Targets: r.Targets,
			LastFired: map[string]time.Time{},
		}
		if su := r.Setup; su != nil {
			sr.Setup = &statusSetup{When: rule.Format(su.When), Invalidate: rule.Format(su.Invalidate), Within: su.Within.String()}
		}
		for sym, t := range gate.LastFired(r.ID) {
			sr.LastFired[sym] = t.In(loc)
		}
		doc.Rules = append(doc.Rules, sr)
	}

	for _, s := range cfg.Symbols {
		fi := feedInfo{}
		if t := mon.LastEvent[s]; !t.IsZero() {
			age := now.Sub(t).Seconds()
			fi.LastEvent, fi.AgeSeconds = optTime(t, loc), &age
		}
		doc.Feeds[s] = fi
	}
	return doc
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// statusHandler serves GET /status as indented JSON.
func statusHandler(store *config.Store, auto *autoState, gate *signal.Gate, mon *health.Monitor, clk clock.Clock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		doc := buildStatus(store.Current(), auto.snapshot(), gate, mon.Snapshot(), clk.Now())
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.SetEscapeHTML(false)
		_ = enc.Encode(doc)
	})
}
