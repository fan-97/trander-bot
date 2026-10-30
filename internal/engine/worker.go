// Package engine runs rule evaluation: one Worker per symbol owns all of that
// symbol's state, and the Router fans market events and config out to them.
package engine

import (
	"cmp"
	"context"
	"log/slog"
	"reflect"
	"slices"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/config"
	"trader-bot/internal/indicator"
	"trader-bot/internal/market"
	"trader-bot/internal/rule"
	"trader-bot/internal/signal"
)

// maxBars is how many closed bars per interval a worker keeps for replay.
const maxBars = 500

// newIndicator is indicator.New; tests wrap it to count constructions.
var newIndicator = indicator.New

// ruleMeta is precomputed per rule.
type ruleMeta struct {
	contractOnly bool   // When references only contract operands
	srcs         source // contract sources the rule reads
}

// Worker evaluates every rule for one symbol. All state is owned by the
// goroutine calling Warmup and then Run; it is not safe for concurrent use.
type Worker struct {
	symbol string
	gate   *signal.Gate
	out    chan<- signal.Signal
	clk    clock.Clock
	log    *slog.Logger

	rules   []config.Rule
	meta    []ruleMeta
	targets []market.Interval // aggregated intervals, shortest first

	aggs       map[market.Interval]*market.Aggregator
	inds       map[string]indicator.Indicator // indKey(interval, operand) -> shared instance
	ivInds     map[market.Interval][]indicator.Indicator
	bars       map[market.Interval][]market.Kline // last maxBars closed bars
	lastClosed map[market.Interval]time.Time      // newest closed OpenTime processed
	lastPrice  float64

	contract contract
	started  bool // Run has begun: liq_volume reads 0 instead of unavailable
}

// NewWorker creates a worker for symbol using the rules cfg applies to it.
func NewWorker(symbol string, cfg *config.Config, gate *signal.Gate, out chan<- signal.Signal, clk clock.Clock) *Worker {
	w := &Worker{
		symbol:     symbol,
		gate:       gate,
		out:        out,
		clk:        clk,
		log:        slog.Default().With("symbol", symbol),
		aggs:       map[market.Interval]*market.Aggregator{},
		inds:       map[string]indicator.Indicator{},
		bars:       map[market.Interval][]market.Kline{},
		lastClosed: map[market.Interval]time.Time{},
	}
	w.setConfig(cfg)
	return w
}

// Warmup feeds historical closed bars (per interval, oldest first) into the
// indicators and the replay buffer without evaluating rules. 1m bars newer
// than an interval's last closed bar are also fed to that interval's
// aggregator, so a bucket already in progress is complete when it closes.
func (w *Worker) Warmup(history map[market.Interval][]market.Kline) {
	ivs := make([]market.Interval, 0, len(history))
	for iv := range history {
		ivs = append(ivs, iv)
	}
	slices.SortFunc(ivs, func(a, b market.Interval) int { return cmp.Compare(a.Duration(), b.Duration()) })
	for _, iv := range ivs {
		for _, k := range sortedClosed(history[iv]) {
			if k.Interval == iv {
				_ = w.onBar(context.Background(), k, false)
			}
		}
	}
	ones := sortedClosed(history[market.I1m])
	for _, iv := range w.targets {
		last, ok := w.lastClosed[iv]
		if iv == market.I1m || !ok || w.aggs[iv] == nil {
			continue
		}
		next := last.Add(iv.Duration())
		for _, k := range ones {
			if k.Interval != market.I1m || k.OpenTime.Before(next) {
				continue
			}
			for _, b := range w.aggs[iv].Add(k) {
				_ = w.onBar(context.Background(), b, false)
			}
		}
	}
	if n := len(ones); n > 0 {
		w.lastPrice = ones[n-1].Close
	}
}

func sortedClosed(ks []market.Kline) []market.Kline {
	out := make([]market.Kline, 0, len(ks))
	for _, k := range ks {
		if k.Closed {
			out = append(out, k)
		}
	}
	slices.SortStableFunc(out, func(a, b market.Kline) int { return a.OpenTime.Compare(b.OpenTime) })
	return out
}

// Run processes events and config snapshots until ctx is cancelled or in is
// closed (both return nil). Signal sends honour ctx.
func (w *Worker) Run(ctx context.Context, in <-chan market.Event, cfgs <-chan *config.Config) error {
	w.started = true
	for {
		select {
		case <-ctx.Done():
			return nil
		case c, ok := <-cfgs:
			if !ok {
				cfgs = nil
				continue
			}
			if c != nil {
				w.reconfigure(c)
			}
		case ev, ok := <-in:
			if !ok {
				return nil
			}
			if err := w.handle(ctx, ev); err != nil {
				return nil // ctx cancelled while sending
			}
		}
	}
}

// handle processes one event; it returns an error only when ctx ended.
func (w *Worker) handle(ctx context.Context, ev market.Event) error {
	if ev.EventSymbol() != w.symbol {
		return nil
	}
	if k, ok := ev.(market.KlineEvent); ok {
		if k.Interval != market.I1m {
			return nil // the stream delivers 1m; higher intervals are aggregated here
		}
		w.lastPrice = k.Close
		for _, iv := range w.targets {
			for _, b := range w.aggs[iv].Add(k.Kline) {
				if err := w.onBar(ctx, b, true); err != nil {
					return err
				}
			}
		}
		return nil
	}
	src := w.contract.apply(ev, w.clk.Now())
	if src == 0 {
		return nil
	}
	price := w.lastPrice
	if price == 0 {
		if m, _, ok := w.contract.mark.get(); ok {
			price = m
		}
	}
	for i, r := range w.rules {
		if m := w.meta[i]; m.contractOnly && m.srcs&src != 0 {
			if err := w.evaluate(ctx, r, w.values(r.Interval, nil), price); err != nil {
				return err
			}
		}
	}
	return nil
}

// onBar handles an aggregated bar of one target interval.
func (w *Worker) onBar(ctx context.Context, k market.Kline, eval bool) error {
	iv := k.Interval
	if w.aggs[iv] == nil {
		return nil // not a tracked interval
	}
	if last, ok := w.lastClosed[iv]; ok && !k.OpenTime.After(last) {
		return nil // duplicate or stale bar
	}
	if !k.Closed {
		if !eval {
			return nil
		}
		live := k
		for _, r := range w.rules {
			if r.Interval == iv && r.Trigger == config.TriggerRealtime {
				if err := w.evaluate(ctx, r, w.values(iv, &live), k.Close); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, ind := range w.ivInds[iv] {
		ind.Update(k)
	}
	w.retain(k)
	w.lastClosed[iv] = k.OpenTime
	if !eval {
		return nil
	}
	v := w.values(iv, nil)
	for _, r := range w.rules {
		if r.Interval == iv {
			if err := w.evaluate(ctx, r, v, k.Close); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) retain(k market.Kline) {
	bs := w.bars[k.Interval]
	if len(bs) >= maxBars {
		bs = append(bs[:0], bs[len(bs)-maxBars+1:]...)
	}
	w.bars[k.Interval] = append(bs, k)
}

// evaluate runs r through the gate and emits a Signal on a fire or a
// cooldown-suppressed edge (R2).
func (w *Worker) evaluate(ctx context.Context, r config.Rule, v rule.Values, price float64) error {
	if r.When == nil {
		return nil
	}
	res := rule.Eval(r.When, v)
	fire, suppressed := w.gate.Observe(r.ID, w.symbol, res, r.Cooldown)
	if !fire && suppressed != "cooldown" {
		return nil
	}
	detail := rule.Describe(r.When, v)
	if detail == "" {
		detail = r.ID
	}
	s := signal.Signal{
		RuleID:     r.ID,
		Symbol:     w.symbol,
		Interval:   r.Interval,
		Level:      r.Level,
		Time:       w.clk.Now(),
		Price:      price,
		Detail:     detail,
		Channels:   slices.Clone(r.Channels),
		Suppressed: suppressed,
	}
	select {
	case w.out <- s:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reconfigure applies a new config: gate state is dropped for this symbol's
// rules that were removed or whose When/Interval changed, then indicators are
// rebuilt (by replaying retained bars). Contract state is kept.
func (w *Worker) reconfigure(cfg *config.Config) {
	next := map[string]config.Rule{}
	for _, r := range cfg.RulesFor(w.symbol) {
		next[r.ID] = r
	}
	for _, old := range w.rules {
		n, ok := next[old.ID]
		if !ok || n.Interval != old.Interval || !reflect.DeepEqual(n.When, old.When) {
			w.gate.ForgetRuleSymbol(old.ID, w.symbol)
		}
	}
	w.setConfig(cfg)
}

func (w *Worker) setConfig(cfg *config.Config) {
	w.rules = cfg.RulesFor(w.symbol)
	w.meta = make([]ruleMeta, len(w.rules))

	// Aggregated intervals: the configured ones plus every rule's interval.
	set := map[market.Interval]bool{}
	for _, iv := range cfg.Intervals {
		if iv.Valid() {
			set[iv] = true
		}
	}
	oiNeed, liqNeed := 0, time.Duration(0)
	for i, r := range w.rules {
		if r.Interval.Valid() {
			set[r.Interval] = true
		}
		var fs []rule.Operand
		if r.When != nil {
			fs = rule.Funcs(r.When)
		}
		m := ruleMeta{contractOnly: len(fs) > 0}
		for _, o := range fs {
			if !o.IsContract() {
				m.contractOnly = false
				continue
			}
			m.srcs |= contractSource[o.Func]
			switch o.Func {
			case "oi_change_pct":
				if n, ok := posInt(o.Args); ok {
					oiNeed = max(oiNeed, n+2)
				}
			case "liq_volume":
				if len(o.Args) == 1 && o.Args[0] > 0 && o.Args[0] < 1e6 {
					liqNeed = max(liqNeed, time.Duration(o.Args[0]*minuteNS))
				}
			}
		}
		w.meta[i] = m
	}
	w.contract.size(oiNeed, liqNeed)

	targets := make([]market.Interval, 0, len(set))
	for iv := range set {
		targets = append(targets, iv)
	}
	slices.SortFunc(targets, func(a, b market.Interval) int { return cmp.Compare(a.Duration(), b.Duration()) })
	for iv := range w.aggs {
		if !set[iv] {
			delete(w.aggs, iv)
			delete(w.bars, iv)
			delete(w.lastClosed, iv)
		}
	}
	for _, iv := range targets {
		if w.aggs[iv] == nil {
			w.aggs[iv] = market.NewAggregator(w.symbol, []market.Interval{iv})
		}
	}
	w.targets = targets

	// Indicators: one instance per key, shared by every rule using it.
	// Unchanged keys keep their instance; new ones replay the retained bars.
	inds := map[string]indicator.Indicator{}
	ivInds := map[market.Interval][]indicator.Indicator{}
	for _, r := range w.rules {
		if r.When == nil || !r.Interval.Valid() {
			continue
		}
		for _, o := range rule.Funcs(r.When) {
			if o.IsContract() {
				continue
			}
			key := indKey(r.Interval, o)
			if _, dup := inds[key]; dup {
				continue
			}
			ind, ok := w.inds[key]
			if !ok {
				var err error
				ind, err = newIndicator(o.Func, o.Args)
				if err != nil {
					w.log.Error("engine: cannot build indicator; rule stays unknown", "rule", r.ID, "operand", o.String(), "err", err)
					continue
				}
				for _, b := range w.bars[r.Interval] {
					ind.Update(b)
				}
			}
			inds[key] = ind
			ivInds[r.Interval] = append(ivInds[r.Interval], ind)
		}
	}
	w.inds, w.ivInds = inds, ivInds
}
