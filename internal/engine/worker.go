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

	// staleCheck marks signals from closed bars that arrive long after they
	// closed (gap fill after an outage) as stale: recorded, never delivered.
	staleCheck bool

	// history, if set, warms intervals gained on a reload (I3). cold holds
	// those intervals until their history is applied; while an interval is
	// cold its first closed bucket started mid-way and is dropped instead of
	// reaching the indicators. histCh carries fetched history back to Run.
	history  HistoryFunc
	cold     map[market.Interval]bool
	histCh   chan histResult
	fetching bool
}

type histResult struct {
	ivs  []market.Interval
	hist map[market.Interval][]market.Kline
	err  error
}

// minStaleLag is the floor of staleLag.
const minStaleLag = 2 * time.Minute

// staleLag is how long after its close a bar may arrive and still count as
// live: max(2 intervals, 2m).
func staleLag(iv market.Interval) time.Duration {
	return max(2*iv.Duration(), minStaleLag)
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
		staleCheck: true,
		cold:       map[market.Interval]bool{},
		histCh:     make(chan histResult, 1),
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
	w.primeGate(nil)
}

// primeGate evaluates the rules of ivs (nil: every rule) once on the
// warmed-up state and seeds the gate with the result without emitting, so
// conditions already true before a (re)start are not announced again (R23).
// Rules whose data is not ready evaluate Unknown and are left unprimed.
func (w *Worker) primeGate(ivs []market.Interval) {
	for _, r := range w.rules {
		if r.When == nil || w.aggs[r.Interval] == nil {
			continue
		}
		if ivs != nil && !slices.Contains(ivs, r.Interval) {
			continue
		}
		if _, ok := w.lastClosed[r.Interval]; !ok {
			continue
		}
		w.gate.Prime(r.ID, w.symbol, rule.Eval(r.When, w.values(r.Interval, nil)))
	}
}

// fetchCold fetches history for the intervals gained on a reload, off the
// event loop; Run applies the result. At most one fetch is in flight.
func (w *Worker) fetchCold(ctx context.Context) {
	if w.history == nil || w.fetching || len(w.cold) == 0 {
		return
	}
	ivs := make([]market.Interval, 0, len(w.cold))
	for iv := range w.cold {
		ivs = append(ivs, iv)
	}
	slices.SortFunc(ivs, func(a, b market.Interval) int { return cmp.Compare(a.Duration(), b.Duration()) })
	w.fetching = true
	go func() {
		hist, err := w.history(ctx, w.symbol)
		select {
		case w.histCh <- histResult{ivs: ivs, hist: hist, err: err}:
		case <-ctx.Done():
		}
	}()
}

// applyHistory warms the reloaded intervals in h like Warmup does. An
// interval stays cold (its first, partial bucket is dropped) when the fetch
// failed or no 1m bars could complete its in-progress bucket.
func (w *Worker) applyHistory(ctx context.Context, h histResult) {
	w.fetching = false
	if h.err != nil {
		// The history func reports the failure itself (预热失败 alert).
		w.log.Warn("engine: history for reloaded intervals failed; their first bucket is dropped", "intervals", h.ivs, "err", h.err)
		return
	}
	ones := sortedClosed(h.hist[market.I1m])
	var warmed []market.Interval
	for _, iv := range h.ivs {
		if !w.cold[iv] || w.aggs[iv] == nil {
			continue
		}
		delete(w.cold, iv)
		for _, k := range sortedClosed(h.hist[iv]) {
			if k.Interval == iv {
				_ = w.onBar(ctx, k, false)
			}
		}
		last, ok := w.lastClosed[iv]
		if !ok {
			w.cold[iv] = true
			continue
		}
		filled := false
		next := last.Add(iv.Duration())
		for _, k := range ones {
			if k.Interval != market.I1m || k.OpenTime.Before(next) {
				continue
			}
			filled = true
			for _, b := range w.aggs[iv].Add(k) {
				_ = w.onBar(ctx, b, false)
			}
		}
		if !filled && iv != market.I1m {
			w.cold[iv] = true
		}
		warmed = append(warmed, iv)
	}
	if len(warmed) > 0 {
		w.primeGate(warmed)
	}
	w.fetchCold(ctx) // intervals gained while this fetch was in flight
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
				w.fetchCold(ctx)
			}
		case h := <-w.histCh:
			w.applyHistory(ctx, h)
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
	// Only realtime contract-only rules react to contract events; on_close
	// ones wait for their interval's close like every other rule (R22).
	for i, r := range w.rules {
		if m := w.meta[i]; m.contractOnly && m.srcs&src != 0 && r.Trigger == config.TriggerRealtime {
			if err := w.evaluate(ctx, r, w.values(r.Interval, nil), price, false); err != nil {
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
				if err := w.evaluate(ctx, r, w.values(iv, &live), k.Close, false); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if w.cold[iv] {
		// Gained on a reload without (enough) history: this bucket started
		// mid-way, so its volume/high/low are partial. Skip it (I3).
		delete(w.cold, iv)
		w.lastClosed[iv] = k.OpenTime
		w.log.Info("engine: dropped the first, partial bucket of a reloaded interval", "interval", iv)
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
	stale := w.staleCheck && w.clk.Now().Sub(k.OpenTime.Add(iv.Duration())) > staleLag(iv)
	for _, r := range w.rules {
		if r.Interval == iv {
			if err := w.evaluate(ctx, r, v, k.Close, stale); err != nil {
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
// cooldown-suppressed edge (R2). A fire on a stale bar is emitted with
// Suppressed="stale": recorded, never delivered.
func (w *Worker) evaluate(ctx context.Context, r config.Rule, v rule.Values, price float64, stale bool) error {
	if r.When == nil {
		return nil
	}
	res := rule.Eval(r.When, v)
	fire, suppressed := w.gate.Observe(r.ID, w.symbol, res, r.Cooldown)
	if !fire && suppressed != "cooldown" {
		return nil
	}
	if fire && stale {
		suppressed = "stale"
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
// rules that were removed; a rule whose When/Interval changed only has its
// edge reset, so editing a threshold cannot bypass the cooldown. Indicators
// are rebuilt (by replaying retained bars). Contract state is kept.
func (w *Worker) reconfigure(cfg *config.Config) {
	next := map[string]config.Rule{}
	for _, r := range cfg.RulesFor(w.symbol) {
		next[r.ID] = r
	}
	for _, old := range w.rules {
		switch n, ok := next[old.ID]; {
		case !ok:
			w.gate.ForgetRuleSymbol(old.ID, w.symbol)
		case n.Interval != old.Interval || !reflect.DeepEqual(n.When, old.When):
			w.gate.ResetEdge(old.ID, w.symbol)
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
	for iv := range w.cold {
		if !set[iv] {
			delete(w.cold, iv)
		}
	}
	for _, iv := range targets {
		if w.aggs[iv] == nil {
			w.aggs[iv] = market.NewAggregator(w.symbol, []market.Interval{iv})
			if w.started {
				w.cold[iv] = true // gained on a reload: no history yet (I3)
			}
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
