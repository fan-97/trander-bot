package engine

import (
	"time"

	"trader-bot/internal/config"
	"trader-bot/internal/rule"
)

// seqState is the per-rule state of a two-stage (setup) rule.
type seqState struct {
	setupLast rule.Result // previous Setup.When result (Unknown: not seen)
	trigLast  rule.Result // previous When result
	armed     bool
	lastTrue  time.Time // when Setup.When was last True while armed
	detail    string    // Setup.When description when it armed
}

func newSeqState() *seqState { return &seqState{setupLast: rule.Unknown, trigLast: rule.Unknown} }

// sequence folds the setup stage into r's trigger result trig at time at. It
// returns True only when When goes to True while the rule is armed (and then
// disarms), False otherwise and Unknown when trig is Unknown, so the gate's
// own edge, cooldown and stale handling apply unchanged.
func (w *Worker) sequence(r config.Rule, v rule.Values, trig rule.Result, at time.Time) rule.Result {
	st := w.seq[r.ID]
	if st == nil {
		st = newSeqState()
		w.seq[r.ID] = st
	}
	su := r.Setup
	setup := rule.Eval(su.When, v)
	switch {
	case setup == rule.True && st.setupLast != rule.True:
		// A fresh rising edge arms (or re-arms) the rule.
		st.armed, st.lastTrue = true, at
		st.detail = rule.Describe(su.When, v)
	case setup == rule.True && st.armed:
		st.lastTrue = at
	}
	if setup != rule.Unknown {
		st.setupLast = setup
	}
	if st.armed && su.Invalidate != nil && rule.Eval(su.Invalidate, v) == rule.True {
		st.armed = false
	}
	if st.armed && at.Sub(st.lastTrue) > su.Within {
		st.armed = false
	}

	if trig == rule.Unknown {
		return rule.Unknown
	}
	rising := trig == rule.True && st.trigLast != rule.True
	st.trigLast = trig
	if rising && st.armed {
		st.armed = false // one arming yields at most one signal
		return rule.True
	}
	return rule.False
}

// primeSequence seeds r's state from the warmed-up values: a trigger already
// True is not a new edge, and a setup already True arms the rule from now.
func (w *Worker) primeSequence(r config.Rule, v rule.Values) {
	st := newSeqState()
	st.trigLast = rule.Eval(r.When, v)
	if rule.Eval(r.Setup.When, v) == rule.True {
		st.setupLast, st.armed, st.lastTrue = rule.True, true, w.clk.Now()
		st.detail = rule.Describe(r.Setup.When, v)
	}
	w.seq[r.ID] = st
}

// seqDetail is appended to a two-stage rule's signal detail.
func seqDetail(st *seqState) string {
	if st == nil || st.detail == "" {
		return ""
	}
	return " | 前置: " + st.detail
}
