package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
)

const (
	// streamsPerSymbol is kline_1m + markPrice@1s + forceOrder.
	streamsPerSymbol = 3
	// maxStreamsPerConn is Binance's per-connection limit.
	maxStreamsPerConn = 1024
	// maxURLLen bounds a combined-stream URL. Probed 2026-10-31: Binance
	// accepted ~5.5KB (90 symbols) but dropped the handshake at ~6.2KB
	// (100 symbols), so stay well below.
	maxURLLen = 4000
	// closeMargin: a REST bar counts as closed for gap fill only if it ended
	// at least this long before the request, so a partial bar is never
	// emitted as Closed.
	closeMargin = 2 * time.Second
)

// Stream maintains Binance combined-stream connections for a symbol pool and
// turns them into market events. See Run.
//
// onState flaps false→true on every symbol-pool change (all connections are
// rebuilt) and reports false on shutdown, so consumers that alert on
// disconnects must debounce (e.g. only alert after a minute down).
type Stream struct {
	// Logger defaults to slog.Default().
	Logger *slog.Logger

	wsURL   string
	rest    *REST
	clk     clock.Clock
	onState func(connected bool)

	// Tunables; tests override them.
	minBackoff  time.Duration
	maxBackoff  time.Duration
	stableAfter time.Duration
	readTimeout time.Duration
	rotateAfter time.Duration
	fillTimeout time.Duration

	emitMu     sync.Mutex
	lastClosed map[string]time.Time // last emitted closed 1m OpenTime per symbol

	stateMu  sync.Mutex
	shardUp  map[int]bool
	shards   int
	reported bool
	lastUp   bool
}

// NewStream returns a Stream for wsURL (e.g. DefaultWSURL). rest is used for
// gap filling (its Clock should be clk). onState, if non-nil, is called with
// true once all connections are up and false when any goes down (or the
// first dial fails); it is called synchronously and must not block.
func NewStream(wsURL string, rest *REST, clk clock.Clock, onState func(connected bool)) *Stream {
	if clk == nil {
		clk = clock.Real{}
	}
	if u, err := url.Parse(wsURL); err != nil || u.Path == "" || u.Path == "/" {
		slog.Default().Warn("binance stream: ws URL has no path; Binance accepts such connections but sends no data — use "+DefaultWSURL, "ws_url", wsURL)
	}
	return &Stream{
		wsURL:       wsURL,
		rest:        rest,
		clk:         clk,
		onState:     onState,
		minBackoff:  time.Second,
		maxBackoff:  60 * time.Second,
		stableAfter: time.Minute,
		readTimeout: 3 * time.Minute,
		rotateAfter: 23 * time.Hour,
		fillTimeout: 10 * time.Second,
		lastClosed:  make(map[string]time.Time),
	}
}

func (s *Stream) log() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// Run streams <s>@kline_1m, <s>@markPrice@1s and <s>@forceOrder for every
// symbol in symbols() until ctx ends, then returns ctx.Err(). Dropped or
// silent (3 min) connections are re-dialled with exponential back-off
// (1s..60s, reset after a minute up); each connection is replaced after 23h
// by opening the new one before closing the old. A signal on changed rebuilds
// the subscriptions from symbols(). After every reconnect the missing closed
// 1m bars are fetched over REST and emitted before live data; closed bars are
// never emitted twice or out of order.
func (s *Stream) Run(ctx context.Context, symbols func() []string, changed <-chan struct{}, out chan<- market.Event) error {
	for {
		syms := normalizeSymbols(symbols())
		s.pruneLastClosed(syms)
		shards := streamShards(s.wsURL, syms)

		sctx, cancel := context.WithCancel(ctx)
		s.resetShards(len(shards))
		var wg sync.WaitGroup
		for i, sh := range shards {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.runShard(sctx, i, sh.url, sh.syms, out)
			}()
		}
		select {
		case <-ctx.Done():
		case <-changed:
			s.log().Info("binance stream: symbol pool changed, resubscribing")
		}
		cancel()
		wg.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// Seed records lastClosed as the OpenTime of the last closed 1m bar of symbol
// already known to the caller (e.g. from warmup), so the next closed live bar
// triggers a fill of anything missing in between. An older value is ignored.
func (s *Stream) Seed(symbol string, lastClosed time.Time) {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if last, ok := s.lastClosed[sym]; !ok || lastClosed.After(last) {
		s.lastClosed[sym] = lastClosed
	}
}

// pruneLastClosed forgets symbols that left the pool, so one re-added later
// starts fresh instead of back-filling the whole time it was away.
func (s *Stream) pruneLastClosed(syms []string) {
	keep := make(map[string]bool, len(syms))
	for _, sym := range syms {
		keep[sym] = true
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	for sym := range s.lastClosed {
		if !keep[sym] {
			delete(s.lastClosed, sym)
		}
	}
}

func normalizeSymbols(in []string) []string {
	seen := make(map[string]bool, len(in))
	var out []string
	for _, s := range in {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

type shard struct {
	url  string
	syms []string
}

// streamShards splits syms over as few combined-stream URLs as possible,
// keeping each under maxURLLen and maxStreamsPerConn.
func streamShards(wsURL string, syms []string) []shard {
	base := strings.TrimRight(wsURL, "/") + "/stream?streams="
	var out []shard
	var cur shard
	var b strings.Builder
	flush := func() {
		if len(cur.syms) > 0 {
			cur.url = base + b.String()
			out = append(out, cur)
		}
		cur = shard{}
		b.Reset()
	}
	for _, sym := range syms {
		l := strings.ToLower(sym)
		part := l + "@kline_1m/" + l + "@markPrice@1s/" + l + "@forceOrder"
		sep := 0
		if b.Len() > 0 {
			sep = 1
		}
		if len(cur.syms) > 0 && (len(base)+b.Len()+sep+len(part) > maxURLLen ||
			(len(cur.syms)+1)*streamsPerSymbol > maxStreamsPerConn) {
			flush()
			sep = 0
		}
		if sep == 1 {
			b.WriteByte('/')
		}
		b.WriteString(part)
		cur.syms = append(cur.syms, sym)
	}
	flush()
	return out
}

// streamURLs returns the URLs of streamShards.
func streamURLs(wsURL string, syms []string) []string {
	var urls []string
	for _, sh := range streamShards(wsURL, syms) {
		urls = append(urls, sh.url)
	}
	return urls
}

// ---- connection state aggregation ----

func (s *Stream) resetShards(n int) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.shards = n
	s.shardUp = make(map[int]bool, n)
}

func (s *Stream) setShard(id int, up bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.shardUp[id] = up
	all := s.shards > 0
	for i := 0; i < s.shards; i++ {
		all = all && s.shardUp[i]
	}
	if s.reported && all == s.lastUp {
		return
	}
	if !s.reported && !all && up {
		return // first start: wait until every shard is up
	}
	s.reported, s.lastUp = true, all
	s.callState(all)
}

func (s *Stream) callState(up bool) {
	if s.onState != nil {
		s.onState(up)
	}
}

// ---- one connection slot ----

type conn struct {
	c      *websocket.Conn
	msgs   chan []byte
	errc   chan error
	cancel context.CancelFunc
}

func (s *Stream) dial(ctx context.Context, u string) (*conn, error) {
	dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
	defer dcancel()
	c, _, err := websocket.Dial(dctx, u, nil)
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(1 << 20)
	rctx, cancel := context.WithCancel(ctx)
	cn := &conn{c: c, msgs: make(chan []byte), errc: make(chan error, 1), cancel: cancel}
	go func() {
		for {
			// Read also answers server pings.
			_, b, err := c.Read(rctx)
			if err != nil {
				cn.errc <- err
				return
			}
			select {
			case cn.msgs <- b:
			case <-rctx.Done():
				cn.errc <- rctx.Err()
				return
			}
		}
	}()
	return cn, nil
}

func (cn *conn) close() {
	cn.cancel()
	cn.c.CloseNow()
}

var errReadTimeout = errors.New("binance stream: no message within read timeout")

func (s *Stream) runShard(ctx context.Context, id int, u string, syms []string, out chan<- market.Event) {
	backoff := s.minBackoff
	for ctx.Err() == nil {
		cn, err := s.dial(ctx, u)
		var wait <-chan time.Time
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log().Warn("binance stream: dial failed", "shard", id, "err", err, "retry_in", backoff)
			wait = s.clk.After(backoff)
			s.setShard(id, false)
		} else {
			connectedAt := s.clk.Now()
			// Arm the timers before reporting up so a test (or caller) that
			// reacts to onState(true) by moving the clock is observed.
			rotate := s.clk.After(s.rotateAfter)
			watch := s.clk.After(s.readTimeout)
			s.setShard(id, true)
			s.gapFill(ctx, syms, out)
			err = s.serve(ctx, cn, u, rotate, watch, out)
			if ctx.Err() != nil {
				s.setShard(id, false)
				return
			}
			if s.clk.Now().Sub(connectedAt) >= s.stableAfter {
				backoff = s.minBackoff
			}
			s.log().Warn("binance stream: disconnected", "shard", id, "err", err, "retry_in", backoff)
			wait = s.clk.After(backoff)
			s.setShard(id, false)
		}
		backoff = min(backoff*2, s.maxBackoff)
		select {
		case <-ctx.Done():
			return
		case <-wait:
		}
	}
}

// serve reads the active connection, rotating it after rotateAfter, until it
// fails, goes silent for readTimeout, or ctx ends.
func (s *Stream) serve(ctx context.Context, cur *conn, u string, rotate, watch <-chan time.Time, out chan<- market.Event) error {
	var old *conn
	defer func() {
		cur.close()
		if old != nil {
			old.close()
		}
	}()
	lastMsg := s.clk.Now()
	var oldMsgs <-chan []byte
	var oldErrc <-chan error
	dropOld := func() {
		if old != nil {
			old.close()
			old, oldMsgs, oldErrc = nil, nil, nil
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b := <-cur.msgs:
			lastMsg = s.clk.Now()
			dropOld() // the new connection is live; the overlap ends
			s.handle(ctx, b, out)
		case b := <-oldMsgs:
			s.handle(ctx, b, out)
		case err := <-cur.errc:
			return err
		case <-oldErrc:
			dropOld()
		case <-rotate:
			nc, err := s.dial(ctx, u)
			if err != nil {
				s.log().Warn("binance stream: rotation dial failed, keeping current connection", "err", err)
				rotate = s.clk.After(time.Minute)
				continue
			}
			dropOld()
			old, oldMsgs, oldErrc = cur, cur.msgs, cur.errc
			cur = nc
			lastMsg = s.clk.Now()
			rotate = s.clk.After(s.rotateAfter)
		case <-watch:
			idle := s.clk.Now().Sub(lastMsg)
			if idle >= s.readTimeout {
				return errReadTimeout
			}
			watch = s.clk.After(s.readTimeout - idle)
		}
	}
}

// gapFill runs after each (re)connect: for every symbol with a known last
// closed bar it emits the bars that closed safely (closeMargin) before now.
// One bounded attempt only; a failure is caught later by hole detection in
// forward.
func (s *Stream) gapFill(ctx context.Context, syms []string, out chan<- market.Event) {
	if s.rest == nil {
		return
	}
	until := s.clk.Now().Add(-closeMargin)
	for _, sym := range syms {
		s.emitMu.Lock()
		if _, ok := s.lastClosed[sym]; ok {
			if err := s.fillLocked(ctx, sym, until, out); err != nil && ctx.Err() == nil {
				s.log().Warn("binance stream: reconnect gap fill failed", "symbol", sym, "err", err)
			}
		}
		s.emitMu.Unlock()
	}
}

// errIncomplete reports a fill whose REST answer ended before the requested
// range.
type errIncomplete struct{ upTo, want time.Time }

func (e *errIncomplete) Error() string {
	return fmt.Sprintf("incomplete: filled up to %s of %s", e.upTo.Format(time.RFC3339), e.want.Format(time.RFC3339))
}

// withClockTimeout returns ctx bounded by d both in real time (a deadline,
// which REST uses to fail fast under a long pause) and on s.clk.
func (s *Stream) withClockTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	tctx, cancel := context.WithTimeout(ctx, d)
	fire := s.clk.After(d)
	go func() {
		select {
		case <-fire:
			cancel()
		case <-tctx.Done():
		}
	}()
	return tctx, cancel
}

// fillLocked emits, in order, the closed 1m bars of sym after its last closed
// bar that ended at or before until, fetched over REST in one attempt bounded
// by fillTimeout. It returns an error if the REST call fails or the range is
// not completely covered (*errIncomplete). emitMu must be held, which is why
// the attempt is bounded: every shard waits on this lock.
func (s *Stream) fillLocked(ctx context.Context, sym string, until time.Time, out chan<- market.Event) error {
	start := s.lastClosed[sym].Add(time.Minute)
	n := int(until.Sub(start) / time.Minute) // bars ending at or before until
	if n <= 0 {
		return nil
	}
	fctx, cancel := s.withClockTimeout(ctx, s.fillTimeout)
	ks, err := s.rest.Klines(fctx, sym, market.I1m, start, n)
	cancel()
	if err != nil {
		return err
	}
	for _, k := range ks {
		last := s.lastClosed[sym]
		if k.Closed && k.OpenTime.After(last) && !k.OpenTime.Add(time.Minute).After(until) {
			s.emitLocked(ctx, market.KlineEvent{Kline: k}, out)
		}
	}
	want := start.Add(time.Duration(n) * time.Minute)
	if got := s.lastClosed[sym].Add(time.Minute); got.Before(want) {
		return &errIncomplete{upTo: got, want: want}
	}
	return nil
}

// holeRetryWaits are the waits between hole-fill attempts.
var holeRetryWaits = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

// fillHoleLocked fills the bars between the last closed bar of sym and a live
// closed bar opening at next: up to 1+len(holeRetryWaits) attempts of at most
// fillTimeout each, so emitMu is held for at most 4×10s + 7s. Under a REST
// rate-limit pause it gives up at once. If the hole stays, it is logged and
// accepted, so live data never stalls. emitMu must be held.
func (s *Stream) fillHoleLocked(ctx context.Context, sym string, next time.Time, out chan<- market.Event) {
	from := s.lastClosed[sym].Add(time.Minute)
	var err error
	for attempt := 0; ; attempt++ {
		if s.rest == nil {
			err = errors.New("no REST client")
			break
		}
		err = s.fillLocked(ctx, sym, next, out)
		if err == nil || ctx.Err() != nil || errors.Is(err, ErrPaused) || attempt >= len(holeRetryWaits) {
			break
		}
		s.log().Warn("binance stream: hole fill failed, retrying", "symbol", sym, "err", err, "retry_in", holeRetryWaits[attempt])
		select {
		case <-ctx.Done():
		case <-s.clk.After(holeRetryWaits[attempt]):
		}
	}
	if err == nil || ctx.Err() != nil {
		return
	}
	var inc *errIncomplete
	if errors.As(err, &inc) && !s.lastClosed[sym].Before(from) {
		// Most bars came back; the tail is likely an exchange-side gap.
		s.log().Warn("binance stream: hole only partly filled, continuing with live data",
			"symbol", sym, "from", from, "to", next.Add(-time.Minute), "err", err)
		return
	}
	s.log().Error("binance stream: could not fill missing 1m bars, continuing with live data",
		"symbol", sym, "from", from, "to", next.Add(-time.Minute), "err", err)
}

func (s *Stream) handle(ctx context.Context, b []byte, out chan<- market.Event) {
	ev, err := parseMessage(b)
	if err != nil {
		s.log().Warn("binance stream: bad message", "err", err)
		return
	}
	if ev != nil {
		s.forward(ctx, ev, out)
	}
}

// forward sends ev unless it is a kline at or before the last emitted closed
// bar of its symbol. A closed bar that skips ahead of last+1m first gets the
// hole filled over REST (see fillHoleLocked). Sends are serialized so
// ordering holds across connections overlapping during rotation; a hole fill
// therefore holds back events of other connections until it finishes.
func (s *Stream) forward(ctx context.Context, ev market.Event, out chan<- market.Event) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if k, ok := ev.(market.KlineEvent); ok {
		if k.Interval != market.I1m {
			return
		}
		last, seen := s.lastClosed[k.Symbol]
		if seen && !k.OpenTime.After(last) {
			return
		}
		if seen && k.Closed && k.OpenTime.After(last.Add(time.Minute)) {
			s.fillHoleLocked(ctx, k.Symbol, k.OpenTime, out)
		}
	}
	s.emitLocked(ctx, ev, out)
}

// emitLocked sends ev and records closed 1m bars. emitMu must be held.
func (s *Stream) emitLocked(ctx context.Context, ev market.Event, out chan<- market.Event) {
	if k, ok := ev.(market.KlineEvent); ok && k.Closed {
		s.lastClosed[k.Symbol] = k.OpenTime
	}
	select {
	case out <- ev:
	case <-ctx.Done():
	}
}

// ---- message parsing ----

type wsEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// fields decodes a JSON object keeping keys case-sensitive: Binance uses
// keys that differ only in case ("t"/"T", "p"/"P"), which encoding/json's
// case-insensitive struct matching would confuse.
type fields map[string]json.RawMessage

func (f fields) str(k string) (string, error) {
	raw, ok := f[k]
	if !ok {
		return "", fmt.Errorf("missing %q", k)
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", fmt.Errorf("field %q: %w", k, err)
	}
	return v, nil
}

func (f fields) num(k string) (float64, error) {
	v, err := f.str(k)
	if err != nil {
		return 0, err
	}
	x, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", k, err)
	}
	return x, nil
}

func (f fields) ms(k string) (time.Time, error) {
	raw, ok := f[k]
	if !ok {
		return time.Time{}, fmt.Errorf("missing %q", k)
	}
	var v int64
	if err := json.Unmarshal(raw, &v); err != nil {
		return time.Time{}, fmt.Errorf("field %q: %w", k, err)
	}
	return time.UnixMilli(v).UTC(), nil
}

func (f fields) obj(k string) (fields, error) {
	var o fields
	raw, ok := f[k]
	if !ok {
		return nil, fmt.Errorf("missing %q", k)
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil, fmt.Errorf("field %q: %w", k, err)
	}
	return o, nil
}

// parseMessage decodes one combined-stream message. Messages that are not
// kline/markPrice/forceOrder events (e.g. subscription acks) yield nil, nil.
func parseMessage(b []byte) (market.Event, error) {
	var env wsEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	if len(env.Data) == 0 {
		return nil, nil
	}
	var d fields
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return nil, fmt.Errorf("%s: %w", env.Stream, err)
	}
	typ, _ := d.str("e")
	var ev market.Event
	var err error
	switch typ {
	case "kline":
		ev, err = parseKline(d)
	case "markPriceUpdate":
		ev, err = parseMarkPrice(d)
	case "forceOrder":
		ev, err = parseForceOrder(d)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", env.Stream, err)
	}
	return ev, nil
}

// firstErr returns the first non-nil error.
func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func parseKline(d fields) (market.Event, error) {
	k, err := d.obj("k")
	if err != nil {
		return nil, err
	}
	var kl market.Kline
	var e [10]error
	kl.Symbol, e[0] = d.str("s")
	var iv string
	iv, e[1] = k.str("i")
	kl.Interval = market.Interval(iv)
	kl.OpenTime, e[2] = k.ms("t")
	kl.Open, e[3] = k.num("o")
	kl.High, e[4] = k.num("h")
	kl.Low, e[5] = k.num("l")
	kl.Close, e[6] = k.num("c")
	kl.Volume, e[7] = k.num("v")
	kl.QuoteVolume, e[8] = k.num("q")
	if raw, ok := k["x"]; ok {
		e[9] = json.Unmarshal(raw, &kl.Closed)
	} else {
		e[9] = errors.New(`missing "x"`)
	}
	if err := firstErr(e[:]...); err != nil {
		return nil, err
	}
	return market.KlineEvent{Kline: kl}, nil
}

func parseMarkPrice(d fields) (market.Event, error) {
	var m market.MarkPriceEvent
	var e [5]error
	m.Symbol, e[0] = d.str("s")
	m.Time, e[1] = d.ms("E")
	m.MarkPrice, e[2] = d.num("p")
	m.FundingRate, e[3] = d.num("r")
	m.NextFunding, e[4] = d.ms("T")
	if err := firstErr(e[:]...); err != nil {
		return nil, err
	}
	return m, nil
}

func parseForceOrder(d fields) (market.Event, error) {
	o, err := d.obj("o")
	if err != nil {
		return nil, err
	}
	var l market.LiquidationEvent
	var e [5]error
	l.Symbol, e[0] = o.str("s")
	l.Side, e[1] = o.str("S")
	l.Qty, e[2] = o.num("z")
	l.Time, e[3] = o.ms("T")
	if ap, aerr := o.num("ap"); aerr == nil && ap > 0 {
		l.Price = ap
	} else {
		l.Price, e[4] = o.num("p")
	}
	if err := firstErr(e[:]...); err != nil {
		return nil, err
	}
	return l, nil
}
