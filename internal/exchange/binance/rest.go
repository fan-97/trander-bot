// Package binance connects to Binance USDT-M perpetual futures: a thin
// rate-limited REST client and a resilient combined WebSocket stream.
package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/market"
)

// Default production endpoints. Binance serves kline/markPrice/forceOrder on
// the "/market" WebSocket path; the legacy root path accepts connections but
// no longer delivers these streams.
const (
	DefaultRESTURL = "https://fapi.binance.com"
	DefaultWSURL   = "wss://fstream.binance.com/market"
)

// DefaultWeightPerMin leaves headroom below Binance's 2400/min IP limit.
const DefaultWeightPerMin = 1200

// maxKlinesPerRequest is Binance's cap on the klines limit parameter.
const maxKlinesPerRequest = 1500

// REST is a Binance USDT-M futures REST client. Every request goes through a
// weight-based token bucket driven by Clock.
type REST struct {
	// Clock drives rate limiting, Retry-After back-off and the Closed flag of
	// klines. Set before first use; defaults to clock.Real.
	Clock clock.Clock
	// HTTP is the client used for requests; defaults to a 15s-timeout client.
	HTTP *http.Client
	// Logger defaults to slog.Default().
	Logger *slog.Logger

	baseURL string
	bucket  bucket
}

// NewREST returns a client for baseURL (e.g. DefaultRESTURL) that spends at
// most weightPerMin request weight per minute (<=0 means DefaultWeightPerMin).
func NewREST(baseURL string, weightPerMin int) *REST {
	if weightPerMin <= 0 {
		weightPerMin = DefaultWeightPerMin
	}
	return &REST{
		Clock:   clock.Real{},
		HTTP:    &http.Client{Timeout: 15 * time.Second},
		baseURL: strings.TrimRight(baseURL, "/"),
		bucket:  bucket{capacity: float64(weightPerMin)},
	}
}

func (r *REST) clk() clock.Clock {
	if r.Clock == nil {
		return clock.Real{}
	}
	return r.Clock
}

func (r *REST) log() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// bucket is a continuously refilling token bucket: capacity tokens per minute.
type bucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	last     time.Time
	started  bool
	// pauseUntil blocks every caller after a 429/418 until Retry-After ends.
	pauseUntil time.Time
}

// pause makes all acquires wait until t (a later pause wins).
func (b *bucket) pause(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.After(b.pauseUntil) {
		b.pauseUntil = t
	}
}

func (b *bucket) refill(now time.Time) {
	if !b.started {
		b.started, b.tokens, b.last = true, b.capacity, now
		return
	}
	if el := now.Sub(b.last); el > 0 {
		b.tokens += b.capacity * el.Minutes()
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
}

// ErrPaused is returned instead of waiting when the shared 429/418 pause
// lasts beyond the caller's context deadline.
var ErrPaused = errors.New("binance: requests paused after rate limit (429/418)")

// acquire blocks until weight tokens are available or ctx is done. A caller
// whose ctx deadline comes before the end of a 429/418 pause gets ErrPaused
// at once rather than sleeping into its deadline.
func (b *bucket) acquire(ctx context.Context, clk clock.Clock, weight int) error {
	w := float64(weight)
	if w > b.capacity {
		w = b.capacity // otherwise it could never be satisfied
	}
	for {
		b.mu.Lock()
		now := clk.Now()
		if now.Before(b.pauseUntil) {
			wait := b.pauseUntil.Sub(now)
			b.mu.Unlock()
			if dl, ok := ctx.Deadline(); ok && time.Until(dl) < wait {
				return fmt.Errorf("%w for another %v", ErrPaused, wait)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-clk.After(wait):
			}
			continue
		}
		b.refill(now)
		if b.tokens >= w {
			b.tokens -= w
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((w - b.tokens) / b.capacity * float64(time.Minute))
		b.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(wait):
		}
	}
}

// observe syncs the bucket with the server-reported used weight: above 90%
// of capacity, local tokens are cut to what the server says is left.
func (b *bucket) observe(clk clock.Clock, used int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if float64(used) <= 0.9*b.capacity {
		return
	}
	b.refill(clk.Now())
	left := b.capacity - float64(used)
	if left < 0 {
		left = 0
	}
	if b.tokens > left {
		b.tokens = left
	}
}

// klinesWeight is the documented request weight for a klines call:
// [1,100) 1, [100,500) 2, [500,1000] 5, >1000 10.
func klinesWeight(limit int) int {
	switch {
	case limit < 100:
		return 1
	case limit < 500:
		return 2
	case limit <= 1000:
		return 5
	}
	return 10
}

// StatusError is a non-2xx Binance response.
type StatusError struct {
	Path   string
	Status int
	Body   string

	retryAfter string // Retry-After header, used for 429/418 back-off
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("binance: GET %s: status %d: %s", e.Path, e.Status, e.Body)
}

// get performs a rate-limited GET and decodes the JSON body into out.
// HTTP 429/418 pauses every caller for Retry-After (default 60s; a 418 is an
// IP ban) and this call is retried once after the pause.
func (r *REST) get(ctx context.Context, path string, q url.Values, weight int, out any) error {
	for attempt := 0; ; attempt++ {
		err := r.getOnce(ctx, path, q, weight, out)
		var se *StatusError
		if !errors.As(err, &se) || (se.Status != http.StatusTooManyRequests && se.Status != http.StatusTeapot) {
			return err
		}
		wait := 60 * time.Second
		if v, perr := strconv.Atoi(se.retryAfter); perr == nil && v >= 0 {
			wait = time.Duration(v) * time.Second
		}
		r.log().Warn("binance rate limited, pausing all requests", "path", path, "status", se.Status, "wait", wait)
		// The pause applies to every caller; the retry's acquire waits it out.
		r.bucket.pause(r.clk().Now().Add(wait))
		if attempt > 0 {
			return err
		}
	}
}

func (r *REST) getOnce(ctx context.Context, path string, q url.Values, weight int, out any) error {
	if err := r.bucket.acquire(ctx, r.clk(), weight); err != nil {
		return err
	}
	u := r.baseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	hc := r.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("binance: GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	if used, err := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); err == nil {
		r.bucket.observe(r.clk(), used)
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{Path: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(body)),
			retryAfter: resp.Header.Get("Retry-After")}
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("binance: GET %s: decode: %w", path, err)
	}
	return nil
}

// Symbols returns the tradable USDT-margined perpetual contracts.
func (r *REST) Symbols(ctx context.Context) (map[string]bool, error) {
	cs, err := r.Contracts(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(cs))
	for _, c := range cs {
		out[c.Symbol] = true
	}
	return out, nil
}

// Contract is a tradable USDT perpetual from exchangeInfo.
type Contract struct {
	Symbol         string
	BaseAsset      string
	UnderlyingType string    // COIN, INDEX, ...
	OnboardDate    time.Time // listing time; zero if unknown
}

// Contracts returns the USDT perpetuals currently trading (weight 1).
func (r *REST) Contracts(ctx context.Context) ([]Contract, error) {
	var info struct {
		Symbols []struct {
			Symbol         string `json:"symbol"`
			ContractType   string `json:"contractType"`
			QuoteAsset     string `json:"quoteAsset"`
			Status         string `json:"status"`
			BaseAsset      string `json:"baseAsset"`
			UnderlyingType string `json:"underlyingType"`
			OnboardDate    int64  `json:"onboardDate"`
		} `json:"symbols"`
	}
	if err := r.get(ctx, "/fapi/v1/exchangeInfo", nil, 1, &info); err != nil {
		return nil, err
	}
	var out []Contract
	for _, s := range info.Symbols {
		if s.ContractType != "PERPETUAL" || s.QuoteAsset != "USDT" || s.Status != "TRADING" {
			continue
		}
		c := Contract{Symbol: s.Symbol, BaseAsset: s.BaseAsset, UnderlyingType: s.UnderlyingType}
		if s.OnboardDate > 0 {
			c.OnboardDate = time.UnixMilli(s.OnboardDate).UTC()
		}
		out = append(out, c)
	}
	return out, nil
}

// Ticker24h is one symbol's rolling 24h statistics.
type Ticker24h struct {
	Symbol      string
	QuoteVolume float64 // USDT
	ChangePct   float64
}

// Tickers24h returns 24h statistics for every symbol (weight 40).
func (r *REST) Tickers24h(ctx context.Context) ([]Ticker24h, error) {
	var rows []struct {
		Symbol             string          `json:"symbol"`
		QuoteVolume        json.RawMessage `json:"quoteVolume"`
		PriceChangePercent json.RawMessage `json:"priceChangePercent"`
	}
	if err := r.get(ctx, "/fapi/v1/ticker/24hr", nil, 40, &rows); err != nil {
		return nil, err
	}
	out := make([]Ticker24h, 0, len(rows))
	for _, row := range rows {
		qv, err := rawFloat(row.QuoteVolume)
		if err != nil {
			return nil, fmt.Errorf("binance: ticker %s quoteVolume: %w", row.Symbol, err)
		}
		cp, err := rawFloat(row.PriceChangePercent)
		if err != nil {
			return nil, fmt.Errorf("binance: ticker %s priceChangePercent: %w", row.Symbol, err)
		}
		out = append(out, Ticker24h{Symbol: row.Symbol, QuoteVolume: qv, ChangePct: cp})
	}
	return out, nil
}

// ValidateSymbols splits want (case-insensitive, de-duplicated, order kept)
// into symbols present in have and those that are not.
func ValidateSymbols(want []string, have map[string]bool) (ok []string, dropped []string) {
	seen := make(map[string]bool, len(want))
	for _, w := range want {
		s := strings.ToUpper(strings.TrimSpace(w))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		if have[s] {
			ok = append(ok, s)
		} else {
			dropped = append(dropped, s)
		}
	}
	return ok, dropped
}

// Klines fetches up to limit bars of iv starting at start (<=0 means 500),
// paging past Binance's 1500-per-request cap. A bar is Closed only if it
// ended at or before Clock.Now().
func (r *REST) Klines(ctx context.Context, symbol string, iv market.Interval, start time.Time, limit int) ([]market.Kline, error) {
	if !iv.Valid() {
		return nil, fmt.Errorf("binance: invalid interval %q", iv)
	}
	if limit <= 0 {
		limit = 500
	}
	var all []market.Kline
	for len(all) < limit {
		n := limit - len(all)
		if n > maxKlinesPerRequest {
			n = maxKlinesPerRequest
		}
		page, err := r.klinesPage(ctx, symbol, iv, start, n)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < n {
			break
		}
		start = page[len(page)-1].OpenTime.Add(iv.Duration())
	}
	return all, nil
}

func (r *REST) klinesPage(ctx context.Context, symbol string, iv market.Interval, start time.Time, n int) ([]market.Kline, error) {
	q := url.Values{
		"symbol":    {symbol},
		"interval":  {string(iv)},
		"startTime": {strconv.FormatInt(start.UnixMilli(), 10)},
		"limit":     {strconv.Itoa(n)},
	}
	var rows [][]json.RawMessage
	if err := r.get(ctx, "/fapi/v1/klines", q, klinesWeight(n), &rows); err != nil {
		return nil, err
	}
	now := r.clk().Now()
	out := make([]market.Kline, 0, len(rows))
	for i, row := range rows {
		if len(row) < 8 {
			return nil, fmt.Errorf("binance: klines row %d: %d fields", i, len(row))
		}
		var openMS int64
		if err := json.Unmarshal(row[0], &openMS); err != nil {
			return nil, fmt.Errorf("binance: klines row %d open time: %w", i, err)
		}
		k := market.Kline{Symbol: symbol, Interval: iv, OpenTime: time.UnixMilli(openMS).UTC()}
		for _, f := range []struct {
			idx int
			dst *float64
		}{{1, &k.Open}, {2, &k.High}, {3, &k.Low}, {4, &k.Close}, {5, &k.Volume}, {7, &k.QuoteVolume}} {
			v, err := rawFloat(row[f.idx])
			if err != nil {
				return nil, fmt.Errorf("binance: klines row %d field %d: %w", i, f.idx, err)
			}
			*f.dst = v
		}
		k.Closed = !k.OpenTime.Add(iv.Duration()).After(now)
		out = append(out, k)
	}
	return out, nil
}

// rawFloat parses a JSON string or number holding a decimal.
func rawFloat(b json.RawMessage) (float64, error) {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		s = string(b)
	}
	return strconv.ParseFloat(s, 64)
}

// OpenInterest returns the current open interest of symbol.
func (r *REST) OpenInterest(ctx context.Context, symbol string) (market.OpenInterestEvent, error) {
	var resp struct {
		OpenInterest string `json:"openInterest"`
		Time         int64  `json:"time"`
	}
	if err := r.get(ctx, "/fapi/v1/openInterest", url.Values{"symbol": {symbol}}, 1, &resp); err != nil {
		return market.OpenInterestEvent{}, err
	}
	oi, err := strconv.ParseFloat(resp.OpenInterest, 64)
	if err != nil {
		return market.OpenInterestEvent{}, fmt.Errorf("binance: openInterest %s: %w", symbol, err)
	}
	return market.OpenInterestEvent{Symbol: symbol, Time: time.UnixMilli(resp.Time).UTC(), OI: oi}, nil
}

// LongShort returns the latest 5m long/short ratio: kind "global" is the
// account ratio of all traders, "top" the position ratio of top traders.
func (r *REST) LongShort(ctx context.Context, symbol, kind string) (market.LongShortEvent, error) {
	var path string
	switch kind {
	case "global":
		path = "/futures/data/globalLongShortAccountRatio"
	case "top":
		path = "/futures/data/topLongShortPositionRatio"
	default:
		return market.LongShortEvent{}, fmt.Errorf("binance: unknown long/short kind %q", kind)
	}
	var rows []struct {
		Ratio     string `json:"longShortRatio"`
		Timestamp int64  `json:"timestamp"`
	}
	q := url.Values{"symbol": {symbol}, "period": {"5m"}, "limit": {"1"}}
	if err := r.get(ctx, path, q, 1, &rows); err != nil {
		return market.LongShortEvent{}, err
	}
	if len(rows) == 0 {
		return market.LongShortEvent{}, fmt.Errorf("binance: %s %s: empty response", path, symbol)
	}
	last := rows[len(rows)-1]
	ratio, err := strconv.ParseFloat(last.Ratio, 64)
	if err != nil {
		return market.LongShortEvent{}, fmt.Errorf("binance: %s %s: %w", path, symbol, err)
	}
	return market.LongShortEvent{Symbol: symbol, Time: time.UnixMilli(last.Timestamp).UTC(), Kind: kind, Ratio: ratio}, nil
}

// PollContract samples open interest and both long/short ratios for every
// symbol immediately and then every `every` (<=0 means 5m) on clk, sending
// events to out.
// A failing call is logged and skipped. It returns ctx.Err() when ctx ends.
func PollContract(ctx context.Context, r *REST, symbols func() []string, every time.Duration, out chan<- market.Event, clk clock.Clock) error {
	if every <= 0 {
		every = 5 * time.Minute
	}
	send := func(ev market.Event) error {
		select {
		case out <- ev:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		for _, sym := range symbols() {
			if oi, err := r.OpenInterest(ctx, sym); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.log().Warn("binance open interest poll failed", "symbol", sym, "err", err)
			} else if err := send(oi); err != nil {
				return err
			}
			for _, kind := range []string{"global", "top"} {
				ls, err := r.LongShort(ctx, sym, kind)
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					r.log().Warn("binance long/short poll failed", "symbol", sym, "kind", kind, "err", err)
					continue
				}
				if err := send(ls); err != nil {
					return err
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(every):
		}
	}
}
