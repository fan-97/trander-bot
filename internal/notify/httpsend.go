package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// redactedError hides secrets (bot tokens, webhook keys, passwords) that
// appear in *url.Error messages, while keeping the cause reachable through
// errors.Is/As.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// redactURLError replaces every non-empty secret in err's text with
// "<redacted>". The *url.Error wrapper is dropped from the unwrap chain (it
// carries the URL) but its cause (context.DeadlineExceeded, ...) stays
// reachable. Longer secrets are replaced first so overlapping ones are safe.
func redactURLError(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	inner := err
	var ue *url.Error
	if errors.As(err, &ue) {
		inner = ue.Err
	}
	ss := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if s != "" {
			ss = append(ss, s)
		}
	}
	sort.Slice(ss, func(i, j int) bool { return len(ss[i]) > len(ss[j]) })
	msg := err.Error()
	for _, s := range ss {
		msg = strings.ReplaceAll(msg, s, "<redacted>")
	}
	return &redactedError{msg: msg, err: inner}
}

// urlSecrets lists the substrings of a request URL that may hold secrets
// (the whole URL, path, query and longer query values). Fragments shorter
// than 6 bytes are skipped so error text is not garbled.
func urlSecrets(raw string) []string {
	cand := []string{raw}
	if u, err := url.Parse(raw); err == nil {
		cand = append(cand, u.String(), u.RawQuery, u.EscapedPath(), u.Path)
		for _, vs := range u.Query() {
			cand = append(cand, vs...)
		}
		if u.User != nil {
			cand = append(cand, u.User.String())
			if p, ok := u.User.Password(); ok {
				cand = append(cand, p)
			}
		}
	}
	out := cand[:0]
	for _, c := range cand {
		if len(c) >= 6 {
			out = append(out, c)
		}
	}
	return out
}

// errInvalidURL is returned instead of the library's parse error, which would
// print the (secret-bearing) URL in a form redaction cannot match.
var errInvalidURL = errors.New("invalid webhook URL")

// validateURL checks that raw is an absolute http(s) URL. The error names the
// param but never echoes the value.
func validateURL(name, typ, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.ContainsFunc(raw, unicode.IsControl) {
		return fmt.Errorf("channel %q: %s url is invalid", name, typ)
	}
	return nil
}

func newHTTPClient(hc *http.Client) *http.Client {
	if hc == nil {
		return &http.Client{Timeout: 15 * time.Second}
	}
	return hc
}

// marshalJSON encodes v without HTML escaping or a trailing newline.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// fitJSON shrinks text (on rune boundaries) until the marshalled body built
// from it is at most limit bytes, and returns that body.
func fitJSON(text string, limit int, build func(text string) any) any {
	for {
		v := build(text)
		raw, err := marshalJSON(v)
		if err != nil || len(raw) <= limit || text == "" {
			return v
		}
		// Each raw byte marshals to at least one byte, so cutting the excess
		// always makes progress.
		text = truncateBytes(text, max(0, len(text)-(len(raw)-limit)))
	}
}

// postJSON POSTs body as JSON to rawURL and hands the status and (at most
// 64 KiB of) response body to check. Every returned error has the URL and its
// secret-bearing parts redacted.
func postJSON(ctx context.Context, hc *http.Client, rawURL string, body any, check func(status int, respBody []byte) error) error {
	secrets := urlSecrets(rawURL)
	buf, err := marshalJSON(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(buf))
	if err != nil {
		return errInvalidURL
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return redactURLError(err, secrets...)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err := check(resp.StatusCode, raw); err != nil {
		return redactURLError(err, secrets...)
	}
	return nil
}

// check2xx accepts any 2xx status.
func check2xx(status int, body []byte) error {
	if status < 200 || status > 299 {
		return fmt.Errorf("HTTP %d: %s", status, snippet(body))
	}
	return nil
}

func snippet(b []byte) string {
	return truncateRunes(strings.TrimSpace(string(b)), 200)
}

// renderText is the shared plain-text rendering: title, one line per entry,
// then links as "text: url".
func renderText(m Message) string {
	parts := make([]string, 0, 1+len(m.Lines)+len(m.Links))
	parts = append(parts, m.Title)
	parts = append(parts, m.Lines...)
	for _, l := range m.Links {
		parts = append(parts, l.Text+": "+l.URL)
	}
	return strings.Join(parts, "\n")
}

// truncateRunes returns s unchanged if it has at most n runes; otherwise the
// first n-1 runes followed by "…" (n runes in total).
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}

// truncateBytes cuts s to at most n bytes on a rune boundary.
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// requireParams returns an error naming the first missing param (never a value).
func requireParams(name, typ string, p map[string]string, keys ...string) error {
	for _, k := range keys {
		if p[k] == "" {
			return fmt.Errorf("channel %q: %s %s is required", name, typ, k)
		}
	}
	return nil
}
