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
// (the whole URL, path, query and query values).
func urlSecrets(raw string) []string {
	out := []string{raw}
	u, err := url.Parse(raw)
	if err != nil {
		return out
	}
	out = append(out, u.String(), u.RawQuery, u.EscapedPath())
	if len(u.Path) > 1 {
		out = append(out, u.Path)
	}
	for _, vs := range u.Query() {
		out = append(out, vs...)
	}
	if u.User != nil {
		out = append(out, u.User.String())
		if p, ok := u.User.Password(); ok {
			out = append(out, p)
		}
	}
	return out
}

// postJSON POSTs body as JSON to rawURL and hands the status and (at most
// 64 KiB of) response body to check, whose error is returned as is. Transport
// errors have the URL (and its secret-bearing parts) redacted.
func postJSON(ctx context.Context, hc *http.Client, rawURL string, body any, check func(status int, respBody []byte) error) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(buf))
	if err != nil {
		return redactURLError(err, urlSecrets(rawURL)...)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return redactURLError(err, urlSecrets(rawURL)...)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return check(resp.StatusCode, raw)
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
