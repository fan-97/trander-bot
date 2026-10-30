package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	telegramDefaultBase = "https://api.telegram.org"
	// telegramMaxChars is Telegram's sendMessage text limit.
	telegramMaxChars = 4096
)

// Telegram sends messages through the Bot API sendMessage method with
// parse_mode=HTML.
type Telegram struct {
	name    string
	token   string
	chatID  string
	baseURL string
	hc      *http.Client
}

func newTelegram(name string, p map[string]string, hc *http.Client) (*Telegram, error) {
	t := &Telegram{name: name, token: p["token"], chatID: p["chat_id"], baseURL: p["base_url"], hc: hc}
	if t.token == "" {
		return nil, fmt.Errorf("channel %q: telegram token is required", name)
	}
	if t.chatID == "" {
		return nil, fmt.Errorf("channel %q: telegram chat_id is required", name)
	}
	if t.baseURL == "" {
		t.baseURL = telegramDefaultBase
	}
	t.baseURL = strings.TrimRight(t.baseURL, "/")
	if t.hc == nil {
		t.hc = &http.Client{Timeout: 15 * time.Second}
	}
	return t, nil
}

// Name returns the configured channel name.
func (t *Telegram) Name() string { return t.name }

// Send posts m to the configured chat.
func (t *Telegram) Send(ctx context.Context, m Message) error {
	body, err := json.Marshal(map[string]any{
		"chat_id":                  t.chatID,
		"text":                     renderTelegram(m),
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	})
	if err != nil {
		return fmt.Errorf("telegram %s: %w", t.name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		t.baseURL+"/bot"+t.token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return t.redact(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.hc.Do(req)
	if err != nil {
		return t.redact(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var r struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	jerr := json.Unmarshal(raw, &r)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		desc := r.Description
		if desc == "" {
			desc = strings.TrimSpace(string(raw))
		}
		return t.redact(fmt.Errorf("telegram %s: HTTP %d: %s", t.name, resp.StatusCode, desc))
	}
	if jerr != nil {
		return t.redact(fmt.Errorf("telegram %s: bad response: %w", t.name, jerr))
	}
	if !r.OK {
		return t.redact(fmt.Errorf("telegram %s: not ok: %s", t.name, r.Description))
	}
	return nil
}

// redactedError hides the bot token, which is part of the request URL and
// therefore appears in *url.Error messages.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

func (t *Telegram) redact(err error) error {
	inner := err
	var ue *url.Error
	if errors.As(err, &ue) {
		inner = ue.Err // keep context.DeadlineExceeded etc. reachable via errors.Is
	}
	msg := strings.ReplaceAll(err.Error(), t.token, "<redacted>")
	if !strings.HasPrefix(msg, "telegram ") {
		msg = "telegram " + t.name + ": " + msg
	}
	return &redactedError{msg: msg, err: inner}
}

// renderTelegram builds the HTML text: bold title, one line per entry, then
// the links. If the result exceeds Telegram's limit, trailing lines are
// replaced by "…(+N more)".
func renderTelegram(m Message) string {
	head := "<b>" + html.EscapeString(m.Title) + "</b>"
	var links string
	if len(m.Links) > 0 {
		parts := make([]string, len(m.Links))
		for i, l := range m.Links {
			parts[i] = `<a href="` + html.EscapeString(l.URL) + `">` + html.EscapeString(l.Text) + "</a>"
		}
		links = strings.Join(parts, " | ")
	}
	lines := make([]string, len(m.Lines))
	for i, l := range m.Lines {
		lines[i] = html.EscapeString(l)
	}

	build := func(n int) string {
		parts := []string{head}
		parts = append(parts, lines[:n]...)
		if n < len(lines) {
			parts = append(parts, fmt.Sprintf("…(+%d more)", len(lines)-n))
		}
		if links != "" {
			parts = append(parts, links)
		}
		return strings.Join(parts, "\n")
	}

	n := len(lines)
	text := build(n)
	for utf8.RuneCountInString(text) > telegramMaxChars && n > 0 {
		n--
		text = build(n)
	}
	if utf8.RuneCountInString(text) > telegramMaxChars {
		// Title/links alone are too long: hard cut (may break HTML, but
		// this is far beyond any realistic message).
		text = string([]rune(text)[:telegramMaxChars])
	}
	return text
}
