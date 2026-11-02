package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"strings"
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
	t.hc = newHTTPClient(t.hc)
	return t, nil
}

// Name returns the configured channel name.
func (t *Telegram) Name() string { return t.name }

// Send posts m to the configured chat.
func (t *Telegram) Send(ctx context.Context, m Message) error {
	body := map[string]any{
		"chat_id":                  t.chatID,
		"text":                     renderTelegram(m),
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	err := postJSON(ctx, t.hc, t.baseURL+"/bot"+t.token+"/sendMessage", body, func(status int, raw []byte) error {
		var r struct {
			OK          bool   `json:"ok"`
			Description string `json:"description"`
		}
		jerr := json.Unmarshal(raw, &r)
		if status < 200 || status > 299 {
			desc := r.Description
			if desc == "" {
				desc = strings.TrimSpace(string(raw))
			}
			return fmt.Errorf("telegram %s: HTTP %d: %s", t.name, status, desc)
		}
		if jerr != nil {
			return fmt.Errorf("telegram %s: bad response: %w", t.name, jerr)
		}
		if !r.OK {
			return fmt.Errorf("telegram %s: not ok: %s", t.name, r.Description)
		}
		return nil
	})
	if err != nil {
		return t.redact(err)
	}
	return nil
}

func (t *Telegram) redact(err error) error {
	r := redactURLError(err, t.token)
	if !strings.HasPrefix(r.Error(), "telegram ") {
		return &redactedError{msg: "telegram " + t.name + ": " + r.Error(), err: errors.Unwrap(r)}
	}
	return r
}

// renderTelegram builds the HTML text: bold title, one line per entry, then
// the links. It never slices HTML, so the result always parses:
//   - links are capped at telegramLinkBudget (1000) runes; omitted ones become
//     "+N more symbols" (N = distinct symbols, the last word of a link text);
//   - if still over the limit, trailing lines are replaced by "…(+N more)";
//   - if the title alone is still too long, the plain title is cut before
//     escaping.
func renderTelegram(m Message) string {
	links := renderLinks(m.Links)
	lines := make([]string, len(m.Lines))
	for i, l := range m.Lines {
		lines[i] = html.EscapeString(l)
	}

	build := func(title string, n int) string {
		parts := []string{"<b>" + html.EscapeString(title) + "</b>"}
		parts = append(parts, lines[:n]...)
		if n < len(lines) {
			parts = append(parts, fmt.Sprintf("…(+%d more)", len(lines)-n))
		}
		if links != "" {
			parts = append(parts, links)
		}
		return strings.Join(parts, "\n")
	}

	// Drop trailing lines until the text fits (quadratic in the worst case,
	// which is negligible for batch-sized inputs).
	n := len(lines)
	size := utf8.RuneCountInString(build(m.Title, n))
	for size > telegramMaxChars && n > 0 {
		n--
		size = utf8.RuneCountInString(build(m.Title, n))
	}
	if size <= telegramMaxChars {
		return build(m.Title, n)
	}

	// Only the title can still be too long. Escaping never shortens text,
	// so cutting `excess` raw runes removes at least `excess` output runes.
	title := []rune(m.Title)
	for size > telegramMaxChars && len(title) > 0 {
		cut := min(size-telegramMaxChars+1, len(title)) // +1 makes room for "…"
		title = title[:len(title)-cut]
		size = utf8.RuneCountInString(build(string(title)+"…", n))
	}
	return build(string(title)+"…", n)
}

// telegramLinkBudget caps the rendered link block so a market-wide batch
// still leaves room for the signal lines.
const telegramLinkBudget = 1000

// renderLinks renders links joined by " | ", stopping before the block would
// exceed telegramLinkBudget runes and appending "+N more symbols" for the
// rest. Each link is rendered whole, so the HTML is always balanced.
func renderLinks(ls []Link) string {
	if len(ls) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ls))
	size := 0
	k := 0
	for ; k < len(ls); k++ {
		p := `<a href="` + html.EscapeString(ls[k].URL) + `">` + html.EscapeString(ls[k].Text) + "</a>"
		add := utf8.RuneCountInString(p)
		if k > 0 {
			add += len(" | ")
		}
		// Reserve room for the "+N more symbols" marker (< 40 runes).
		if size+add > telegramLinkBudget-40 {
			break
		}
		parts = append(parts, p)
		size += add
	}
	out := strings.Join(parts, " | ")
	if k < len(ls) {
		syms := make(map[string]bool)
		for _, l := range ls[k:] {
			f := strings.Fields(l.Text)
			if len(f) > 0 {
				syms[f[len(f)-1]] = true
			}
		}
		marker := fmt.Sprintf("+%d more symbols", len(syms))
		if out == "" {
			return marker
		}
		out += " | " + marker
	}
	return out
}
