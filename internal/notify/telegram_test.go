package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"trader-bot/internal/config"
)

const testToken = "123456:SECRET-token"

type tgReq struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

func tgServer(t *testing.T, status int, body string, got chan<- tgReq) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/bot"+testToken+"/sendMessage" {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		raw, _ := io.ReadAll(r.Body)
		var q tgReq
		if err := json.Unmarshal(raw, &q); err != nil {
			t.Errorf("body %q: %v", raw, err)
		}
		if got != nil {
			got <- q
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTG(t *testing.T, base string) Notifier {
	t.Helper()
	n, err := NewFromConfig("tg", config.ChannelConfig{Type: "telegram", Params: map[string]string{
		"token": testToken, "chat_id": "-1001", "base_url": base,
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTelegram_SendsHTML(t *testing.T) {
	got := make(chan tgReq, 1)
	srv := tgServer(t, 200, `{"ok":true,"result":{}}`, got)
	n := newTG(t, srv.URL)
	if n.Name() != "tg" {
		t.Fatalf("Name = %q", n.Name())
	}
	err := n.Send(context.Background(), Message{
		Title: "[WARN] 1 signal(s)",
		Lines: []string{"[5m] BTCUSDT r @ 1 | close < ema(20) & x"},
		Links: []Link{{Text: "Binance BTCUSDT", URL: "https://example.com/?a=1&b=2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := <-got
	if q.ChatID != "-1001" || q.ParseMode != "HTML" {
		t.Fatalf("chat_id=%q parse_mode=%q", q.ChatID, q.ParseMode)
	}
	for _, want := range []string{
		"<b>[WARN] 1 signal(s)</b>",
		"close &lt; ema(20) &amp; x",
		`<a href="https://example.com/?a=1&amp;b=2">Binance BTCUSDT</a>`,
	} {
		if !strings.Contains(q.Text, want) {
			t.Errorf("text missing %q:\n%s", want, q.Text)
		}
	}
}

func TestTelegram_Truncates(t *testing.T) {
	got := make(chan tgReq, 1)
	srv := tgServer(t, 200, `{"ok":true}`, got)
	n := newTG(t, srv.URL)
	lines := make([]string, 200)
	for i := range lines {
		lines[i] = strings.Repeat("x", 50)
	}
	if err := n.Send(context.Background(), Message{Title: "T", Lines: lines}); err != nil {
		t.Fatal(err)
	}
	q := <-got
	if c := utf8.RuneCountInString(q.Text); c > 4096 {
		t.Fatalf("text has %d chars", c)
	}
	if !strings.Contains(q.Text, "…(+") || !strings.HasSuffix(strings.TrimSpace(q.Text), "more)") {
		t.Fatalf("no truncation marker: ...%s", q.Text[len(q.Text)-40:])
	}
}

func TestTelegram_Errors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"not ok", 200, `{"ok":false,"description":"Bad Request: chat not found"}`, "chat not found"},
		{"non-2xx", 401, `{"ok":false,"description":"Unauthorized"}`, "Unauthorized"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := tgServer(t, c.status, c.body, nil)
			err := newTG(t, srv.URL).Send(context.Background(), Message{Title: "t"})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
			if strings.Contains(err.Error(), testToken) {
				t.Fatalf("token leaked: %v", err)
			}
		})
	}
}

func TestTelegram_TokenNotInTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	base := srv.URL
	srv.Close() // connection refused
	err := newTG(t, base).Send(context.Background(), Message{Title: "t"})
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("token leaked: %v", err)
	}
}

func TestNewFromConfig(t *testing.T) {
	if _, err := NewFromConfig("x", config.ChannelConfig{Type: "telegram", Params: map[string]string{"chat_id": "1"}}, nil); err == nil {
		t.Error("missing token: want error")
	}
	if _, err := NewFromConfig("x", config.ChannelConfig{Type: "telegram", Params: map[string]string{"token": "t"}}, nil); err == nil {
		t.Error("missing chat_id: want error")
	}
	_, err := NewFromConfig("x", config.ChannelConfig{Type: "carrier-pigeon"}, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown channel type") {
		t.Errorf("unknown type: err = %v", err)
	}
	tg, err := NewFromConfig("x", config.ChannelConfig{Type: "telegram", Params: map[string]string{"token": "t", "chat_id": "1"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tg.(*Telegram).baseURL != "https://api.telegram.org" {
		t.Errorf("default base_url = %q", tg.(*Telegram).baseURL)
	}
}
