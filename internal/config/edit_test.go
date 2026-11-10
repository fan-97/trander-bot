package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const editTail = `rules:
  - id: r
    interval: 5m
    when: close > 1
    channels: [w]
channels:
  w: {type: webhook, params: {url: "http://x"}}
`

func TestReplaceSymbolsKeepsRest(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"flow",
			"# head\nsymbols: [BTCUSDT, ETHUSDT]   # keep?\n# auto comment\n\n" + editTail,
			"# head\nsymbols: [BTCUSDT, SOLUSDT]\n# auto comment\n\n" + editTail},
		{"block",
			"symbols:\n  - BTCUSDT\n  # inner\n  - ETHUSDT\n\n# next\n" + editTail,
			"symbols: [BTCUSDT, SOLUSDT]\n\n# next\n" + editTail},
		{"last key no newline",
			editTail + "symbols: [BTCUSDT]",
			editTail + "symbols: [BTCUSDT, SOLUSDT]"},
		{"missing",
			"# head\n" + editTail,
			"# head\nsymbols: [BTCUSDT, SOLUSDT]\n" + editTail},
		{"crlf",
			strings.ReplaceAll("symbols: [BTCUSDT]\n"+editTail, "\n", "\r\n"),
			strings.ReplaceAll("symbols: [BTCUSDT, SOLUSDT]\n"+editTail, "\n", "\r\n")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := replaceSymbols([]byte(c.in), []string{"BTCUSDT", "SOLUSDT"})
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != c.want {
				t.Fatalf("got:\n%q\nwant:\n%q", out, c.want)
			}
			if _, err := Parse(out); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEditSymbols(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("symbols: [BTCUSDT]\n"+editTail), 0o640); err != nil {
		t.Fatal(err)
	}
	c, err := EditSymbols(path, func(cur []string) ([]string, error) { return append(cur, "ETHUSDT"), nil })
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.Symbols, ","); got != "BTCUSDT,ETHUSDT" {
		t.Fatalf("symbols %s", got)
	}
	got, _ := FileSymbols(path)
	if strings.Join(got, ",") != "BTCUSDT,ETHUSDT" {
		t.Fatalf("file symbols %v", got)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v", fi.Mode())
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Fatalf("temp file left behind: %v", ents)
	}

	// An edit that makes the config invalid is rejected and nothing is written.
	before, _ := os.ReadFile(path)
	if _, err := EditSymbols(path, func([]string) ([]string, error) { return nil, nil }); err == nil {
		t.Fatal("empty pool accepted")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("file changed by a rejected edit")
	}
}

func TestTelegramAdminConfig(t *testing.T) {
	base := "symbols: [BTCUSDT]\n" + editTail + `  tg: {type: telegram, params: {token: "t", chat_id: "%s"}}
`
	c, err := Parse([]byte(strings.Replace(base, "%s", "-1001", 1) + "telegram_admin: {channel: tg}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if a := c.TelegramAdmin; a == nil || !a.Enabled || len(a.Allow) != 1 || a.Allow[0] != -1001 {
		t.Fatalf("admin %+v", c.TelegramAdmin)
	}
	c, err = Parse([]byte(strings.Replace(base, "%s", "-1001", 1) + "telegram_admin: {channel: tg, allow: [\"42\", \"7\"]}\n"))
	if err != nil || len(c.TelegramAdmin.Allow) != 2 {
		t.Fatalf("allow: %v %+v", err, c.TelegramAdmin)
	}
	for _, bad := range []string{
		"telegram_admin: {channel: w}\n",
		"telegram_admin: {channel: nope}\n",
		"telegram_admin: {channel: tg, allow: [abc]}\n",
	} {
		if _, err := Parse([]byte(strings.Replace(base, "%s", "-1001", 1) + bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := Parse([]byte(strings.Replace(base, "%s", "@chan", 1) + "telegram_admin: {channel: tg}\n")); err == nil {
		t.Error("non-numeric chat_id without allow accepted")
	}
}
