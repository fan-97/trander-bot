package notify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"trader-bot/internal/config"
)

const secretTok = "SECRET-TOKEN-xyz"

var testMsg = Message{
	Title: "[WARN] 1 signal(s)",
	Lines: []string{"[1h] BTCUSDT rsi @ 100 | oversold"},
	Level: config.LevelWarn,
	Links: []Link{{Text: "Binance BTCUSDT", URL: "https://b.example/BTCUSDT"}},
}

type captured struct {
	path, query string
	body        []byte
}

func server(t *testing.T, status int, resp string) (*httptest.Server, *captured) {
	t.Helper()
	c := &captured{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.path, c.query = r.URL.Path, r.URL.RawQuery
		c.body, _ = io.ReadAll(r.Body)
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("content-type %q", ct)
		}
		w.WriteHeader(status)
		io.WriteString(w, resp)
	}))
	t.Cleanup(s.Close)
	return s, c
}

func mk(t *testing.T, typ string, p map[string]string) Notifier {
	t.Helper()
	n, err := NewFromConfig("ch", config.ChannelConfig{Type: typ, Params: p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDiscord(t *testing.T) {
	s, c := server(t, 204, "")
	n := mk(t, "discord", map[string]string{"url": s.URL + "/api/webhooks/1/" + secretTok})
	if err := n.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	var b map[string]string
	if err := json.Unmarshal(c.body, &b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b["content"], "BTCUSDT rsi") || !strings.Contains(b["content"], "Binance BTCUSDT: https://b.example/BTCUSDT") {
		t.Errorf("content %q", b["content"])
	}
}

func TestDiscord_TruncatesTo2000Chars(t *testing.T) {
	s, c := server(t, 204, "")
	n := mk(t, "discord", map[string]string{"url": s.URL})
	m := Message{Title: "t", Lines: []string{strings.Repeat("信", 3000)}}
	if err := n.Send(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	var b map[string]string
	_ = json.Unmarshal(c.body, &b)
	if got := utf8.RuneCountInString(b["content"]); got != 2000 || !strings.HasSuffix(b["content"], "…") {
		t.Errorf("runes=%d suffix ok=%v", got, strings.HasSuffix(b["content"], "…"))
	}
}

func TestDiscord_HTTPErrorRedacted(t *testing.T) {
	s, _ := server(t, 400, "bad")
	n := mk(t, "discord", map[string]string{"url": s.URL + "/" + secretTok})
	err := n.Send(context.Background(), testMsg)
	if err == nil || strings.Contains(err.Error(), secretTok) || !strings.Contains(err.Error(), "400") {
		t.Fatalf("err=%v", err)
	}
}

func TestWebhook(t *testing.T) {
	s, c := server(t, 200, "ok")
	n := mk(t, "webhook", map[string]string{"url": s.URL + "/hook"})
	if err := n.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	var b struct {
		Title string                       `json:"title"`
		Level string                       `json:"level"`
		Lines []string                     `json:"lines"`
		Links []struct{ Text, URL string } `json:"links"`
	}
	if err := json.Unmarshal(c.body, &b); err != nil {
		t.Fatal(err)
	}
	if b.Title != testMsg.Title || b.Level != "warn" || len(b.Lines) != 1 || len(b.Links) != 1 || b.Links[0].URL != "https://b.example/BTCUSDT" {
		t.Errorf("body %s", c.body)
	}
	if c.path != "/hook" {
		t.Errorf("path %s", c.path)
	}
}

func TestWebhook_Non2xx(t *testing.T) {
	s, _ := server(t, 500, "boom")
	n := mk(t, "webhook", map[string]string{"url": s.URL})
	if err := n.Send(context.Background(), testMsg); err == nil {
		t.Fatal("want error")
	}
}

func TestWecom(t *testing.T) {
	s, c := server(t, 200, `{"errcode":0,"errmsg":"ok"}`)
	n := mk(t, "wecom", map[string]string{"url": s.URL + "/send?key=" + secretTok})
	if err := n.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	var b struct {
		Msgtype  string
		Markdown struct{ Content string }
	}
	if err := json.Unmarshal(c.body, &b); err != nil {
		t.Fatal(err)
	}
	if b.Msgtype != "markdown" || !strings.Contains(b.Markdown.Content, "**[WARN] 1 signal(s)**") ||
		!strings.Contains(b.Markdown.Content, "[Binance BTCUSDT](https://b.example/BTCUSDT)") {
		t.Errorf("body %s", c.body)
	}
}

func TestWecom_ErrcodeAndLimit(t *testing.T) {
	s, _ := server(t, 200, `{"errcode":93000,"errmsg":"invalid webhook url"}`)
	n := mk(t, "wecom", map[string]string{"url": s.URL + "?key=" + secretTok})
	err := n.Send(context.Background(), testMsg)
	if err == nil || !strings.Contains(err.Error(), "invalid webhook url") || strings.Contains(err.Error(), secretTok) {
		t.Fatalf("err=%v", err)
	}
	s2, c := server(t, 200, `{"errcode":0}`)
	n2 := mk(t, "wecom", map[string]string{"url": s2.URL})
	if err := n2.Send(context.Background(), Message{Title: "t", Lines: []string{strings.Repeat("信", 3000)}}); err != nil {
		t.Fatal(err)
	}
	var b struct{ Markdown struct{ Content string } }
	_ = json.Unmarshal(c.body, &b)
	if len(b.Markdown.Content) > 4096 || !utf8.ValidString(b.Markdown.Content) {
		t.Errorf("len=%d", len(b.Markdown.Content))
	}
}

// Vector computed independently:
//
//	python3 -I -c 'import hmac,hashlib,base64,urllib.parse;ts="1700000000000";s="SECtest";
//	d=hmac.new(s.encode(),(ts+"\n"+s).encode(),hashlib.sha256).digest();
//	print(urllib.parse.quote_plus(base64.b64encode(d).decode()))'
func TestDingtalkSign(t *testing.T) {
	const want = "aZLLrriXgn05YbwaGR7knYsLeJADjr9NwLaNNKpxh4g%3D"
	if got := dingtalkSign("SECtest", "1700000000000"); got != want {
		t.Errorf("sign=%s want %s", got, want)
	}
}

func TestDingtalk(t *testing.T) {
	s, c := server(t, 200, `{"errcode":0,"errmsg":"ok"}`)
	n := mk(t, "dingtalk", map[string]string{"url": s.URL + "/robot/send?access_token=" + secretTok, "secret": "SECtest"})
	n.(*Dingtalk).now = func() time.Time { return time.UnixMilli(1700000000000) }
	if err := n.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	q, _ := url.ParseQuery(c.query)
	if q.Get("access_token") != secretTok || q.Get("timestamp") != "1700000000000" ||
		q.Get("sign") != "aZLLrriXgn05YbwaGR7knYsLeJADjr9NwLaNNKpxh4g=" {
		t.Errorf("query %s", c.query)
	}
	var b struct {
		Msgtype string
		Text    struct{ Content string }
	}
	_ = json.Unmarshal(c.body, &b)
	if b.Msgtype != "text" || !strings.Contains(b.Text.Content, "BTCUSDT rsi") {
		t.Errorf("body %s", c.body)
	}
}

func TestDingtalk_ErrcodeRedacted(t *testing.T) {
	s, _ := server(t, 200, `{"errcode":310000,"errmsg":"sign not match"}`)
	n := mk(t, "dingtalk", map[string]string{"url": s.URL + "?access_token=" + secretTok, "secret": "SECtest"})
	err := n.Send(context.Background(), testMsg)
	if err == nil || !strings.Contains(err.Error(), "sign not match") || strings.Contains(err.Error(), secretTok) {
		t.Fatalf("err=%v", err)
	}
}

func TestDingtalk_TransportErrorRedacted(t *testing.T) {
	s, _ := server(t, 200, "")
	u := s.URL + "?access_token=" + secretTok
	s.Close()
	n := mk(t, "dingtalk", map[string]string{"url": u, "secret": "SECtest"})
	err := n.Send(context.Background(), testMsg)
	if err == nil || strings.Contains(err.Error(), secretTok) || strings.Contains(err.Error(), "SECtest") {
		t.Fatalf("err=%v", err)
	}
}

// Vector computed independently:
//
//	python3 -I -c 'import hmac,hashlib,base64;ts="1700000000";s="SECtest";
//	print(base64.b64encode(hmac.new((ts+"\n"+s).encode(),b"",hashlib.sha256).digest()).decode())'
func TestFeishuSign(t *testing.T) {
	const want = "G7XpBpG8NgG02fJOAhX6FRAObIljmFoxVReo8I62pEk="
	if got := feishuSign("SECtest", "1700000000"); got != want {
		t.Errorf("sign=%s want %s", got, want)
	}
}

func TestFeishu_SignedAndUnsigned(t *testing.T) {
	s, c := server(t, 200, `{"code":0,"msg":"success"}`)
	n := mk(t, "feishu", map[string]string{"url": s.URL + "/hook/" + secretTok, "secret": "SECtest"})
	n.(*Feishu).now = func() time.Time { return time.Unix(1700000000, 0) }
	if err := n.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	var b struct {
		Timestamp, Sign string
		MsgType         string `json:"msg_type"`
		Content         struct{ Text string }
	}
	_ = json.Unmarshal(c.body, &b)
	if b.Timestamp != "1700000000" || b.Sign != "G7XpBpG8NgG02fJOAhX6FRAObIljmFoxVReo8I62pEk=" ||
		b.MsgType != "text" || !strings.Contains(b.Content.Text, "BTCUSDT rsi") {
		t.Errorf("body %s", c.body)
	}
	n2 := mk(t, "feishu", map[string]string{"url": s.URL})
	if err := n2.Send(context.Background(), testMsg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(c.body), `"sign":`) || strings.Contains(string(c.body), `"timestamp":`) {
		t.Errorf("unsigned body has sign: %s", c.body)
	}
}

func TestFeishu_CodeErrors(t *testing.T) {
	for _, resp := range []string{`{"code":19021,"msg":"sign match fail"}`, `{"StatusCode":19021,"msg":"sign match fail"}`} {
		s, _ := server(t, 200, resp)
		n := mk(t, "feishu", map[string]string{"url": s.URL + "/" + secretTok})
		err := n.Send(context.Background(), testMsg)
		if err == nil || !strings.Contains(err.Error(), "sign match fail") || strings.Contains(err.Error(), secretTok) {
			t.Errorf("%s: err=%v", resp, err)
		}
	}
}

func TestEmail_BuildsMessage(t *testing.T) {
	m := Message{Title: "[WARN] 信号 1", Lines: []string{"line1"}, Links: testMsg.Links}
	raw := string(buildEmailAt(m, "bot@example.com", []string{"a@example.com", "b@example.com"}, fixedT))
	for _, want := range []string{
		"From: bot@example.com\r\n", "To: a@example.com, b@example.com\r\n",
		"Subject: =?utf-8?q?", "MIME-Version: 1.0\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n", "Date: ", "\r\n\r\n",
		"line1", "Binance BTCUSDT: https://b.example/BTCUSDT",
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("missing %q in\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "Subject: [WARN] 信号") {
		t.Error("subject not encoded")
	}
	ascii := string(buildEmailAt(Message{Title: "plain"}, "a@b.c", []string{"d@e.f"}, fixedT))
	if !strings.Contains(ascii, "Subject: plain\r\n") {
		t.Errorf("ascii subject: %s", ascii)
	}
}

func TestEmail_ConstructorErrors(t *testing.T) {
	full := map[string]string{"host": "h", "port": "587", "username": "u", "password": "PW-secret", "from": "a@b.c", "to": "d@e.f, g@h.i"}
	if _, err := NewFromConfig("e", config.ChannelConfig{Type: "email", Params: full}, nil); err != nil {
		t.Fatal(err)
	}
	for k := range full {
		p := map[string]string{}
		for kk, v := range full {
			if kk != k {
				p[kk] = v
			}
		}
		_, err := NewFromConfig("e", config.ChannelConfig{Type: "email", Params: p}, nil)
		if err == nil || !strings.Contains(err.Error(), k) || strings.Contains(err.Error(), "PW-secret") {
			t.Errorf("missing %s: err=%v", k, err)
		}
	}
}

func TestMissingURLParams(t *testing.T) {
	for _, typ := range []string{"discord", "webhook", "wecom", "dingtalk", "feishu"} {
		p := map[string]string{"secret": "s"}
		_, err := NewFromConfig("c", config.ChannelConfig{Type: typ, Params: p}, nil)
		if err == nil || !strings.Contains(err.Error(), "url") {
			t.Errorf("%s: err=%v", typ, err)
		}
	}
	if _, err := NewFromConfig("c", config.ChannelConfig{Type: "dingtalk", Params: map[string]string{"url": "http://x"}}, nil); err == nil || !strings.Contains(err.Error(), "secret") {
		t.Errorf("dingtalk secret: %v", err)
	}
}

func TestNewFromConfig_UnknownType(t *testing.T) {
	_, err := NewFromConfig("x", config.ChannelConfig{Type: "carrier-pigeon"}, nil)
	if err == nil || !errors.Is(err, ErrUnknownChannelType) {
		t.Fatalf("err=%v", err)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncateBytes("a信b", 3); got != "a" {
		t.Errorf("got %q", got)
	}
	if got := truncateRunes("abcdef", 4); got != "abc…" {
		t.Errorf("got %q", got)
	}
}

var fixedT = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

func echoServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, "bad request "+r.URL.String())
	}))
	t.Cleanup(s.Close)
	return s
}

func TestDiscord_EchoedURLRedacted(t *testing.T) {
	s := echoServer(t)
	n := mk(t, "discord", map[string]string{"url": s.URL + "/api/webhooks/1/" + secretTok})
	err := n.Send(context.Background(), testMsg)
	if err == nil || strings.Contains(err.Error(), secretTok) {
		t.Fatalf("err=%v", err)
	}
}

func TestDingtalk_EchoedSignedURLRedacted(t *testing.T) {
	s := echoServer(t)
	n := mk(t, "dingtalk", map[string]string{"url": s.URL + "/robot/send?access_token=" + secretTok, "secret": "SECtest"})
	n.(*Dingtalk).now = func() time.Time { return time.UnixMilli(1700000000000) }
	err := n.Send(context.Background(), testMsg)
	if err == nil {
		t.Fatal("want error")
	}
	for _, leak := range []string{secretTok, "1700000000000", "aZLLrriXgn05YbwaGR7knYsLeJADjr9NwLaNNKpxh4g", "access_token=" + secretTok} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("leaked %q: %v", leak, err)
		}
	}
}

func TestURLWithControlCharRejected(t *testing.T) {
	bad := "https://example.com/hook/" + secretTok + "\n"
	for _, typ := range []string{"discord", "webhook", "wecom", "dingtalk", "feishu"} {
		_, err := NewFromConfig("c", config.ChannelConfig{Type: typ, Params: map[string]string{"url": bad, "secret": "s"}}, nil)
		if err == nil || strings.Contains(err.Error(), secretTok) || !strings.Contains(err.Error(), "url") {
			t.Errorf("%s: err=%v", typ, err)
		}
	}
	// postJSON itself must not echo the URL on request-construction errors.
	err := postJSON(context.Background(), http.DefaultClient, bad, map[string]string{}, check2xx)
	if err == nil || strings.Contains(err.Error(), secretTok) {
		t.Errorf("postJSON err=%v", err)
	}
}

func TestSerializedBodyLimits(t *testing.T) {
	m := Message{Title: "t", Lines: []string{strings.Repeat("<", 40000)}}
	s, c := server(t, 200, `{"errcode":0,"code":0}`)
	for _, tc := range []struct {
		typ   string
		limit int
	}{{"dingtalk", dingtalkMaxBytes}, {"feishu", feishuMaxBytes}} {
		n := mk(t, tc.typ, map[string]string{"url": s.URL, "secret": "SECtest"})
		if err := n.Send(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		if len(c.body) > tc.limit || !json.Valid(c.body) {
			t.Errorf("%s: body %d bytes (limit %d)", tc.typ, len(c.body), tc.limit)
		}
	}
}

func TestSMTPTLSPolicy(t *testing.T) {
	cases := []struct {
		host, port             string
		starttls, insecure, ok bool
	}{
		{"smtp.example.com", "587", true, false, true},
		{"smtp.example.com", "587", false, false, false},
		{"smtp.example.com", "465", false, false, true},
		{"smtp.example.com", "25", false, true, true},
		{"localhost", "25", false, false, true},
		{"127.0.0.1", "25", false, false, true},
		{"::1", "25", false, false, true},
	}
	for _, c := range cases {
		if err := smtpTLSPolicy(c.host, c.port, c.starttls, c.insecure); (err == nil) != c.ok {
			t.Errorf("%+v: err=%v", c, err)
		}
	}
}

func TestEmail_CRLFNormalized(t *testing.T) {
	raw := string(buildEmailAt(Message{Title: "t", Lines: []string{"a\r\nb"}}, "a@b.c", []string{"d@e.f"}, fixedT))
	if strings.Contains(raw, "\r\r") || !strings.Contains(raw, "a\r\nb") || !strings.Contains(raw, "Date: Fri, 30 Oct 2026 12:00:00 +0000") {
		t.Errorf("%q", raw)
	}
}
