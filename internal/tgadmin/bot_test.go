package tgadmin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseSymbols(t *testing.T) {
	syms, bad := ParseSymbols("btc, ETHUSDT，sol 1000pepe btc usdcusdt x$y")
	want := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT", "USDCUSDT"}
	if !slices.Equal(syms, want) {
		t.Fatalf("syms %v", syms)
	}
	if !slices.Equal(bad, []string{"x$y"}) {
		t.Fatalf("bad %v", bad)
	}
}

func TestRenderPanelPaging(t *testing.T) {
	var v View
	for i := 0; i < perPage+2; i++ {
		v.Manual = append(v.Manual, "S"+strings.Repeat("A", i%3)+string(rune('A'+i%26))+"USDT")
	}
	text, kb := renderPanel(v, 1, "")
	if !strings.Contains(text, "自动币种: 未开启") {
		t.Fatal(text)
	}
	// page 2: 2 symbol buttons (one row), nav row, action row
	if len(kb.Rows) != 3 || len(kb.Rows[0]) != 2 || kb.Rows[1][0].Data != "p:0" {
		t.Fatalf("rows %+v", kb.Rows)
	}
	for _, r := range kb.Rows {
		for _, b := range r {
			if len(b.Data) > 64 {
				t.Fatalf("callback data too long: %q", b.Data)
			}
		}
	}
}

type fakeBackend struct {
	mu     sync.Mutex
	manual []string
}

func (f *fakeBackend) View() (View, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return View{Manual: slices.Clone(f.manual)}, nil
}

func (f *fakeBackend) Add(_ context.Context, syms []string) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.manual = append(f.manual, syms...)
	return Result{Changed: syms, Applied: true}, nil
}

func (f *fakeBackend) Remove(_ context.Context, syms []string) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.manual = slices.DeleteFunc(f.manual, func(s string) bool { return slices.Contains(syms, s) })
	return Result{Changed: syms, Applied: true}, nil
}

// fakeTelegram serves getUpdates from a queue and records every other call.
type fakeTelegram struct {
	mu      sync.Mutex
	updates []update
	calls   []call
	nextID  int64
}

type call struct {
	method string
	params map[string]any
}

func (f *fakeTelegram) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	raw, _ := io.ReadAll(r.Body)
	var p map[string]any
	_ = json.Unmarshal(raw, &p)
	f.mu.Lock()
	defer f.mu.Unlock()
	var result any = true
	switch method {
	case "getUpdates":
		off := int64(p["offset"].(float64))
		var out []update
		for _, u := range f.updates {
			if u.UpdateID >= off {
				out = append(out, u)
			}
		}
		result = out
		if len(out) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	case "sendMessage":
		f.nextID++
		result = map[string]any{"message_id": f.nextID, "chat": map[string]any{"id": p["chat_id"]}}
		fallthrough
	default:
		f.calls = append(f.calls, call{method, p})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
}

func (f *fakeTelegram) push(u update) {
	f.mu.Lock()
	u.UpdateID = int64(len(f.updates) + 1)
	f.updates = append(f.updates, u)
	f.mu.Unlock()
}

func (f *fakeTelegram) waitCall(t *testing.T, pred func(call) bool) call {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		for _, c := range f.calls {
			if pred(c) {
				f.mu.Unlock()
				return c
			}
		}
		f.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected call not seen")
	return call{}
}

func TestBotFlow(t *testing.T) {
	tg := &fakeTelegram{}
	srv := httptest.NewServer(tg)
	defer srv.Close()
	be := &fakeBackend{manual: []string{"BTCUSDT", "ETHUSDT"}}
	bot := New(Options{Token: "TOKEN", BaseURL: srv.URL, Allow: []int64{100}, Backend: be})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bot.Run(ctx)

	now := time.Now().Unix()
	text := func(c call) string { s, _ := c.params["text"].(string); return s }

	// Unauthorized chat is refused.
	tg.push(update{Message: &message{Chat: chat{ID: 5}, From: &user{ID: 5}, Date: now, Text: "/symbols"}})
	tg.waitCall(t, func(c call) bool { return c.method == "sendMessage" && strings.Contains(text(c), "无权限") })

	// Panel lists the symbols with one button each.
	tg.push(update{Message: &message{Chat: chat{ID: 100}, From: &user{ID: 1}, Date: now, Text: "/symbols@mybot"}})
	tg.waitCall(t, func(c call) bool {
		return c.method == "sendMessage" && strings.Contains(text(c), "BTCUSDT, ETHUSDT") && c.params["reply_markup"] != nil
	})

	// Tap 添加, then send symbols as plain text.
	tg.push(update{CallbackQuery: &callbackQuery{ID: "q1", From: user{ID: 1}, Message: &message{MessageID: 1, Chat: chat{ID: 100}}, Data: "a"}})
	tg.waitCall(t, func(c call) bool { return c.method == "sendMessage" && strings.Contains(text(c), "请发送要添加的币种") })
	tg.push(update{Message: &message{Chat: chat{ID: 100}, From: &user{ID: 1}, Date: now, Text: "sol"}})
	tg.waitCall(t, func(c call) bool { return c.method == "editMessageText" && strings.Contains(text(c), "已添加 SOLUSDT（已热加载生效）") })

	// Tap a symbol, confirm, it is removed.
	tg.push(update{CallbackQuery: &callbackQuery{ID: "q2", From: user{ID: 1}, Message: &message{MessageID: 1, Chat: chat{ID: 100}}, Data: "d:ETHUSDT@0"}})
	tg.waitCall(t, func(c call) bool { return c.method == "editMessageText" && strings.Contains(text(c), "确认删除") })
	tg.push(update{CallbackQuery: &callbackQuery{ID: "q3", From: user{ID: 1}, Message: &message{MessageID: 1, Chat: chat{ID: 100}}, Data: "D:ETHUSDT@0"}})
	tg.waitCall(t, func(c call) bool { return c.method == "editMessageText" && strings.Contains(text(c), "已删除 ETHUSDT") })

	if v, _ := be.View(); !slices.Equal(v.Manual, []string{"BTCUSDT", "SOLUSDT"}) {
		t.Fatalf("manual %v", v.Manual)
	}

	// Stale messages (sent while the bot was down) are ignored.
	tg.push(update{Message: &message{Chat: chat{ID: 100}, From: &user{ID: 1}, Date: now - 3600, Text: "/del BTC"}})
	tg.push(update{Message: &message{Chat: chat{ID: 100}, From: &user{ID: 1}, Date: now, Text: "/help"}})
	tg.waitCall(t, func(c call) bool { return c.method == "sendMessage" && strings.Contains(text(c), "/cancel") })
	if v, _ := be.View(); !slices.Contains(v.Manual, "BTCUSDT") {
		t.Fatal("stale /del applied")
	}
}

func TestAPIRedactsToken(t *testing.T) {
	a := newAPI("http://127.0.0.1:1", "SECRET", nil)
	err := a.call(context.Background(), "getMe", map[string]any{}, nil)
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err %v", err)
	}
}
