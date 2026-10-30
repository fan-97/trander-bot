package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// feishuMaxBytes is Feishu's request body limit.
const feishuMaxBytes = 30000

// Feishu sends text messages to a Feishu/Lark custom bot webhook.
type Feishu struct {
	name, url, secret string
	hc                *http.Client
	now               func() time.Time // request signing timestamp, not business time
}

func newFeishu(name string, p map[string]string, hc *http.Client) (*Feishu, error) {
	if err := requireParams(name, "feishu", p, "url"); err != nil {
		return nil, err
	}
	if err := validateURL(name, "feishu", p["url"]); err != nil {
		return nil, err
	}
	return &Feishu{name: name, url: p["url"], secret: p["secret"], hc: newHTTPClient(hc), now: time.Now}, nil
}

// Name returns the configured channel name.
func (f *Feishu) Name() string { return f.name }

// feishuSign returns base64(HMAC-SHA256(key=timestamp+"\n"+secret, msg=empty));
// timestamp is seconds as a string.
func feishuSign(secret, timestamp string) string {
	h := hmac.New(sha256.New, []byte(timestamp+"\n"+secret))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// Send posts m, signing the body when a secret is configured.
func (f *Feishu) Send(ctx context.Context, m Message) error {
	ts := strconv.FormatInt(f.now().Unix(), 10)
	body := fitJSON(renderText(m), feishuMaxBytes, func(t string) any {
		b := map[string]any{"msg_type": "text", "content": map[string]string{"text": t}}
		if f.secret != "" {
			b["timestamp"] = ts
			b["sign"] = feishuSign(f.secret, ts)
		}
		return b
	})
	if err := postJSON(ctx, f.hc, f.url, body, checkFeishu); err != nil {
		return fmt.Errorf("feishu %s: %w", f.name, redactURLError(err, f.secret))
	}
	return nil
}

// checkFeishu accepts {"code":0} (and the older {"StatusCode":0}); anything
// else is an error carrying msg.
func checkFeishu(status int, raw []byte) error {
	if err := check2xx(status, raw); err != nil {
		return err
	}
	var r struct {
		Code       *int   `json:"code"`
		StatusCode *int   `json:"StatusCode"`
		Msg        string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("bad response: %w", err)
	}
	code := 0
	switch {
	case r.Code != nil:
		code = *r.Code
	case r.StatusCode != nil:
		code = *r.StatusCode
	}
	if code != 0 {
		return fmt.Errorf("code %d: %s", code, r.Msg)
	}
	return nil
}
