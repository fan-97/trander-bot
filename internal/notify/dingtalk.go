package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// dingtalkMaxBytes is DingTalk's request body limit.
const dingtalkMaxBytes = 20000

// Dingtalk sends text messages to a signed DingTalk group robot webhook.
type Dingtalk struct {
	name, url, secret string
	hc                *http.Client
	now               func() time.Time // request signing timestamp, not business time
}

func newDingtalk(name string, p map[string]string, hc *http.Client) (*Dingtalk, error) {
	if err := requireParams(name, "dingtalk", p, "url", "secret"); err != nil {
		return nil, err
	}
	if err := validateURL(name, "dingtalk", p["url"]); err != nil {
		return nil, err
	}
	return &Dingtalk{name: name, url: p["url"], secret: p["secret"], hc: newHTTPClient(hc), now: time.Now}, nil
}

// Name returns the configured channel name.
func (d *Dingtalk) Name() string { return d.name }

// dingtalkSign returns the URL-encoded base64(HMAC-SHA256(key=secret,
// msg=timestamp+"\n"+secret)); timestamp is milliseconds as a string.
func dingtalkSign(secret, timestamp string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(timestamp + "\n" + secret))
	return url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
}

// Send posts m with timestamp and sign appended to the webhook URL.
func (d *Dingtalk) Send(ctx context.Context, m Message) error {
	ts := strconv.FormatInt(d.now().UnixMilli(), 10)
	sep := "&"
	if !strings.Contains(d.url, "?") {
		sep = "?"
	}
	full := d.url + sep + "timestamp=" + ts + "&sign=" + dingtalkSign(d.secret, ts)
	body := fitJSON(renderText(m), dingtalkMaxBytes, func(t string) any {
		return map[string]any{"msgtype": "text", "text": map[string]string{"content": t}}
	})
	if err := postJSON(ctx, d.hc, full, body, checkErrcode); err != nil {
		return fmt.Errorf("dingtalk %s: %w", d.name, redactURLError(err, d.secret))
	}
	return nil
}
