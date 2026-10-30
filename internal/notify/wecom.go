package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// wecomMaxBytes is WeCom's markdown content limit.
const wecomMaxBytes = 4096

// Wecom sends markdown messages to a WeCom group robot webhook.
type Wecom struct {
	name, url string
	hc        *http.Client
}

func newWecom(name string, p map[string]string, hc *http.Client) (*Wecom, error) {
	if err := requireParams(name, "wecom", p, "url"); err != nil {
		return nil, err
	}
	return &Wecom{name: name, url: p["url"], hc: newHTTPClient(hc)}, nil
}

// Name returns the configured channel name.
func (w *Wecom) Name() string { return w.name }

func renderWecom(m Message) string {
	parts := []string{"**" + m.Title + "**"}
	parts = append(parts, m.Lines...)
	for _, l := range m.Links {
		parts = append(parts, "["+l.Text+"]("+l.URL+")")
	}
	return truncateBytes(strings.Join(parts, "\n"), wecomMaxBytes)
}

// Send posts m; a non-zero errcode in the JSON reply is an error.
func (w *Wecom) Send(ctx context.Context, m Message) error {
	body := map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": renderWecom(m)}}
	if err := postJSON(ctx, w.hc, w.url, body, checkErrcode); err != nil {
		return fmt.Errorf("wecom %s: %w", w.name, err)
	}
	return nil
}

// checkErrcode handles the {"errcode":N,"errmsg":"..."} reply shared by
// WeCom and DingTalk.
func checkErrcode(status int, raw []byte) error {
	if err := check2xx(status, raw); err != nil {
		return err
	}
	var r struct {
		Errcode int    `json:"errcode"`
		Errmsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("bad response: %w", err)
	}
	if r.Errcode != 0 {
		return fmt.Errorf("errcode %d: %s", r.Errcode, r.Errmsg)
	}
	return nil
}
