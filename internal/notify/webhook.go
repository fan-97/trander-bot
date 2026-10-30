package notify

import (
	"context"
	"fmt"
	"net/http"
)

// Webhook POSTs the message as JSON to an arbitrary URL.
type Webhook struct {
	name, url string
	hc        *http.Client
}

func newWebhook(name string, p map[string]string, hc *http.Client) (*Webhook, error) {
	if err := requireParams(name, "webhook", p, "url"); err != nil {
		return nil, err
	}
	return &Webhook{name: name, url: p["url"], hc: newHTTPClient(hc)}, nil
}

// Name returns the configured channel name.
func (w *Webhook) Name() string { return w.name }

// Send posts {"title","level","lines","links"}; any 2xx is success.
func (w *Webhook) Send(ctx context.Context, m Message) error {
	type link struct {
		Text string `json:"text"`
		URL  string `json:"url"`
	}
	links := make([]link, 0, len(m.Links))
	for _, l := range m.Links {
		links = append(links, link{l.Text, l.URL})
	}
	lines := m.Lines
	if lines == nil {
		lines = []string{}
	}
	body := map[string]any{"title": m.Title, "level": string(m.Level), "lines": lines, "links": links}
	if err := postJSON(ctx, w.hc, w.url, body, check2xx); err != nil {
		return fmt.Errorf("webhook %s: %w", w.name, err)
	}
	return nil
}
