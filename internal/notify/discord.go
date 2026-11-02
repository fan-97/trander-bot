package notify

import (
	"context"
	"fmt"
	"net/http"
)

// discordMaxChars is Discord's message content limit.
const discordMaxChars = 2000

// Discord posts plain-text messages to a Discord webhook URL.
type Discord struct {
	name, url string
	hc        *http.Client
}

func newDiscord(name string, p map[string]string, hc *http.Client) (*Discord, error) {
	if err := requireParams(name, "discord", p, "url"); err != nil {
		return nil, err
	}
	if err := validateURL(name, "discord", p["url"]); err != nil {
		return nil, err
	}
	return &Discord{name: name, url: p["url"], hc: newHTTPClient(hc)}, nil
}

// Name returns the configured channel name.
func (d *Discord) Name() string { return d.name }

// Send posts m; Discord answers 204 on success.
func (d *Discord) Send(ctx context.Context, m Message) error {
	body := map[string]string{"content": truncateRunes(renderText(m), discordMaxChars)}
	if err := postJSON(ctx, d.hc, d.url, body, check2xx); err != nil {
		return fmt.Errorf("discord %s: %w", d.name, err)
	}
	return nil
}
