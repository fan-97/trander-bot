package notify

import (
	"context"
	"crypto/tls"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"strings"
	"time"
)

const emailTimeout = 30 * time.Second

// Email sends plain-text mail over SMTP (STARTTLS when offered; implicit TLS
// on port 465).
type Email struct {
	name, host, port, username, password, from string
	to                                         []string
	now                                        func() time.Time
}

func newEmail(name string, p map[string]string) (*Email, error) {
	if err := requireParams(name, "email", p, "host", "port", "username", "password", "from", "to"); err != nil {
		return nil, err
	}
	var to []string
	for _, a := range strings.Split(p["to"], ",") {
		if a = strings.TrimSpace(a); a != "" {
			to = append(to, a)
		}
	}
	if len(to) == 0 {
		return nil, fmt.Errorf("channel %q: email to is required", name)
	}
	return &Email{name: name, host: p["host"], port: p["port"], username: p["username"],
		password: p["password"], from: p["from"], to: to, now: time.Now}, nil
}

// Name returns the configured channel name.
func (e *Email) Name() string { return e.name }

// buildEmail renders the RFC 5322 message with CRLF line endings.
func buildEmail(m Message, from string, to []string) []byte {
	return buildEmailAt(m, from, to, time.Now())
}

func buildEmailAt(m Message, from string, to []string, at time.Time) []byte {
	subject := strings.NewReplacer("\r", " ", "\n", " ").Replace(m.Title)
	var b strings.Builder
	b.WriteString("From: " + from + "\r\n")
	b.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("Date: " + at.Format(time.RFC1123Z) + "\r\n")
	b.WriteString("\r\n")
	b.WriteString(strings.ReplaceAll(renderText(m), "\n", "\r\n"))
	b.WriteString("\r\n")
	return []byte(b.String())
}

// Send delivers m, honouring ctx for dialing and for the whole exchange.
func (e *Email) Send(ctx context.Context, m Message) error {
	if err := e.send(ctx, m); err != nil {
		return fmt.Errorf("email %s: %w", e.name, redactURLError(err, e.password, e.username))
	}
	return nil
}

func (e *Email) send(ctx context.Context, m Message) error {
	ctx, cancel := context.WithTimeout(ctx, emailTimeout)
	defer cancel()
	addr := net.JoinHostPort(e.host, e.port)
	d := &net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	cfg := &tls.Config{ServerName: e.host, MinVersion: tls.VersionTLS12}
	if e.port == "465" {
		conn = tls.Client(conn, cfg)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	// Closing the conn on ctx cancel unblocks any pending read/write.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	c, err := smtp.NewClient(conn, e.host)
	if err != nil {
		conn.Close()
		return err
	}
	defer c.Close()
	if e.port != "465" {
		if ok, _ := c.Extension("STARTTLS"); ok {
			if err := c.StartTLS(cfg); err != nil {
				return err
			}
		}
	}
	if ok, _ := c.Extension("AUTH"); ok {
		if err := c.Auth(smtp.PlainAuth("", e.username, e.password, e.host)); err != nil {
			return err
		}
	}
	if err := c.Mail(e.from); err != nil {
		return err
	}
	for _, r := range e.to {
		if err := c.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(buildEmailAt(m, e.from, e.to, e.now())); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
