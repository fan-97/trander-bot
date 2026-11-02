package notify

import (
	"context"
	"crypto/tls"
	"errors"
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
	insecure                                   bool
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
		password: p["password"], from: p["from"], to: to, insecure: p["insecure"] == "true", now: time.Now}, nil
}

// Name returns the configured channel name.
func (e *Email) Name() string { return e.name }

// smtpTLSPolicy decides whether the session may proceed. Port 465 is
// implicitly TLS. Otherwise STARTTLS must be advertised unless the host is
// loopback or insecure is set explicitly.
func smtpTLSPolicy(host, port string, hasStartTLS, insecure bool) error {
	if port == "465" || hasStartTLS || insecure {
		return nil
	}
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return nil
	}
	return errors.New("server does not advertise STARTTLS; refusing to send credentials in cleartext (set insecure=true to override)")
}

// plainAuth is PLAIN auth that, unlike smtp.PlainAuth, does not refuse
// unencrypted connections; smtpTLSPolicy has already vetted the transport.
type plainAuth struct{ user, pass string }

func (a plainAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "PLAIN", []byte("\x00" + a.user + "\x00" + a.pass), nil
}

func (a plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, errors.New("unexpected server challenge")
	}
	return nil, nil
}

// buildEmailAt renders the RFC 5322 message with CRLF line endings.
func buildEmailAt(m Message, from string, to []string, at time.Time) []byte {
	subject := strings.NewReplacer("\r", " ", "\n", " ").Replace(m.Title)
	body := strings.ReplaceAll(renderText(m), "\r\n", "\n")
	body = strings.ReplaceAll(body, "\n", "\r\n")
	var b strings.Builder
	b.WriteString("From: " + from + "\r\n")
	b.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("Date: " + at.Format(time.RFC1123Z) + "\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	b.WriteString("\r\n")
	return []byte(b.String())
}

// Send delivers m, honouring ctx for dialing and for the whole exchange.
func (e *Email) Send(ctx context.Context, m Message) error {
	if err := e.send(ctx, m); err != nil {
		return fmt.Errorf("email %s: %w", e.name, redactURLError(err, e.password))
	}
	return nil
}

func (e *Email) send(ctx context.Context, m Message) (err error) {
	ctx, cancel := context.WithTimeout(ctx, emailTimeout)
	defer cancel()
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
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
	hasTLS, _ := c.Extension("STARTTLS")
	if err := smtpTLSPolicy(e.host, e.port, hasTLS, e.insecure); err != nil {
		return err
	}
	if e.port != "465" && hasTLS {
		if err := c.StartTLS(cfg); err != nil {
			return err
		}
	}
	if ok, _ := c.Extension("AUTH"); !ok {
		return errors.New("server does not advertise AUTH")
	}
	if err := c.Auth(plainAuth{e.username, e.password}); err != nil {
		return err
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
