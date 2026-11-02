// Package notify formats signal batches into messages and delivers them to
// notification channels (Telegram, ...) through a non-blocking Dispatcher.
package notify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trader-bot/internal/config"
	"trader-bot/internal/signal"
)

// Message is a channel-agnostic notification.
type Message struct {
	Title string
	Lines []string
	Level config.Level
	Links []Link
}

// Link is a labelled URL attached to a Message.
type Link struct{ Text, URL string }

// Notifier delivers messages to one configured channel.
type Notifier interface {
	Name() string
	Send(ctx context.Context, m Message) error
}

// ErrUnknownChannelType is returned by NewFromConfig for unsupported types.
var ErrUnknownChannelType = errors.New("unknown channel type")

// NewFromConfig builds the Notifier for channel name from its config.
// hc may be nil, in which case a client with a 15s timeout is used.
func NewFromConfig(name string, c config.ChannelConfig, hc *http.Client) (Notifier, error) {
	switch c.Type {
	case "telegram":
		return newTelegram(name, c.Params, hc)
	case "discord":
		return newDiscord(name, c.Params, hc)
	case "webhook":
		return newWebhook(name, c.Params, hc)
	case "email":
		return newEmail(name, c.Params)
	case "wecom":
		return newWecom(name, c.Params, hc)
	case "dingtalk":
		return newDingtalk(name, c.Params, hc)
	case "feishu":
		return newFeishu(name, c.Params, hc)
	}
	return nil, fmt.Errorf("channel %q: %w %q", name, ErrUnknownChannelType, c.Type)
}

func binanceURL(sym string) string { return "https://www.binance.com/zh-CN/futures/" + sym }

func tradingViewURL(sym string) string {
	return "https://www.tradingview.com/chart/?symbol=BINANCE:" + sym + ".P"
}

// Format renders a batch: one line per signal, a title with the highest level
// and the signal count, and Binance/TradingView links for each distinct symbol
// in first-seen order. An empty batch (which the Batcher never emits) yields
// "[INFO] 0 signal(s)" with level info.
func Format(b signal.Batch) Message { return FormatIn(b, nil) }

// FormatIn is Format showing bar times in loc (nil means UTC). A signal with
// a bar is rendered as "[5m 11-02 14:35 盘中] SYM rule @ price 本根 +2.31% | detail";
// 盘中 marks a bar still open, 收盘 a closed one.
func FormatIn(b signal.Batch, loc *time.Location) Message {
	if loc == nil {
		loc = time.UTC
	}
	m := Message{Level: config.LevelInfo, Lines: make([]string, 0, len(b.Signals))}
	seen := make(map[string]bool)
	for i, s := range b.Signals {
		if i == 0 || s.Level.Rank() > m.Level.Rank() {
			m.Level = s.Level
		}
		detail := s.Detail
		if detail == "" {
			detail = s.RuleID
		}
		head := string(s.Interval)
		if !s.BarTime.IsZero() {
			state := "收盘"
			if s.BarLive {
				state = "盘中"
			}
			head += " " + s.BarTime.In(loc).Format("01-02 15:04") + " " + state
		}
		change := ""
		if s.HasChange {
			change = fmt.Sprintf(" 本根 %+.2f%%", s.ChangePct)
		}
		m.Lines = append(m.Lines, fmt.Sprintf("[%s] %s %s @ %s%s | %s",
			head, s.Symbol, s.RuleID, strconv.FormatFloat(s.Price, 'f', -1, 64), change, detail))
		if !seen[s.Symbol] {
			seen[s.Symbol] = true
			m.Links = append(m.Links,
				Link{Text: "Binance " + s.Symbol, URL: binanceURL(s.Symbol)},
				Link{Text: "TradingView " + s.Symbol, URL: tradingViewURL(s.Symbol)})
		}
	}
	m.Title = fmt.Sprintf("[%s] %d signal(s)", strings.ToUpper(string(m.Level)), len(b.Signals))
	return m
}
