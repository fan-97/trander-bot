// Package tgadmin is a Telegram bot for managing the watched symbols: an
// inline-keyboard panel plus /add and /del commands. Changes go through a
// Backend, which writes them to the config file so they are hot reloaded.
package tgadmin

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pollTimeout = 30 * time.Second // getUpdates long-poll
	callTimeout = 15 * time.Second // other API calls
	perPage     = 24               // symbol buttons per panel page
	perRow      = 3
	// pendingTTL: after tapping 添加, the next text in that chat within this
	// long is taken as the symbols to add.
	pendingTTL = 5 * time.Minute
	// staleAfter: messages older than this (sent while the bot was down) are ignored.
	staleAfter = 5 * time.Minute
	// failAlertAfter consecutive getUpdates failures raise one alert.
	failAlertAfter = 3
	maxBackoff     = time.Minute
)

// View is what the panel shows.
type View struct {
	Manual   []string        // manual symbols, as in the config file
	Inactive map[string]bool // manual symbols not in the running watch pool
	Auto     []string        // auto-selected symbols (read-only here)
	AutoOn   bool
}

// Result describes one Add or Remove.
type Result struct {
	Changed []string // symbols actually added or removed
	Notes   []string // why requested symbols were skipped
	Applied bool     // the running bot picked the change up (hot reload done)
}

// Backend applies symbol changes.
type Backend interface {
	View() (View, error)
	Add(ctx context.Context, syms []string) (Result, error)
	Remove(ctx context.Context, syms []string) (Result, error)
}

// Options configure a Bot.
type Options struct {
	Token   string
	BaseURL string // default https://api.telegram.org
	HTTP    *http.Client
	Allow   []int64 // chat or user ids allowed to manage
	Backend Backend
	Log     *slog.Logger
	// Alert is called once when polling keeps failing, and once when it
	// recovers (may be nil).
	Alert func(title, text string)
}

// Bot polls Telegram for updates and serves the admin panel.
type Bot struct {
	api   *api
	allow map[int64]bool
	be    Backend
	log   *slog.Logger
	alert func(title, text string)
	now   func() time.Time

	mu      sync.Mutex
	pending map[int64]time.Time // chat id -> 添加 tapped at
}

// New returns a Bot; call Run to start it.
func New(o Options) *Bot {
	b := &Bot{
		api:     newAPI(o.BaseURL, o.Token, o.HTTP),
		allow:   map[int64]bool{},
		be:      o.Backend,
		log:     o.Log,
		alert:   o.Alert,
		now:     time.Now,
		pending: map[int64]time.Time{},
	}
	for _, id := range o.Allow {
		b.allow[id] = true
	}
	if b.log == nil {
		b.log = slog.Default()
	}
	return b
}

// Run long-polls getUpdates until ctx is cancelled (returns nil).
func (b *Bot) Run(ctx context.Context) error {
	b.setCommands(ctx)
	var offset int64
	fails := 0
	alerted := false
	backoff := time.Second
	for ctx.Err() == nil {
		var ups []update
		cctx, cancel := context.WithTimeout(ctx, pollTimeout+15*time.Second)
		err := b.api.call(cctx, "getUpdates", map[string]any{
			"offset": offset, "timeout": int(pollTimeout / time.Second),
			"allowed_updates": []string{"message", "callback_query"},
		}, &ups)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fails++
			b.log.Warn("telegram admin: getUpdates failed", "err", err, "attempt", fails)
			if fails == failAlertAfter && !alerted && b.alert != nil {
				alerted = true
				hint := err.Error()
				var ae *apiError
				if errors.As(err, &ae) && ae.Code == http.StatusConflict {
					hint += "（该 bot 设置了 webhook 或有其他进程在 getUpdates）"
				}
				b.alert("Telegram 管理面板不可用", "拉取 Telegram 消息连续失败，币种管理面板暂不可用: "+hint)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		if alerted && b.alert != nil {
			b.alert("Telegram 管理面板已恢复", "币种管理面板恢复可用")
		}
		fails, alerted, backoff = 0, false, time.Second
		for _, u := range ups {
			offset = max(offset, u.UpdateID+1)
			b.handle(ctx, u)
		}
	}
	return nil
}

func (b *Bot) setCommands(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	err := b.api.call(cctx, "setMyCommands", map[string]any{"commands": []map[string]string{
		{"command": "symbols", "description": "监控币种管理面板"},
		{"command": "add", "description": "添加币种，如 /add BTC ETHUSDT"},
		{"command": "del", "description": "删除币种，如 /del SOLUSDT"},
		{"command": "help", "description": "帮助"},
	}}, nil)
	if err != nil && ctx.Err() == nil {
		b.log.Warn("telegram admin: setMyCommands failed", "err", err)
	}
}

func (b *Bot) authorized(chatID int64, from *user) bool {
	if b.allow[chatID] {
		return true
	}
	return from != nil && b.allow[from.ID]
}

func (b *Bot) handle(ctx context.Context, u update) {
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("telegram admin: handler panic", "panic", r)
		}
	}()
	switch {
	case u.CallbackQuery != nil:
		b.handleCallback(ctx, u.CallbackQuery)
	case u.Message != nil:
		b.handleMessage(ctx, u.Message)
	}
}

var cmdRe = regexp.MustCompile(`^/([A-Za-z_]+)(?:@\S+)?\s*(.*)$`)

func (b *Bot) handleMessage(ctx context.Context, m *message) {
	text := strings.TrimSpace(m.Text)
	if text == "" || b.now().Sub(m.time()) > staleAfter {
		return
	}
	cmd, args := "", text
	if sm := cmdRe.FindStringSubmatch(text); sm != nil {
		cmd, args = strings.ToLower(sm[1]), sm[2]
	}
	if !b.authorized(m.Chat.ID, m.From) {
		if cmd != "" {
			b.log.Warn("telegram admin: unauthorized command", "chat", m.Chat.ID, "cmd", cmd)
			b.send(ctx, m.Chat.ID, fmt.Sprintf("无权限。把 chat id <code>%d</code> 加到配置的 telegram_admin.allow 后再试。", m.Chat.ID), nil)
		}
		return
	}
	switch cmd {
	case "start", "symbols", "menu", "list":
		b.clearPending(m.Chat.ID)
		b.sendPanel(ctx, m.Chat.ID, "")
	case "help":
		b.send(ctx, m.Chat.ID, helpText, nil)
	case "add":
		if strings.TrimSpace(args) == "" {
			b.promptAdd(ctx, m.Chat.ID)
			return
		}
		b.clearPending(m.Chat.ID)
		b.apply(ctx, m.Chat.ID, 0, true, args)
	case "del", "delete", "remove", "rm":
		b.clearPending(m.Chat.ID)
		if strings.TrimSpace(args) == "" {
			b.sendPanel(ctx, m.Chat.ID, "点击下面的币种按钮删除，或发送 /del 币种")
			return
		}
		b.apply(ctx, m.Chat.ID, 0, false, args)
	case "cancel":
		b.clearPending(m.Chat.ID)
		b.send(ctx, m.Chat.ID, "已取消", nil)
	case "":
		if b.takePending(m.Chat.ID) {
			b.apply(ctx, m.Chat.ID, 0, true, args)
		}
	}
}

const helpText = `<b>监控币种管理</b>
/symbols 打开管理面板：点币种按钮删除，点「➕ 添加」后发送币种
/add BTC ETHUSDT 添加（多个用空格或逗号分隔，不写 USDT 会自动补上）
/del SOLUSDT 删除
/cancel 取消待输入的添加

修改会写回配置文件并自动热加载，无需重启。自动选币（auto_symbols）选出的币种只读。`

func (b *Bot) handleCallback(ctx context.Context, q *callbackQuery) {
	if q.Message == nil {
		b.answer(ctx, q.ID, "")
		return
	}
	chatID, msgID := q.Message.Chat.ID, q.Message.MessageID
	if !b.authorized(chatID, &q.From) {
		b.answer(ctx, q.ID, "无权限")
		return
	}
	kind, arg, _ := strings.Cut(q.Data, ":")
	switch kind {
	case "p": // show page
		b.answer(ctx, q.ID, "")
		page, _ := strconv.Atoi(arg)
		b.editPanel(ctx, chatID, msgID, page, "")
	case "d": // ask to confirm removal
		b.answer(ctx, q.ID, "")
		sym, page := splitPage(arg)
		b.edit(ctx, chatID, msgID, fmt.Sprintf("确认删除 <b>%s</b>？\n删除后不再监控该币种（自动选币选中的除外），并立即热加载。", html.EscapeString(sym)),
			&keyboard{Rows: [][]button{{
				{Text: "✅ 确认删除", Data: "D:" + sym + "@" + strconv.Itoa(page)},
				{Text: "↩️ 取消", Data: "p:" + strconv.Itoa(page)},
			}}})
	case "D": // removal confirmed
		b.answer(ctx, q.ID, "正在删除…")
		sym, page := splitPage(arg)
		b.applyEdit(ctx, chatID, msgID, page, false, []string{sym})
	case "a": // add: ask for symbols
		b.answer(ctx, q.ID, "")
		b.promptAdd(ctx, chatID)
	default:
		b.answer(ctx, q.ID, "")
	}
}

func splitPage(arg string) (string, int) {
	sym, p, _ := strings.Cut(arg, "@")
	page, _ := strconv.Atoi(p)
	return sym, page
}

func (b *Bot) promptAdd(ctx context.Context, chatID int64) {
	b.mu.Lock()
	b.pending[chatID] = b.now()
	b.mu.Unlock()
	b.send(ctx, chatID, "请发送要添加的币种，多个用空格或逗号分隔，如 <code>BTC ETHUSDT</code>（不写 USDT 会自动补上）。发送 /cancel 取消。",
		map[string]any{"force_reply": true, "input_field_placeholder": "BTC ETH SOL"})
}

func (b *Bot) takePending(chatID int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	at, ok := b.pending[chatID]
	delete(b.pending, chatID)
	return ok && b.now().Sub(at) <= pendingTTL
}

func (b *Bot) clearPending(chatID int64) {
	b.mu.Lock()
	delete(b.pending, chatID)
	b.mu.Unlock()
}

// apply parses args and adds or removes them, showing progress in a new
// panel message.
func (b *Bot) apply(ctx context.Context, chatID int64, page int, add bool, args string) {
	syms, bad := ParseSymbols(args)
	if len(syms) == 0 {
		msg := "没有识别到币种"
		if len(bad) > 0 {
			msg += "：无效的输入 " + strings.Join(bad, ", ")
		}
		b.send(ctx, chatID, "❌ "+html.EscapeString(msg), nil)
		return
	}
	msgID := b.send(ctx, chatID, "⏳ 正在保存并热加载…", nil)
	if msgID == 0 {
		return
	}
	res, err := b.do(ctx, add, syms)
	notice := formatResult(add, res, err, bad)
	b.editPanel(ctx, chatID, msgID, page, notice)
}

func (b *Bot) applyEdit(ctx context.Context, chatID, msgID int64, page int, add bool, syms []string) {
	b.edit(ctx, chatID, msgID, "⏳ 正在保存并热加载…", nil)
	res, err := b.do(ctx, add, syms)
	b.editPanel(ctx, chatID, msgID, page, formatResult(add, res, err, nil))
}

func (b *Bot) do(ctx context.Context, add bool, syms []string) (Result, error) {
	if add {
		return b.be.Add(ctx, syms)
	}
	return b.be.Remove(ctx, syms)
}

// formatResult renders the outcome of an edit as an HTML notice.
func formatResult(add bool, res Result, err error, bad []string) string {
	verb := "删除"
	if add {
		verb = "添加"
	}
	var lines []string
	if err != nil {
		lines = append(lines, "❌ "+verb+"失败: "+html.EscapeString(err.Error()))
	} else if len(res.Changed) > 0 {
		state := "已热加载生效"
		if !res.Applied {
			state = "已写入配置，等待热加载"
		}
		lines = append(lines, fmt.Sprintf("✅ 已%s %s（%s）", verb, html.EscapeString(strings.Join(res.Changed, ", ")), state))
	} else {
		lines = append(lines, "ℹ️ 没有变化")
	}
	for _, n := range res.Notes {
		lines = append(lines, "· "+html.EscapeString(n))
	}
	if len(bad) > 0 {
		lines = append(lines, "· 无效的输入: "+html.EscapeString(strings.Join(bad, ", ")))
	}
	return strings.Join(lines, "\n")
}

func (b *Bot) sendPanel(ctx context.Context, chatID int64, notice string) {
	text, kb := b.panel(0, notice)
	b.send(ctx, chatID, text, kb)
}

func (b *Bot) editPanel(ctx context.Context, chatID, msgID int64, page int, notice string) {
	text, kb := b.panel(page, notice)
	b.edit(ctx, chatID, msgID, text, kb)
}

// panel renders the management panel: the symbol lists and one button per
// manual symbol (tap to remove), paged, plus add / refresh buttons.
func (b *Bot) panel(page int, notice string) (string, *keyboard) {
	v, err := b.be.View()
	if err != nil {
		return "❌ 读取配置失败: " + html.EscapeString(err.Error()), &keyboard{Rows: [][]button{{{Text: "🔄 刷新", Data: "p:0"}}}}
	}
	return renderPanel(v, page, notice)
}

func renderPanel(v View, page int, notice string) (string, *keyboard) {
	var sb strings.Builder
	if notice != "" {
		sb.WriteString(notice + "\n\n")
	}
	sb.WriteString("<b>📋 监控币种管理</b>\n")
	fmt.Fprintf(&sb, "手动币种（%d）: ", len(v.Manual))
	if len(v.Manual) == 0 {
		sb.WriteString("无")
	} else {
		parts := make([]string, len(v.Manual))
		for i, s := range v.Manual {
			parts[i] = html.EscapeString(s)
			if v.Inactive[s] {
				parts[i] += "⚠️"
			}
		}
		sb.WriteString("<code>" + strings.Join(parts, ", ") + "</code>")
	}
	sb.WriteString("\n")
	switch {
	case !v.AutoOn:
		sb.WriteString("自动币种: 未开启")
	case len(v.Auto) == 0:
		sb.WriteString("自动币种: 暂无")
	default:
		fmt.Fprintf(&sb, "自动币种（%d，只读）: <code>%s</code>", len(v.Auto), html.EscapeString(strings.Join(v.Auto, ", ")))
	}
	sb.WriteString("\n\n点击币种按钮删除；点「➕ 添加」后发送币种代码。修改自动热加载，无需重启。")
	for _, s := range v.Manual {
		if v.Inactive[s] {
			sb.WriteString("\n⚠️ 表示未在监控（不是可交易的 U 本位永续合约）")
			break
		}
	}

	pages := max(1, (len(v.Manual)+perPage-1)/perPage)
	page = min(max(page, 0), pages-1)
	kb := &keyboard{}
	from, to := page*perPage, min((page+1)*perPage, len(v.Manual))
	var row []button
	for _, s := range v.Manual[from:to] {
		row = append(row, button{Text: "✖ " + s, Data: "d:" + s + "@" + strconv.Itoa(page)})
		if len(row) == perRow {
			kb.Rows = append(kb.Rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		kb.Rows = append(kb.Rows, row)
	}
	if pages > 1 {
		nav := []button{}
		if page > 0 {
			nav = append(nav, button{Text: "◀ 上一页", Data: "p:" + strconv.Itoa(page-1)})
		}
		nav = append(nav, button{Text: fmt.Sprintf("%d/%d", page+1, pages), Data: "x"})
		if page < pages-1 {
			nav = append(nav, button{Text: "下一页 ▶", Data: "p:" + strconv.Itoa(page+1)})
		}
		kb.Rows = append(kb.Rows, nav)
	}
	kb.Rows = append(kb.Rows, []button{{Text: "➕ 添加", Data: "a"}, {Text: "🔄 刷新", Data: "p:" + strconv.Itoa(page)}})
	return sb.String(), kb
}

var symbolRe = regexp.MustCompile(`^[A-Z0-9]{2,20}$`)

// quoteSuffixes are the quote assets a symbol may already end with; anything
// else gets USDT appended (BTC -> BTCUSDT).
var quoteSuffixes = []string{"USDT", "USDC"}

// ParseSymbols splits user input on spaces / commas, upper-cases each token
// and appends USDT when no quote asset is given. Invalid tokens are returned
// separately. The result is de-duplicated, in input order.
func ParseSymbols(s string) (syms, bad []string) {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == ',' || r == '，' || r == '、' || r == ';' || r == '\n' || r == '\t' || r == '/'
	})
	seen := map[string]bool{}
	for _, f := range fields {
		t := strings.ToUpper(strings.TrimSpace(f))
		if t == "" {
			continue
		}
		if !symbolRe.MatchString(t) {
			bad = append(bad, f)
			continue
		}
		hasQuote := false
		for _, q := range quoteSuffixes {
			if strings.HasSuffix(t, q) && len(t) > len(q) {
				hasQuote = true
				break
			}
		}
		if !hasQuote {
			t += "USDT"
		}
		if !seen[t] {
			seen[t] = true
			syms = append(syms, t)
		}
	}
	return syms, bad
}

// send posts text (HTML) and returns the new message id (0 on failure).
func (b *Bot) send(ctx context.Context, chatID int64, text string, markup any) int64 {
	p := map[string]any{"chat_id": chatID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if kb, ok := markup.(*keyboard); ok && kb == nil {
		markup = nil
	}
	if markup != nil {
		p["reply_markup"] = markup
	}
	var m message
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := b.api.call(cctx, "sendMessage", p, &m); err != nil {
		b.log.Warn("telegram admin: sendMessage failed", "err", err)
		return 0
	}
	return m.MessageID
}

func (b *Bot) edit(ctx context.Context, chatID, msgID int64, text string, kb *keyboard) {
	p := map[string]any{"chat_id": chatID, "message_id": msgID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if kb != nil {
		p["reply_markup"] = kb
	}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := b.api.call(cctx, "editMessageText", p, nil); err != nil {
		var ae *apiError
		if errors.As(err, &ae) && strings.Contains(ae.Description, "message is not modified") {
			return
		}
		b.log.Warn("telegram admin: editMessageText failed", "err", err)
	}
}

func (b *Bot) answer(ctx context.Context, id, text string) {
	p := map[string]any{"callback_query_id": id}
	if text != "" {
		p["text"] = text
	}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := b.api.call(cctx, "answerCallbackQuery", p, nil); err != nil {
		b.log.Debug("telegram admin: answerCallbackQuery failed", "err", err)
	}
}
