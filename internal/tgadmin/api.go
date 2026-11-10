package tgadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.telegram.org"

// api is a minimal Telegram Bot API client.
type api struct {
	base  string // https://api.telegram.org/bot<token>
	token string
	hc    *http.Client
}

func newAPI(baseURL, token string, hc *http.Client) *api {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if hc == nil {
		hc = &http.Client{}
	}
	return &api{base: strings.TrimRight(baseURL, "/") + "/bot" + token, token: token, hc: hc}
}

type apiError struct {
	Method      string
	Code        int
	Description string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("telegram %s: %d %s", e.Method, e.Code, e.Description)
}

// call invokes method with params (JSON) and decodes the result into out
// (may be nil). The bot token never appears in returned errors.
func (a *api) call(ctx context.Context, method string, params any, out any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/"+method, bytes.NewReader(body))
	if err != nil {
		return a.redact(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return a.redact(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return a.redact(err)
	}
	var r struct {
		OK          bool            `json:"ok"`
		ErrorCode   int             `json:"error_code"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("telegram %s: HTTP %d: bad response", method, resp.StatusCode)
	}
	if !r.OK {
		code := r.ErrorCode
		if code == 0 {
			code = resp.StatusCode
		}
		return &apiError{Method: method, Code: code, Description: r.Description}
	}
	if out != nil {
		return json.Unmarshal(r.Result, out)
	}
	return nil
}

func (a *api) redact(err error) error {
	if a.token == "" {
		return err
	}
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), a.token, "<token>"))
}

// Telegram types (only the fields used).

type update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *message       `json:"message"`
	CallbackQuery *callbackQuery `json:"callback_query"`
}

type user struct {
	ID int64 `json:"id"`
}

type chat struct {
	ID int64 `json:"id"`
}

type message struct {
	MessageID int64  `json:"message_id"`
	From      *user  `json:"from"`
	Chat      chat   `json:"chat"`
	Date      int64  `json:"date"`
	Text      string `json:"text"`
}

func (m *message) time() time.Time { return time.Unix(m.Date, 0) }

type callbackQuery struct {
	ID      string   `json:"id"`
	From    user     `json:"from"`
	Message *message `json:"message"`
	Data    string   `json:"data"`
}

type button struct {
	Text string `json:"text"`
	Data string `json:"callback_data"`
}

type keyboard struct {
	Rows [][]button `json:"inline_keyboard"`
}
