package config

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const cfgTmpl = `symbols: [%s]
rules:
  - id: r1
    interval: 5m
    when:
      all:
        - change_pct(1) > 3
    cooldown: 30m
    channels: [wh]
channels:
  wh:
    type: webhook
    params:
      url: http://localhost/x
`

func writeAtomic(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func startWatch(t *testing.T, path string, s *Store, onErr func(error)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Watch(ctx, path, s, onErr) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Watch returned %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Watch did not stop after cancel")
		}
	})
	time.Sleep(100 * time.Millisecond) // let the watcher register
}

func TestWatcher_ReloadsOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeAtomic(t, path, "symbols: [BTCUSDT]\n"+cfgTmpl[len("symbols: [%s]\n"):])
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(c)
	ch := s.Subscribe()
	startWatch(t, path, s, func(err error) { t.Errorf("unexpected onErr: %v", err) })

	writeAtomic(t, path, "symbols: [BTCUSDT, ETHUSDT, SOLUSDT]\n"+cfgTmpl[len("symbols: [%s]\n"):])
	select {
	case got := <-ch:
		if len(got.Symbols) != 3 {
			t.Fatalf("symbols=%v", got.Symbols)
		}
		if s.Current() != got {
			t.Error("Current not updated")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no reload broadcast")
	}
}

func TestWatcher_InvalidKeepsOld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeAtomic(t, path, "symbols: [BTCUSDT]\n"+cfgTmpl[len("symbols: [%s]\n"):])
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(c)
	ch := s.Subscribe()
	var n atomic.Int32
	errCh := make(chan error, 4)
	startWatch(t, path, s, func(err error) { n.Add(1); errCh <- err })

	writeAtomic(t, path, "symbols: [unclosed\n  : : bad")
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("onErr not called")
	}
	time.Sleep(500 * time.Millisecond)
	if n.Load() != 1 {
		t.Errorf("onErr calls=%d", n.Load())
	}
	if s.Current() != c {
		t.Error("old snapshot replaced")
	}
	select {
	case <-ch:
		t.Error("unexpected broadcast")
	default:
	}
}

func TestStore_SubscribeLatestWins(t *testing.T) {
	a, b, c := &Config{}, &Config{}, &Config{}
	s := NewStore(a)
	ch := s.Subscribe()
	s.Set(b)
	s.Set(c)
	if got := <-ch; got != c {
		t.Error("expected newest value")
	}
	if s.Current() != c {
		t.Error("Current")
	}
}
