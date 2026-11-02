package config

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

const reloadDebounce = 300 * time.Millisecond

// Store holds the current config snapshot and notifies subscribers on change.
type Store struct {
	cur atomic.Pointer[Config]
	mu  sync.Mutex
	sub []chan *Config
}

func NewStore(c *Config) *Store {
	s := &Store{}
	s.cur.Store(c)
	return s
}

// Current returns the latest snapshot.
func (s *Store) Current() *Config { return s.cur.Load() }

// Subscribe returns a channel (buffer 1) that receives each new config.
// If the subscriber is slow, the pending value is replaced by the newest one.
func (s *Store) Subscribe() <-chan *Config {
	ch := make(chan *Config, 1)
	s.mu.Lock()
	s.sub = append(s.sub, ch)
	s.mu.Unlock()
	return ch
}

// Set replaces the snapshot and broadcasts it to all subscribers without blocking.
func (s *Store) Set(c *Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.Store(c)
	for _, ch := range s.sub {
		for {
			select {
			case ch <- c:
			default:
				select {
				case <-ch: // drop stale pending value, retry
				default:
				}
				continue
			}
			break
		}
	}
}

// Watch reloads path on change (debounced) and publishes valid configs to s.
// Invalid configs are reported via onErr (may be nil) and the old snapshot is kept.
// It returns nil when ctx is cancelled, or an error if the watcher cannot start.
// errWatcherClosed is returned when fsnotify stops delivering events, so the
// loss of hot reload surfaces (errgroup) instead of passing silently.
var errWatcherClosed = errors.New("config watcher: fsnotify channels closed, hot reload stopped")

func Watch(ctx context.Context, path string, s *Store, onErr func(error)) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()

	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	base := filepath.Base(abs)
	if err := w.Add(filepath.Dir(abs)); err != nil {
		return err
	}

	reload := make(chan struct{}, 1)
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	trigger := func() {
		select {
		case reload <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return errWatcherClosed
			}
			// Only a write or a (re)created file means new content. Remove and
			// Rename are the first half of an editor's save; reacting to them
			// would load a missing file and raise a false 配置无效.
			if filepath.Base(ev.Name) != base || ev.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}
			if timer == nil {
				timer = time.AfterFunc(reloadDebounce, trigger)
			} else {
				timer.Reset(reloadDebounce)
			}
		case err, ok := <-w.Errors:
			if !ok {
				return errWatcherClosed
			}
			if onErr != nil {
				onErr(err)
			}
		case <-reload:
			c, err := Load(abs)
			if err != nil {
				if onErr != nil {
					onErr(err)
				}
				continue
			}
			s.Set(c)
		}
	}
}
