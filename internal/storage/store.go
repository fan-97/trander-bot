// Package storage persists market data and signals to PostgreSQL/TimescaleDB.
package storage

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"trader-bot/internal/market"
	"trader-bot/internal/signal"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationSource is the migration FS Open applies. Production always uses
// the embedded files; integration tests may swap in a copy without the
// Timescale blocks (see testmain_integration_test.go).
var migrationSource fs.FS = migrationsFS

// maxParams is PostgreSQL's limit on bind parameters per statement.
const maxParams = 65535

// Store is a pgx-backed repository. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to dsn, applies all pending migrations and returns a Store.
func Open(ctx context.Context, dsn string) (*Store, error) {
	if err := migrateUp(ctx, dsn); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// migrateUp applies pending migrations. golang-migrate has no context
// support, so Up runs on its own goroutine; if ctx ends first, Open signals
// GracefulStop (migrate stops before the next migration) and returns
// ctx.Err() at once. A statement already executing is not interrupted; the
// goroutine finishes it and then closes its own database handle.
func migrateUp(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("storage: parse dsn: %w", err)
	}
	srcDrv, err := iofs.New(migrationSource, "migrations")
	if err != nil {
		return fmt.Errorf("storage: migration source: %w", err)
	}
	db := stdlib.OpenDB(*cfg)
	dbDrv, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		db.Close()
		return fmt.Errorf("storage: migrate driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", srcDrv, "pgx5", dbDrv)
	if err != nil {
		db.Close()
		return fmt.Errorf("storage: migrate: %w", err)
	}
	done := make(chan error, 1)
	go func() {
		err := m.Up()
		m.Close()
		db.Close()
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("storage: migrate up: %w", err)
		}
		return nil
	case <-ctx.Done():
		m.GracefulStop <- true
		return fmt.Errorf("storage: migrate up: %w", ctx.Err())
	}
}

// insertRows runs "head VALUES (...),(...) tail" for n rows of cols columns,
// chunked to stay under the bind-parameter limit.
func (s *Store) insertRows(ctx context.Context, head, tail string, cols, n int, row func(i int) []any) error {
	per := maxParams / cols
	for lo := 0; lo < n; lo += per {
		hi := min(lo+per, n)
		var sb strings.Builder
		sb.WriteString(head)
		sb.WriteString(" VALUES ")
		args := make([]any, 0, (hi-lo)*cols)
		for i := lo; i < hi; i++ {
			if i > lo {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')
			for c := 0; c < cols; c++ {
				if c > 0 {
					sb.WriteByte(',')
				}
				sb.WriteByte('$')
				sb.WriteString(strconv.Itoa(len(args) + c + 1))
			}
			sb.WriteByte(')')
			args = append(args, row(i)...)
		}
		sb.WriteByte(' ')
		sb.WriteString(tail)
		if _, err := s.pool.Exec(ctx, sb.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

type klineKey struct {
	symbol   string
	interval market.Interval
	open     int64
}

// WriteKlines upserts ks; a later bar with the same key wins.
func (s *Store) WriteKlines(ctx context.Context, ks []market.Kline) error {
	// One INSERT ... ON CONFLICT DO UPDATE cannot touch a row twice, so keep
	// only the last bar per key.
	idx := make(map[klineKey]int, len(ks))
	uniq := make([]market.Kline, 0, len(ks))
	for _, k := range ks {
		key := klineKey{k.Symbol, k.Interval, k.OpenTime.UnixNano()}
		if i, ok := idx[key]; ok {
			uniq[i] = k
			continue
		}
		idx[key] = len(uniq)
		uniq = append(uniq, k)
	}
	err := s.insertRows(ctx,
		`INSERT INTO klines (symbol, interval, open_time, open, high, low, close, volume, quote_volume)`,
		`ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
			open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
			volume = EXCLUDED.volume, quote_volume = EXCLUDED.quote_volume`,
		9, len(uniq), func(i int) []any {
			k := uniq[i]
			return []any{k.Symbol, string(k.Interval), k.OpenTime.UTC(), k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume}
		})
	if err != nil {
		return fmt.Errorf("storage: write klines: %w", err)
	}
	return nil
}

// WriteMarkPrices inserts ms; a sample whose key already exists is ignored.
func (s *Store) WriteMarkPrices(ctx context.Context, ms []market.MarkPriceEvent) error {
	err := s.insertRows(ctx, `INSERT INTO mark_prices (symbol, time, mark_price, funding_rate)`,
		`ON CONFLICT DO NOTHING`, 4, len(ms), func(i int) []any {
			m := ms[i]
			return []any{m.Symbol, m.Time.UTC(), m.MarkPrice, m.FundingRate}
		})
	if err != nil {
		return fmt.Errorf("storage: write mark prices: %w", err)
	}
	return nil
}

// WriteOI inserts ois; a sample whose key already exists is ignored.
func (s *Store) WriteOI(ctx context.Context, ois []market.OpenInterestEvent) error {
	err := s.insertRows(ctx, `INSERT INTO open_interest (symbol, time, oi)`,
		`ON CONFLICT DO NOTHING`, 3, len(ois), func(i int) []any {
			o := ois[i]
			return []any{o.Symbol, o.Time.UTC(), o.OI}
		})
	if err != nil {
		return fmt.Errorf("storage: write open interest: %w", err)
	}
	return nil
}

// WriteLongShort inserts ls; a sample whose key already exists is ignored.
func (s *Store) WriteLongShort(ctx context.Context, ls []market.LongShortEvent) error {
	err := s.insertRows(ctx, `INSERT INTO long_short (symbol, time, kind, ratio)`,
		`ON CONFLICT DO NOTHING`, 4, len(ls), func(i int) []any {
			l := ls[i]
			return []any{l.Symbol, l.Time.UTC(), l.Kind, l.Ratio}
		})
	if err != nil {
		return fmt.Errorf("storage: write long/short: %w", err)
	}
	return nil
}

// WriteLiquidations appends ls with COPY.
func (s *Store) WriteLiquidations(ctx context.Context, ls []market.LiquidationEvent) error {
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"liquidations"},
		[]string{"symbol", "time", "side", "price", "qty"},
		pgx.CopyFromSlice(len(ls), func(i int) ([]any, error) {
			l := ls[i]
			return []any{l.Symbol, l.Time.UTC(), l.Side, l.Price, l.Qty}, nil
		}))
	if err != nil {
		return fmt.Errorf("storage: write liquidations: %w", err)
	}
	return nil
}

// WriteSignals appends ss with COPY, suppressed ones included.
func (s *Store) WriteSignals(ctx context.Context, ss []signal.Signal) error {
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"signals"},
		[]string{"time", "rule_id", "symbol", "interval", "level", "price", "detail", "channels", "suppressed"},
		pgx.CopyFromSlice(len(ss), func(i int) ([]any, error) {
			g := ss[i]
			ch := g.Channels
			if ch == nil {
				ch = []string{}
			}
			return []any{g.Time.UTC(), g.RuleID, g.Symbol, string(g.Interval), string(g.Level), g.Price, g.Detail, ch, g.Suppressed}, nil
		}))
	if err != nil {
		return fmt.Errorf("storage: write signals: %w", err)
	}
	return nil
}

// RecentKlines returns the newest n stored (closed) bars for symbol/interval
// in ascending open-time order.
func (s *Store) RecentKlines(ctx context.Context, symbol string, interval market.Interval, n int) ([]market.Kline, error) {
	if n <= 0 {
		return []market.Kline{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT open_time, open, high, low, close, volume, quote_volume FROM (
			SELECT * FROM klines WHERE symbol = $1 AND interval = $2
			ORDER BY open_time DESC LIMIT $3
		) t ORDER BY open_time ASC`, symbol, string(interval), n)
	if err != nil {
		return nil, fmt.Errorf("storage: recent klines: %w", err)
	}
	defer rows.Close()
	out := make([]market.Kline, 0, min(n, 1024))
	for rows.Next() {
		k := market.Kline{Symbol: symbol, Interval: interval, Closed: true}
		if err := rows.Scan(&k.OpenTime, &k.Open, &k.High, &k.Low, &k.Close, &k.Volume, &k.QuoteVolume); err != nil {
			return nil, fmt.Errorf("storage: recent klines: %w", err)
		}
		k.OpenTime = k.OpenTime.UTC()
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: recent klines: %w", err)
	}
	return out, nil
}
