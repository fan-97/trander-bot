# internal/storage

PostgreSQL/TimescaleDB persistence: embedded golang-migrate migrations
(`migrations/`), the pgx `Store`, and the batching `Writer`.

## Tests

Unit tests (no database): `go test -race ./internal/storage/...`

Integration tests (`//go:build integration`) need `TEST_DATABASE_URL` and skip
without it.

### Verifying the real TimescaleDB schema

The Timescale statements (extension, hypertables, compression policy) have
only been checked by an actual TimescaleDB run if you do this:

```sh
docker run -d --name tb-tsdb -p 5433:5432 -e POSTGRES_PASSWORD=pg timescale/timescaledb:2.17.2-pg16
TEST_DATABASE_URL='postgres://postgres:pg@localhost:5433/postgres?sslmode=disable' \
  go test -race -count=1 -tags integration ./internal/storage/...
docker rm -f tb-tsdb
```

Do **not** set `TB_SKIP_TIMESCALE` for this run.

### Plain PostgreSQL (no Timescale)

`TB_SKIP_TIMESCALE=1` is read only by the integration test `TestMain`
(`testmain_integration_test.go`). It strips the `-- timescale:begin` /
`-- timescale:end` blocks from the migrations before `Open` runs them.
Production code never reads it and always applies the full migration.

```sh
TB_SKIP_TIMESCALE=1 TEST_DATABASE_URL='postgres://postgres@localhost:5433/postgres?sslmode=disable' \
  go test -race -count=1 -tags integration ./internal/storage/...
```
