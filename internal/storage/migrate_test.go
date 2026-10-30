package storage

import (
	"bytes"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

// stripTimescale drops every line from "-- timescale:begin" through
// "-- timescale:end" inclusive. Test-only: it lets integration tests run the
// schema on plain PostgreSQL.
func stripTimescale(b []byte) []byte {
	var out bytes.Buffer
	skip := false
	for _, line := range bytes.SplitAfter(b, []byte("\n")) {
		switch strings.TrimSpace(string(line)) {
		case "-- timescale:begin":
			skip = true
		case "-- timescale:end":
			skip = false
		default:
			if !skip {
				out.Write(line)
			}
		}
	}
	return out.Bytes()
}

// strippedMigrations returns the embedded migrations with Timescale blocks removed.
func strippedMigrations() (fs.FS, error) {
	ents, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	out := fstest.MapFS{}
	for _, e := range ents {
		b, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		out["migrations/"+e.Name()] = &fstest.MapFile{Data: stripTimescale(b), Mode: 0o444}
	}
	return out, nil
}

func TestStripTimescale(t *testing.T) {
	in := "A;\n-- timescale:begin\nCREATE EXTENSION x;\n-- timescale:end\nB;\n"
	if got := string(stripTimescale([]byte(in))); got != "A;\nB;\n" {
		t.Fatalf("stripTimescale = %q", got)
	}
	up, err := migrationsFS.ReadFile("migrations/0001_init.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"create_hypertable", "add_compression_policy", "create_default_indexes => FALSE",
		"timescaledb.compress_segmentby = 'symbol, interval'", "timescaledb.compress_orderby = 'open_time DESC'"} {
		if !strings.Contains(string(up), want) {
			t.Fatalf("production migration lacks %q", want)
		}
	}
	if s := string(stripTimescale(up)); strings.Contains(strings.ToLower(s), "timescaledb") || strings.Contains(s, "hypertable") {
		t.Fatalf("stripped migration still references Timescale:\n%s", s)
	}
}
