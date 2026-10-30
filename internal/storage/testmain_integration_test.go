//go:build integration

package storage

import (
	"fmt"
	"os"
	"testing"
)

// TestMain lets the integration suite run on plain PostgreSQL: with
// TB_SKIP_TIMESCALE=1 it points Open at the migrations minus their Timescale
// blocks. Production code never reads this variable.
func TestMain(m *testing.M) {
	if os.Getenv("TB_SKIP_TIMESCALE") == "1" {
		src, err := strippedMigrations()
		if err != nil {
			fmt.Fprintln(os.Stderr, "strip migrations:", err)
			os.Exit(1)
		}
		migrationSource = src
	}
	os.Exit(m.Run())
}
