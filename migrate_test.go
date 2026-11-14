package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// copyFixture copies testdata/v1.pebble, written by the v1 server (commit
// 239bf05), so each test migrates its own copy.
func copyFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.pebble")
	if err := os.CopyFS(path, os.DirFS("testdata/v1.pebble")); err != nil {
		t.Fatal(err)
	}
	return path
}

func rawSchemaVersion(t *testing.T, path string) string {
	t.Helper()
	raw, err := pebble.Open(path, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	value, closer, err := raw.Get(schemaVersionKey)
	if err != nil {
		return "none"
	}
	defer closer.Close()
	return fmt.Sprint(value[7])
}

func checkMigrated(t *testing.T, db *Db) {
	t.Helper()
	withStore(t, db, func(s *Store) {
		all, err := s.Memories()
		if err != nil {
			t.Fatal(err)
		}
		want := []struct {
			id     int64
			text   string
			topics []string
		}{
			{1, "Prefers table-driven tests", []string{"testing", "go"}},
			{2, "CI runs on GitHub Actions", []string{"ci/cd", "ci"}},
			{3, "No topics on this one", nil},
			{5, "Order is kept: zeta before alpha", []string{"zeta", "alpha", "go"}},
			{6, "Uses plain git", []string{"git", "ci"}},
		}
		if len(all) != len(want) {
			t.Fatalf("%d memories after migration, want %d: %v", len(all), len(want), all)
		}
		for i, w := range want {
			m := all[i]
			if m.ID != w.id || m.Text != w.text || !slices.Equal(m.Topics, w.topics) || m.Created == 0 {
				t.Fatalf("memory %d = %+v, want %+v", i, m, w)
			}
		}
		about := func(topic string) (ids []int64) {
			found, err := s.About(topic, 10)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range found {
				ids = append(ids, m.ID)
			}
			return ids
		}
		// In v1, "e/ci/" also matched the keys of topic "ci/cd".
		if got := about("ci"); !slices.Equal(got, []int64{6, 2}) {
			t.Fatalf("about ci = %v", got)
		}
		if got := about("ci/cd"); !slices.Equal(got, []int64{2}) {
			t.Fatalf("about ci/cd = %v", got)
		}
		if got := about("scratch"); len(got) != 0 {
			t.Fatalf("about forgotten topic = %v", got)
		}
		topics, err := s.Topics("")
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(topics); got != "[[ci 2] [go 2] [alpha 1] [ci/cd 1] [git 1] [testing 1] [zeta 1]]" {
			t.Fatalf("topics = %s (orphaned v1 topic nodes must be dropped)", got)
		}
		if id := mustRemember(t, s, "after migration", "go"); id != 7 {
			t.Fatalf("next id = %d, want 7", id)
		}
	})
}

func TestMigrateV1Fixture(t *testing.T) {
	path := copyFixture(t)
	if v := rawSchemaVersion(t, path); v != "1" {
		t.Fatalf("fixture schema = %s, want 1", v)
	}
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	if v := rawSchemaVersion(t, path); v != fmt.Sprint(schemaVersion) {
		t.Fatalf("schema after migration = %s", v)
	}
	checkMigrated(t, db)

	// The v1 data is kept, untouched, beside the database.
	backups, _ := filepath.Glob(path + ".v1-backup-*")
	if len(backups) != 1 {
		t.Fatalf("backups = %v", backups)
	}
	if v := rawSchemaVersion(t, backups[0]); v != "1" {
		t.Fatalf("backup schema = %s, want 1", v)
	}

	// Later opens don't migrate or back up again.
	if _, err = NewDb(path); err != nil {
		t.Fatal(err)
	}
	if again, _ := filepath.Glob(path + ".v1-backup-*"); len(again) != 1 {
		t.Fatalf("backups after reopen = %v", again)
	}
}

// Databases from before the schema version key are v1 too.
func TestMigrateUnversionedV1(t *testing.T) {
	path := copyFixture(t)
	raw, err := pebble.Open(path, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = raw.Delete(schemaVersionKey, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	checkMigrated(t, db)
}

// Opening through the MCP server migrates on the first tool call, and every
// tool answers as v1 did over the same data.
func TestMigratedServesSameOutput(t *testing.T) {
	db, err := NewDb(copyFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	out, err := db.With(func(s *Store) (string, error) { return callTool(s, "recall", map[string]any{"topic": "go"}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "[zeta, alpha, go] Order is kept") || !strings.Contains(out, "[testing, go] Prefers") {
		t.Fatalf("recall = %q", out)
	}
}

// A read-only call that meets a v1 database opens read-write and migrates.
func TestViewMigrates(t *testing.T) {
	path := copyFixture(t)
	db := &Db{path: path} // skip NewDb, which would migrate first
	out, err := db.View(func(s *Store) (string, error) { return callTool(s, "topics", map[string]any{}) })
	if err != nil || !strings.HasPrefix(out, "ci (2)\ngo (2)") {
		t.Fatalf("topics = %q, %v", out, err)
	}
	if v := rawSchemaVersion(t, path); v != fmt.Sprint(schemaVersion) {
		t.Fatalf("schema after View = %s", v)
	}
}
