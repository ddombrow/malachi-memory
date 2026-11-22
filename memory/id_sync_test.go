package memory

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestIDStringRoundTrip(t *testing.T) {
	legacy, err := legacyID(1_700_000_000, 42, "text", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []ID{{}, {15: 1}, {0: 0xff, 15: 0xfe}, legacy} {
		s := id.String()
		if len(s) != 26 {
			t.Fatalf("%x encodes to %q, want 26 characters", id, s)
		}
		back, err := ParseID(strings.ToLower(s))
		if err != nil || back != id {
			t.Fatalf("parse %q = %x, %v; want %x", s, back, err, id)
		}
	}
	for _, bad := range []string{"", "short", strings.Repeat("U", 26), strings.Repeat("Z", 26), "8ZZZZZZZZZZZZZZZZZZZZZZZZZ"} {
		if _, err := ParseID(bad); err == nil {
			t.Fatalf("ParseID(%q) succeeded", bad)
		}
	}
}

func TestNewIDOrdersByTimeAndCount(t *testing.T) {
	defer func(old io.Reader, ms uint64, last ID) { randomSource, lastMS, lastID = old, ms, last }(randomSource, lastMS, lastID)
	randomSource = &repeatingReader{}
	lastMS, lastID = 0, ID{}
	later := newID(1_700_000_100)
	first := newID(1_700_000_000)
	second := newID(1_700_000_000)
	if !first.Less(second) || !second.Less(later) {
		t.Fatalf("ids out of order: %s %s %s", first, second, later)
	}
}

func TestLegacyIDsKeepOrder(t *testing.T) {
	id := func(created, old int64) ID {
		v, err := legacyID(created, old, "text", nil)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if !id(1_700_000_000, 2).Less(id(1_700_000_000, 3)) {
		t.Fatal("same-second legacy ids lose their order")
	}
	if !id(1_700_000_000, 9).Less(id(1_700_000_001, 1)) {
		t.Fatal("legacy ids do not sort by creation time")
	}
}

// Two stores that each numbered a memory #1 in the same second must not give
// them the same id, or a merge would drop one of them.
func TestLegacyIDsDoNotCollideAcrossStores(t *testing.T) {
	a, err := legacyID(1_700_000_000, 1, "Uses tabs", []string{"style"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := legacyID(1_700_000_000, 1, "Uses spaces", []string{"style"})
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("different memories share legacy id %s", a)
	}
	again, _ := legacyID(1_700_000_000, 1, "Uses tabs", []string{"style"})
	if again != a {
		t.Fatal("legacy id is not deterministic")
	}
	if _, err := legacyID(1, 1<<48, "x", nil); err == nil {
		t.Fatal("oversized memory number accepted")
	}
}

// End to end: two v1-style stores whose memories collide on number and second
// both keep their memory after a merge.
func TestMergeMigratedStoresKeepCollidingMemories(t *testing.T) {
	storeA := testDB(t)
	storeB := testDB(t)
	for _, c := range []struct {
		db   *Db
		text string
	}{{storeA, "Uses tabs"}, {storeB, "Uses spaces"}} {
		id, err := legacyID(1_700_000_000, 1, c.text, []string{"style"})
		if err != nil {
			t.Fatal(err)
		}
		withStore(t, c.db, func(s *Store) {
			batch := s.db.NewBatch()
			defer batch.Close()
			if err := s.putMemory(batch, memoryRecord{ID: id, Text: c.text, Created: 1_700_000_000}, []string{"style"}); err != nil {
				t.Fatal(err)
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				t.Fatal(err)
			}
		})
	}
	merged := testDB(t)
	for _, src := range []*Db{storeA, storeB} {
		if _, _, err := merged.Import(strings.NewReader(exportString(t, src))); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(liveIDs(t, merged)); n != 2 {
		t.Fatalf("%d memories after merging colliding stores, want 2", n)
	}
}

// newStore returns a store with the given memories, each on its own topic.
func newStore(t *testing.T, texts ...string) (*Db, []ID) {
	t.Helper()
	db := testDB(t)
	var ids []ID
	withStore(t, db, func(s *Store) {
		for _, text := range texts {
			ids = append(ids, mustRemember(t, s, text, "shared"))
		}
	})
	return db, ids
}

func exportString(t *testing.T, db *Db) string {
	t.Helper()
	var buf bytes.Buffer
	if err := db.Export(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func liveIDs(t *testing.T, db *Db) []ID {
	t.Helper()
	var ids []ID
	withStore(t, db, func(s *Store) {
		all, err := s.Memories()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range all {
			ids = append(ids, m.ID)
		}
	})
	return ids
}

func TestExportImportRoundTrip(t *testing.T) {
	src, ids := newStore(t, "alpha fact", "beta fact", "gamma fact")
	withStore(t, src, func(s *Store) {
		if ok, err := s.Forget(ids[1]); err != nil || !ok {
			t.Fatal(ok, err)
		}
	})
	stream := exportString(t, src)

	dst := testDB(t)
	added, deleted, err := dst.Import(strings.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	if added != 2 || deleted != 1 {
		t.Fatalf("import added %d, deleted %d; want 2, 1", added, deleted)
	}
	if got, want := liveIDs(t, dst), liveIDs(t, src); !slices.Equal(got, want) {
		t.Fatalf("live ids after import = %v, want %v", got, want)
	}
	if got := exportString(t, dst); got != stream {
		t.Fatalf("re-export differs:\n got: %s\nwant: %s", got, stream)
	}
}

// Both orders of two stores' changes converge: a memory added on one side and
// forgotten on the other stays forgotten.
func TestMergeConvergesInEitherOrder(t *testing.T) {
	base, ids := newStore(t, "kept", "forgotten later", "added elsewhere")
	shared := exportString(t, base)

	a := testDB(t)
	b := testDB(t)
	if _, _, err := a.Import(strings.NewReader(shared)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Import(strings.NewReader(shared)); err != nil {
		t.Fatal(err)
	}
	withStore(t, a, func(s *Store) {
		if ok, err := s.Forget(ids[1]); err != nil || !ok {
			t.Fatal(ok, err)
		}
	})
	withStore(t, b, func(s *Store) { mustRemember(t, s, "new on b", "shared") })

	ab, ba := testDB(t), testDB(t)
	for _, step := range []struct {
		dst *Db
		src *Db
	}{{ab, a}, {ab, b}} {
		if _, _, err := step.dst.Import(strings.NewReader(exportString(t, step.src))); err != nil {
			t.Fatal(err)
		}
	}
	for _, step := range []struct {
		dst *Db
		src *Db
	}{{ba, b}, {ba, a}} {
		if _, _, err := step.dst.Import(strings.NewReader(exportString(t, step.src))); err != nil {
			t.Fatal(err)
		}
	}
	if x, y := exportString(t, ab), exportString(t, ba); x != y {
		t.Fatalf("merge orders differ:\nab: %s\nba: %s", x, y)
	}
	for _, id := range liveIDs(t, ab) {
		if id == ids[1] {
			t.Fatal("forgotten memory came back after merge")
		}
	}
	if n := len(liveIDs(t, ab)); n != 3 {
		t.Fatalf("%d live memories after merge, want 3", n)
	}
}

// A tombstone that arrives before its memory still blocks the memory.
func TestTombstoneBeforeMemory(t *testing.T) {
	src, ids := newStore(t, "to delete")
	// The memory line comes from a copy taken before the forget, as a peer
	// that never saw the deletion would send it.
	memoryLine := exportString(t, src)
	withStore(t, src, func(s *Store) {
		if ok, err := s.Forget(ids[0]); err != nil || !ok {
			t.Fatal(ok, err)
		}
	})
	tombLine := ""
	for _, line := range strings.Split(strings.TrimSpace(exportString(t, src)), "\n") {
		if strings.Contains(line, `"tombstone"`) {
			tombLine = line + "\n"
		}
	}
	if tombLine == "" {
		t.Fatal("export has no tombstone")
	}
	dst := testDB(t)
	if _, _, err := dst.Import(strings.NewReader(tombLine)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dst.Import(strings.NewReader(memoryLine)); err != nil {
		t.Fatal(err)
	}
	if n := len(liveIDs(t, dst)); n != 0 {
		t.Fatalf("%d memories after the tombstone arrived first, want 0", n)
	}
}

func TestImportRejectsBadLines(t *testing.T) {
	db := testDB(t)
	for _, bad := range []string{
		`{"memory":{"id":"` + ID{15: 1}.String() + `","text":"","topics":[],"created":1}}` + "\n",
		`{}` + "\n",
		`{"memory":{"id":"` + ID{15: 1}.String() + `","text":"x"},"tombstone":{"id":"` + ID{15: 1}.String() + `","deleted_at":1}}` + "\n",
		"not json\n",
	} {
		if _, _, err := db.Import(strings.NewReader(bad)); err == nil {
			t.Fatalf("import accepted %q", bad)
		}
	}
}

// The v3 fixture is a database written by the server before ULID ids. Its
// memories must come through with their text, topics and order intact.
func TestMigrateV3Fixture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.pebble")
	if err := copyV3Fixture(path); err != nil {
		t.Fatal(err)
	}
	if v := rawSchemaVersion(t, path); v != "3" {
		t.Fatalf("fixture schema = %s, want 3", v)
	}
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	if v := rawSchemaVersion(t, path); v != fmt.Sprint(schemaVersion) {
		t.Fatalf("schema after migration = %s", v)
	}
	withStore(t, db, func(s *Store) {
		all, err := s.Memories()
		if err != nil {
			t.Fatal(err)
		}
		var texts []string
		for _, m := range all {
			texts = append(texts, m.Text+" "+strings.Join(m.Topics, "+"))
		}
		want := []string{
			"Prefers table-driven tests testing+go",
			"CI runs on GitHub Actions ci/cd+ci",
			"No topics on this one ",
			"Order is kept: zeta before alpha zeta+alpha+go",
			"Uses plain git git+ci",
		}
		if !slices.Equal(texts, want) {
			t.Fatalf("migrated memories = %q, want %q", texts, want)
		}
		for i := 1; i < len(all); i++ {
			if !all[i-1].ID.Less(all[i].ID) {
				t.Fatalf("migrated ids are not in order: %s, %s", all[i-1].ID, all[i].ID)
			}
		}
	})
}

// copyV3Fixture copies testdata/v3.pebble, written by the schema v3 server
// before ids became ULIDs, so each test migrates its own copy.
func copyV3Fixture(path string) error {
	return os.CopyFS(path, os.DirFS("testdata/v3.pebble"))
}

// Two replicas forget the same memory at different times. Whichever merge
// order is used, both end with the same bytes, and the earlier deletion time.
func TestConcurrentForgetConverges(t *testing.T) {
	base, ids := newStore(t, "shared fact")
	shared := exportString(t, base)
	a, b := testDB(t), testDB(t)
	for _, db := range []*Db{a, b} {
		if _, _, err := db.Import(strings.NewReader(shared)); err != nil {
			t.Fatal(err)
		}
	}
	old := now
	defer func() { now = old }()
	now = func() time.Time { return time.Unix(1_800_000_500, 0) }
	withStore(t, a, func(s *Store) { s.Forget(ids[0]) })
	now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	withStore(t, b, func(s *Store) { s.Forget(ids[0]) })

	ab, ba := testDB(t), testDB(t)
	for _, src := range []*Db{a, b} {
		if _, _, err := ab.Import(strings.NewReader(exportString(t, src))); err != nil {
			t.Fatal(err)
		}
	}
	for _, src := range []*Db{b, a} {
		if _, _, err := ba.Import(strings.NewReader(exportString(t, src))); err != nil {
			t.Fatal(err)
		}
	}
	x, y := exportString(t, ab), exportString(t, ba)
	if x != y {
		t.Fatalf("merge orders differ:\nab: %s\nba: %s", x, y)
	}
	if !strings.Contains(x, `"deleted_at":1800000000`) {
		t.Fatalf("merged tombstone does not keep the earlier time: %s", x)
	}
	if n := len(liveIDs(t, ab)); n != 0 {
		t.Fatalf("%d live memories, want 0", n)
	}
}
