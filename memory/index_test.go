package memory

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// scanSearch is the search that ran before the index: BM25 over every
// memory. It is the reference the index must match exactly.
func scanSearch(memories []Memory, query string, limit int) []int64 {
	terms := tokenize(query)
	if len(terms) == 0 {
		return nil
	}
	df := make(map[string]int)
	for _, m := range memories {
		seen := map[string]bool{}
		for _, term := range tokenize(m.Text + " " + strings.Join(m.Topics, " ")) {
			seen[term] = true
		}
		for term := range seen {
			df[term]++
		}
	}
	type hit struct {
		memory Memory
		score  float64
	}
	var hits []hit
	for _, m := range memories {
		words := tokenize(m.Text + " " + strings.Join(m.Topics, " "))
		tf := map[string]int{}
		for _, word := range words {
			tf[word]++
		}
		score := 0.0
		for _, term := range terms {
			if tf[term] == 0 {
				continue
			}
			idf := math.Log(1 + (float64(len(memories)-df[term])+0.5)/(float64(df[term])+0.5))
			score += idf * float64(tf[term]) * 2.2 / (float64(tf[term]) + 1.2*(0.25+0.75*float64(len(words))/80))
		}
		if score > 0 {
			hits = append(hits, hit{m, score})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		if hits[i].memory.Created != hits[j].memory.Created {
			return hits[i].memory.Created > hits[j].memory.Created
		}
		return hits[i].memory.ID > hits[j].memory.ID
	})
	var ids []int64
	for i := 0; i < len(hits) && i < limit; i++ {
		ids = append(ids, hits[i].memory.ID)
	}
	return ids
}

func searchIDs(t *testing.T, s *Store, query string, limit int) []int64 {
	t.Helper()
	found, err := s.Search(query, limit)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, m := range found {
		ids = append(ids, m.ID)
	}
	return ids
}

func checkMatchesScan(t *testing.T, s *Store, query string, limit int) {
	t.Helper()
	all, err := s.Memories()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := searchIDs(t, s, query, limit), scanSearch(all, query, limit); !slices.Equal(got, want) {
		t.Fatalf("search %q limit %d = %v, scan = %v", query, limit, got, want)
	}
}

// Mixed case, accents, non-Latin scripts, digits, and a small vocabulary so
// terms repeat across memories and scores tie.
var indexVocab = strings.Fields("go Go GO tests testing café CAFÉ naïve Straße straße ΣΊΣΥΦΟΣ σίσυφος 日本語 42 x1 deploy deploys the a git ci")

func randomWords(r *rand.Rand, n int) string {
	words := make([]string, n)
	for i := range words {
		words[i] = indexVocab[r.Intn(len(indexVocab))]
	}
	return strings.Join(words, []string{" ", ", ", "-", "/"}[r.Intn(4)])
}

// fillRandom remembers and forgets at random. Creation times often repeat so
// ties fall through to the id order.
func fillRandom(t *testing.T, s *Store, r *rand.Rand, n int) {
	t.Helper()
	defer func(old func() time.Time) { now = old }(now)
	for i := 0; i < n; i++ {
		created := int64(1_700_000_000 + r.Intn(5)*86400)
		now = func() time.Time { return time.Unix(created, 0) }
		var topics []string
		for j := r.Intn(3); j > 0; j-- {
			topics = append(topics, indexVocab[r.Intn(len(indexVocab))])
		}
		mustRemember(t, s, randomWords(r, 1+r.Intn(12)), topics...)
		if r.Intn(4) == 0 {
			if _, err := s.Forget(int64(1 + r.Intn(i+1))); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestIndexMatchesScan(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		r := rand.New(rand.NewSource(seed))
		withStore(t, testDB(t), func(s *Store) {
			fillRandom(t, s, r, r.Intn(60))
			for q := 0; q < 25; q++ {
				query := randomWords(r, 1+r.Intn(4))
				if q%7 == 0 {
					query += " nomatch"
				}
				checkMatchesScan(t, s, query, 1+r.Intn(20))
			}
			checkMatchesScan(t, s, "nomatch", 10)
			checkMatchesScan(t, s, "!!", 10)
		})
	}
}

// dumpIndex returns every index key and value.
func dumpIndex(t *testing.T, s *Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: postingPrefix, UpperBound: prefixEnd(postingPrefix)})
	if err != nil {
		t.Fatal(err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key())] = string(iter.Value())
	}
	iter.Close()
	for _, key := range [][]byte{docCountKey, indexVersionKey} {
		v, err := s.getUint(key)
		if err != nil {
			t.Fatal(err)
		}
		out[string(key)] = fmt.Sprint(v)
	}
	return out
}

// The index kept up by remember and forget equals one rebuilt from scratch.
func TestIndexRebuildEquivalent(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	withStore(t, testDB(t), func(s *Store) {
		fillRandom(t, s, r, 80)
		incremental := dumpIndex(t, s)
		if err := s.rebuildIndex(); err != nil {
			t.Fatal(err)
		}
		if rebuilt := dumpIndex(t, s); !maps.Equal(incremental, rebuilt) {
			t.Fatalf("rebuilt index differs: %d keys incremental, %d rebuilt", len(incremental), len(rebuilt))
		}
		all, _ := s.Memories()
		if got := dumpIndex(t, s)[string(docCountKey)]; got != fmt.Sprint(len(all)) {
			t.Fatalf("doc count %s, %d memories", got, len(all))
		}
	})
}

func TestForgetRemovesPostings(t *testing.T) {
	withStore(t, testDB(t), func(s *Store) {
		id := mustRemember(t, s, "Only memory mentioning zebras", "Animals")
		if ok, err := s.Forget(id); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if index := dumpIndex(t, s); len(index) != 2 || index[string(docCountKey)] != "0" {
			t.Fatalf("index after forgetting the only memory = %v", index)
		}
	})
}

// An index of another version, or with stray postings, is rebuilt by the next
// open, including a read-only one.
func TestStaleIndexRebuilt(t *testing.T) {
	path := t.TempDir() + "/memory.pebble"
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, db, func(s *Store) { mustRemember(t, s, "kept memory about pebble", "storage") })
	raw, err := pebble.Open(path, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = raw.Set(postingKey("pebble", 999), []byte{1, 1, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err = raw.Set(indexVersionKey, uint64Bytes(indexVersion-1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	out, err := db.View(func(s *Store) (string, error) { return callTool(s, "recall", map[string]any{"query": "pebble"}) })
	if err != nil || !strings.Contains(out, "kept memory") {
		t.Fatalf("recall = %q, %v", out, err)
	}
	_, err = db.View(func(s *Store) (string, error) {
		if v, _ := s.getUint(indexVersionKey); v != indexVersion {
			t.Fatalf("index version = %d", v)
		}
		if _, closer, err := s.db.Get(postingKey("pebble", 999)); err == nil {
			closer.Close()
			t.Fatal("stray posting survived the rebuild")
		}
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// testdata/v2.pebble was written by the v2 server (commit e8ec32c). Opening it
// adds the index and changes nothing else.
func TestUpgradeV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.pebble")
	if err := os.CopyFS(path, os.DirFS("testdata/v2.pebble")); err != nil {
		t.Fatal(err)
	}
	if v := rawSchemaVersion(t, path); v != "2" {
		t.Fatalf("fixture schema = %s", v)
	}
	before := graphKeys(t, path)
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	if v := rawSchemaVersion(t, path); v != fmt.Sprint(schemaVersion) {
		t.Fatalf("schema after upgrade = %s", v)
	}
	if after := graphKeys(t, path); !maps.Equal(before, after) {
		t.Fatal("upgrading to v3 changed graph records")
	}
	if backups, _ := filepath.Glob(path + ".v*-backup-*"); len(backups) != 0 {
		t.Fatalf("index-only upgrade made a backup: %v", backups)
	}
	withStore(t, db, func(s *Store) {
		for _, q := range []string{"tests", "café", "CAFÉ noir", "ci", "go", "coffee", "plain git"} {
			checkMatchesScan(t, s, q, 10)
		}
		if ids := searchIDs(t, s, "café", 10); !slices.Equal(ids, []int64{7}) {
			t.Fatalf("café = %v", ids)
		}
	})
}

// graphKeys returns every non-index key and value, read straight from Pebble.
func graphKeys(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := pebble.Open(path, &pebble.Options{Logger: quietLogger{}, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	iter, _ := raw.NewIter(nil)
	defer iter.Close()
	out := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if bytes.HasPrefix(key, postingPrefix) || bytes.HasPrefix(key, []byte("meta/")) {
			continue
		}
		out[string(key)] = string(iter.Value())
	}
	return out
}
