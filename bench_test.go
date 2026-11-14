package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// Benchmarks time whole tool calls through Db.With, including the per-call
// Pebble open, at store sizes that show how the full scans grow.
//
//	go test -run '^$' -bench . -benchtime 20x
var benchSizes = []int{1_000, 10_000, 100_000}

var benchWords = func() []string {
	r := rand.New(rand.NewSource(1))
	words := make([]string, 5000)
	for i := range words {
		var b strings.Builder
		for j := 0; j < 4+r.Intn(6); j++ {
			b.WriteByte(byte('a' + r.Intn(26)))
		}
		words[i] = b.String()
	}
	return words
}()

// seed fills a database with n memories of about 15 words and 1-3 of 50
// topics. Writes skip fsync so large stores build quickly.
func seed(b *testing.B, n int) *Db {
	b.Helper()
	db, err := NewDb(b.TempDir() + "/memory.pebble")
	if err != nil {
		b.Fatal(err)
	}
	r := rand.New(rand.NewSource(int64(n)))
	_, err = db.With(func(s *Store) (string, error) {
		for i := 0; i < n; i++ {
			words := make([]string, 10+r.Intn(10))
			for j := range words {
				words[j] = benchWords[int(r.ExpFloat64()*500)%len(benchWords)]
			}
			topics := make([]string, 1+r.Intn(3))
			for j := range topics {
				topics[j] = fmt.Sprintf("topic%d", r.Intn(50))
			}
			if _, err := s.remember(strings.Join(words, " "), topics, int64(1_700_000_000+i), pebble.NoSync); err != nil {
				return "", err
			}
		}
		return "", s.db.Flush()
	})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkTools(b *testing.B) {
	calls := []struct {
		name string
		args map[string]any
	}{
		{"remember", map[string]any{"text": "benchmark memory about deploys", "topics": []any{"topic1", "bench"}}},
		{"recall/query", map[string]any{"query": benchWords[3] + " " + benchWords[40] + " " + benchWords[400]}},
		{"recall/topic", map[string]any{"topic": "topic7"}},
		{"recall/recent", map[string]any{}},
		{"topics", map[string]any{}},
		{"topics/related", map[string]any{"related_to": "topic7"}},
	}
	for _, n := range benchSizes {
		db := seed(b, n)
		for _, c := range calls {
			tool := strings.SplitN(c.name, "/", 2)[0]
			b.Run(fmt.Sprintf("%s/n=%d", c.name, n), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					open := db.With
					if readOnlyTool(tool) {
						open = db.View
					}
					if _, err := open(func(s *Store) (string, error) { return callTool(s, tool, c.args) }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		for _, mode := range []struct {
			name string
			open func(func(*Store) (string, error)) (string, error)
		}{{"open-close/read-write", db.With}, {"open-close/read-only", db.View}} {
			b.Run(fmt.Sprintf("%s/n=%d", mode.name, n), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := mode.open(func(*Store) (string, error) { return "", nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
