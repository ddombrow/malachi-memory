package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/cockroachdb/pebble"
)

// Memory is a graph node containing a durable fact and links to topic nodes.
type Memory struct {
	ID      int64    `json:"id"`
	Text    string   `json:"text"`
	Topics  []string `json:"topics"`
	Created int64    `json:"created"`
}

type Store struct{ db *pebble.DB }
type Db struct{ path string }

func NewDb(path string) (*Db, error) {
	if path == "" {
		return nil, errors.New("database path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db := &Db{path: path}
	// Open once to create the database and check its schema, waiting like any
	// other call if another server process holds the lock.
	if _, err := db.With(func(*Store) (string, error) { return "", nil }); err != nil {
		return nil, err
	}
	return db, nil
}

// Backup writes a consistent copy of the database to dir, which must not
// exist. The copy is itself a Pebble database and can replace the original.
func (d *Db) Backup(dir string) error {
	_, err := d.With(func(s *Store) (string, error) { return "", s.db.Checkpoint(dir, pebble.WithFlushedWAL()) })
	return err
}

// Pebble uses an exclusive database lock. Opening around each tool call lets
// separate server processes take turns using the same database.
func (d *Db) With(f func(*Store) (string, error)) (string, error) {
	deadline := time.Now().Add(lockWait)
	for {
		s, err := d.open()
		if err == nil {
			result, callErr := f(s)
			closeErr := s.Close()
			if callErr != nil {
				return "", callErr
			}
			if closeErr != nil {
				return "", closeErr
			}
			return result, nil
		}
		if !isLocked(err) {
			return "", err
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("memory database is busy: another session held it for over %s", lockWait)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

const lockWait = 5 * time.Second

// isLocked reports whether a Pebble open failed because the database lock is
// held: by another process (fcntl returns EAGAIN or EACCES) or by this one.
func isLocked(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EACCES) ||
		strings.Contains(err.Error(), "lock held by current process")
}

// quietLogger drops Pebble's informational messages, such as WAL replay on
// every open, which would otherwise fill the MCP client's stderr log.
type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{}) {}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "malachi-memory: pebble: "+format+"\n", args...)
	os.Exit(1)
}

func (d *Db) open() (*Store, error) {
	db, err := pebble.Open(d.path, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err = s.checkSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// schemaVersion is the on-disk key layout this server reads and writes.
// v1: m/<id> memory JSON (with topics), t/<topic>, e/<topic>/<id>, meta/next-id.
const schemaVersion = 1

func (s *Store) checkSchema() error {
	value, closer, err := s.db.Get(schemaVersionKey)
	if errors.Is(err, pebble.ErrNotFound) {
		// New databases, and those written before versioning, use the v1 layout.
		return s.db.Set(schemaVersionKey, uint64Bytes(schemaVersion), pebble.Sync)
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	if len(value) != 8 {
		return errors.New("database schema version is corrupt")
	}
	if v := binary.BigEndian.Uint64(value); v != schemaVersion {
		return fmt.Errorf("database schema v%d is not supported by this server (v%d)", v, schemaVersion)
	}
	return nil
}

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (s *Store) Close() error { return s.db.Close() }

var (
	memoryPrefix = []byte("m/")
	topicPrefix  = []byte("t/")
	edgePrefix   = []byte("e/")
	nextIDKey    = []byte("meta/next-id")

	schemaVersionKey = []byte("meta/schema-version")
)

// now is replaced in tests for deterministic creation times.
var now = time.Now

func memoryKey(id int64) []byte {
	key := make([]byte, len(memoryPrefix)+8)
	copy(key, memoryPrefix)
	binary.BigEndian.PutUint64(key[len(memoryPrefix):], uint64(id))
	return key
}
func edgeKey(id int64, topic string) []byte {
	key := make([]byte, len(edgePrefix)+len(topic)+1+8)
	copy(key, edgePrefix)
	copy(key[len(edgePrefix):], topic)
	key[len(edgePrefix)+len(topic)] = '/'
	binary.BigEndian.PutUint64(key[len(edgePrefix)+len(topic)+1:], uint64(id))
	return key
}
func (s *Store) Remember(text string, topics []string) (int64, error) {
	return s.remember(text, topics, now().Unix(), pebble.Sync)
}

func (s *Store) remember(text string, topics []string, created int64, opts *pebble.WriteOptions) (int64, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, errors.New("text is empty")
	}
	topics = normalizeTopics(topics)
	id := int64(1)
	if value, closer, err := s.db.Get(nextIDKey); err == nil {
		if len(value) == 8 {
			id = int64(binary.BigEndian.Uint64(value))
		}
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}
	m := Memory{ID: id, Text: text, Topics: topics, Created: created}
	encoded, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = batch.Set(memoryKey(id), encoded, nil); err != nil {
		return 0, err
	}
	for _, topic := range topics {
		if err = batch.Set(append(append([]byte(nil), topicPrefix...), []byte(topic)...), []byte(topic), nil); err != nil {
			return 0, err
		}
		if err = batch.Set(edgeKey(id, topic), nil, nil); err != nil {
			return 0, err
		}
	}
	if err = batch.Set(nextIDKey, uint64Bytes(uint64(id+1)), nil); err != nil {
		return 0, err
	}
	if err = batch.Commit(opts); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) Get(id int64) (*Memory, error) {
	value, closer, err := s.db.Get(memoryKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var m Memory
	if err = json.Unmarshal(value, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Store) Forget(id int64) (bool, error) {
	m, err := s.Get(id)
	if err != nil || m == nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = batch.Delete(memoryKey(id), nil); err != nil {
		return false, err
	}
	for _, topic := range m.Topics {
		if err = batch.Delete(edgeKey(id, topic), nil); err != nil {
			return false, err
		}
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Memories() ([]Memory, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: memoryPrefix, UpperBound: prefixEnd(memoryPrefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []Memory
	for iter.First(); iter.Valid(); iter.Next() {
		var m Memory
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, iter.Error()
}

func (s *Store) Search(query string, limit int) ([]Memory, error) {
	memories, err := s.Memories()
	if err != nil {
		return nil, err
	}
	terms := tokenize(query)
	if len(terms) == 0 {
		return nil, nil
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
		if hits[i].score == hits[j].score {
			return hits[i].memory.Created > hits[j].memory.Created
		}
		return hits[i].score > hits[j].score
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	out := make([]Memory, len(hits))
	for i, h := range hits {
		out[i] = h.memory
	}
	return out, nil
}

func (s *Store) About(topic string, limit int) ([]Memory, error) {
	topic = strings.ToLower(strings.TrimSpace(topic))
	prefix := append(append([]byte(nil), edgePrefix...), []byte(topic+"/")...)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := make([]Memory, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(prefix)+8 {
			continue
		}
		id := int64(binary.BigEndian.Uint64(key[len(prefix):]))
		m, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		if m != nil {
			out = append(out, *m)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created > out[j].Created })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) Recent(limit int) ([]Memory, error) {
	out, err := s.Memories()
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created > out[j].Created })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) Topics(relatedTo string) ([][2]string, error) {
	memories, err := s.Memories()
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	relatedTo = strings.ToLower(strings.TrimSpace(relatedTo))
	for _, m := range memories {
		if relatedTo != "" {
			found := false
			for _, t := range m.Topics {
				if t == relatedTo {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		for _, t := range m.Topics {
			counts[t]++
		}
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] == counts[names[j]] {
			return names[i] < names[j]
		}
		return counts[names[i]] > counts[names[j]]
	})
	out := make([][2]string, 0, len(names))
	for _, name := range names {
		out = append(out, [2]string{name, fmt.Sprint(counts[name])})
	}
	return out, nil
}

func normalizeTopics(topics []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(topics))
	for _, topic := range topics {
		topic = strings.ToLower(strings.TrimSpace(topic))
		if topic != "" && !seen[topic] {
			seen[topic] = true
			out = append(out, topic)
		}
	}
	return out
}
func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
}
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
