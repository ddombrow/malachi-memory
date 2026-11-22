package memory

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/cockroachdb/pebble"
)

// Memory is a memory node with the topics its ABOUT edges point to, in the
// order they were given.
type Memory struct {
	ID      ID       `json:"id"`
	Text    string   `json:"text"`
	Topics  []string `json:"topics"`
	Created int64    `json:"created"`
}

// Store is an open database; path is kept for the backup a migration takes.
type Store struct {
	db   *pebble.DB
	path string
}
type Db struct{ path string }

func NewDb(path string) (*Db, error) {
	if path == "" {
		return nil, errors.New("database path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := ensurePrivateDir(path); err != nil {
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

func ensurePrivateDir(path string) error {
	if err := os.Mkdir(path, 0o700); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("database path %q is not a directory", path)
	}
	return os.Chmod(path, 0o700)
}

// Export writes the database as a sync stream (see sync.go).
func (d *Db) Export(w io.Writer) error {
	_, err := d.View(func(s *Store) (string, error) { return "", s.Export(w) })
	return err
}

// Import merges a sync stream into the database and reports what changed.
func (d *Db) Import(r io.Reader) (added, deleted int, err error) {
	_, err = d.With(func(s *Store) (string, error) {
		added, deleted, err = s.Import(r)
		return "", err
	})
	return added, deleted, err
}

// Backup writes a consistent copy of the database to dir, which must not
// exist. The copy is itself a Pebble database and can replace the original.
func (d *Db) Backup(dir string) error {
	_, err := d.With(func(s *Store) (string, error) {
		if err := s.db.Checkpoint(dir, pebble.WithFlushedWAL()); err != nil {
			return "", err
		}
		return "", os.Chmod(dir, 0o700)
	})
	return err
}

// Pebble uses an exclusive database lock. Opening around each tool call lets
// separate server processes take turns using the same database.
//
// With opens the database read-write. A read-write open costs about 30 ms,
// almost all of it syncs for the new WAL, manifest and options files.
func (d *Db) With(f func(*Store) (string, error)) (string, error) { return d.run(false, f) }

// View opens the database read-only, which writes and syncs nothing: under
// 1 ms, and the lock is held only that long. Writes through it fail with
// pebble.ErrReadOnly. If the schema needs migrating, View opens read-write
// instead so the migration runs.
func (d *Db) View(f func(*Store) (string, error)) (string, error) { return d.run(true, f) }

func (d *Db) run(readOnly bool, f func(*Store) (string, error)) (string, error) {
	deadline := time.Now().Add(lockWait)
	for {
		s, err := d.open(readOnly)
		if errors.Is(err, errNeedsMigration) {
			readOnly = false
			continue
		}
		if err == nil {
			return runAndClose(s, f)
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

// runAndClose runs f and closes the store even if f panics, so a program
// that recovers the panic doesn't leave the database lock held.
func runAndClose(s *Store, f func(*Store) (string, error)) (result string, err error) {
	defer func() {
		if closeErr := s.Close(); err == nil && closeErr != nil {
			result, err = "", closeErr
		}
	}()
	return f(s)
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

// Fatalf must not return. It panics rather than calling os.Exit, so deferred
// cleanup runs and the stack shows where Pebble failed. A failure in one of
// Pebble's background goroutines still ends the process: nothing can recover
// a panic there.
func (quietLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf("malachi-memory: pebble: "+format, args...))
}

// lBaseMaxBytes keeps Pebble's base level small. Each read-write open
// flushes the last call's few keys to a new L0 file, and because postings
// span the whole key range that file overlaps everything below it. With the
// default 64 MB base level, a whole store under 64 MB sits in one level, so
// every second write compacted all of it (31 MB at 100k memories, ~380 ms
// while holding the lock). A small base level absorbs those writes, so
// remember costs ~43 ms at any size; search pays ~0.4 ms for the extra level.
const lBaseMaxBytes = 1 << 20

func (d *Db) open(readOnly bool) (*Store, error) {
	db, err := pebble.Open(d.path, &pebble.Options{Logger: quietLogger{}, ReadOnly: readOnly, LBaseMaxBytes: lBaseMaxBytes})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, path: d.path}
	if err = s.checkSchema(readOnly); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// The database is a small property graph in ordered keys. Graph records are
// authoritative; nothing else repeats them.
//
// Schema v4 (v3 with ULID memory ids, plus tombstones; the search index, whose
// p/ and meta/ keys are in index.go, is derived):
//
//	m/<id>               memory node: JSON {id, text, created}
//	t/<topic>            topic node: number of memories about it (uint64)
//	e/<topic>\x00<id>    ABOUT edge, topic -> memory (empty value)
//	r/<id><topic>        ABOUT edge, memory -> topic: its position (uint64)
//	x/<id>               tombstone of a forgotten memory: JSON {deleted_at}
//	meta/schema-version  schema version (uint64)
//
// Ids are 16 bytes (see id.go), so memories iterate oldest first and the edges
// of one topic or one memory are contiguous. Topic names cannot contain
// control characters, so the \x00 after a topic name ends it unambiguously.
// Tombstones never expire: an id is never reused, so a deleted memory cannot
// come back when stores are merged (see sync.go).
const schemaVersion = 4

var (
	memoryPrefix    = []byte("m/")
	topicPrefix     = []byte("t/")
	edgePrefix      = []byte("e/")
	reversePrefix   = []byte("r/")
	tombstonePrefix = []byte("x/")

	schemaVersionKey = []byte("meta/schema-version")
)

// now is replaced in tests for deterministic creation times.
var now = time.Now

// memoryRecord is the stored form of a memory node.
type memoryRecord struct {
	ID      ID     `json:"id"`
	Text    string `json:"text"`
	Created int64  `json:"created"`
}

// tombstoneRecord is the stored form of a forgotten memory's tombstone.
type tombstoneRecord struct {
	DeletedAt int64 `json:"deleted_at"`
}

// errNeedsMigration means a read-only open found a database that must be
// written before use: new, unversioned, or an older schema.
var errNeedsMigration = errors.New("database schema needs a read-write open")

func (s *Store) checkSchema(readOnly bool) error {
	value, closer, err := s.db.Get(schemaVersionKey)
	if errors.Is(err, pebble.ErrNotFound) {
		if readOnly {
			return errNeedsMigration
		}
		empty, err := s.isEmpty()
		if err != nil {
			return err
		}
		if !empty {
			// Written before versioning: the v1 layout.
			return s.migrateV1()
		}
		return s.rebuildIndex() // stamps the versions on a new database
	}
	if err != nil {
		return err
	}
	if len(value) != 8 {
		closer.Close()
		return errors.New("database schema version is corrupt")
	}
	v := binary.BigEndian.Uint64(value)
	closer.Close()
	switch v {
	case schemaVersion:
		return s.checkIndex(readOnly)
	case 3, 2:
		// Both keep integer ids; v4 replaces them with ULIDs and adds tombstones.
		if readOnly {
			return errNeedsMigration
		}
		return s.migrateV3(v)
	case 1:
		if readOnly {
			return errNeedsMigration
		}
		return s.migrateV1()
	default:
		return fmt.Errorf("database schema v%d is not supported by this server (v%d)", v, schemaVersion)
	}
}

func (s *Store) isEmpty() (bool, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return !iter.First(), iter.Error()
}

func (s *Store) Close() error { return s.db.Close() }

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func memoryKey(id ID) []byte       { return concat(memoryPrefix, id[:]) }
func topicKey(topic string) []byte { return concat(topicPrefix, []byte(topic)) }
func tombstoneKey(id ID) []byte    { return concat(tombstonePrefix, id[:]) }
func topicEdgesPrefix(topic string) []byte {
	return concat(edgePrefix, []byte(topic), []byte{0})
}
func edgeKey(topic string, id ID) []byte {
	return concat(topicEdgesPrefix(topic), id[:])
}
func memoryEdgesPrefix(id ID) []byte { return concat(reversePrefix, id[:]) }
func reverseKey(id ID, topic string) []byte {
	return concat(memoryEdgesPrefix(id), []byte(topic))
}

func (s *Store) Remember(text string, topics []string) (ID, error) {
	return s.remember(text, topics, now().Unix(), pebble.Sync)
}

func (s *Store) remember(text string, topics []string, created int64, opts *pebble.WriteOptions) (ID, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return ID{}, errors.New("text is empty")
	}
	topics = normalizeTopics(topics)
	id := newID(created)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.putMemory(batch, memoryRecord{ID: id, Text: text, Created: created}, topics); err != nil {
		return ID{}, err
	}
	return id, batch.Commit(opts)
}

// putMemory adds a memory node with ABOUT edges to normalized topics. Topic
// counts are read from the database, so a batch must not add two memories
// about the same new topic; the migration builds its own counts instead.
func (s *Store) putMemory(batch *pebble.Batch, m memoryRecord, topics []string) error {
	encoded, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = batch.Set(memoryKey(m.ID), encoded, nil); err != nil {
		return err
	}
	for pos, topic := range topics {
		count, err := s.getUint(topicKey(topic))
		if err != nil {
			return err
		}
		if err = batch.Set(topicKey(topic), uint64Bytes(count+1), nil); err != nil {
			return err
		}
		if err = putEdge(batch, m.ID, topic, pos); err != nil {
			return err
		}
	}
	if err = indexMemory(batch, m.ID, m.Text, topics, m.Created); err != nil {
		return err
	}
	return s.addDocCount(batch, 1)
}

func putEdge(batch *pebble.Batch, id ID, topic string, pos int) error {
	if err := batch.Set(edgeKey(topic, id), nil, nil); err != nil {
		return err
	}
	return batch.Set(reverseKey(id, topic), uint64Bytes(uint64(pos)), nil)
}

// getUint reads a uint64 value, returning 0 when the key is absent.
func (s *Store) getUint(key []byte) (uint64, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(value) != 8 {
		return 0, fmt.Errorf("corrupt value for key %q", key)
	}
	return binary.BigEndian.Uint64(value), nil
}

func (s *Store) Get(id ID) (*Memory, error) {
	value, closer, err := s.db.Get(memoryKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r memoryRecord
	err = json.Unmarshal(value, &r)
	closer.Close()
	if err != nil {
		return nil, err
	}
	topics, err := s.memoryTopics(id)
	if err != nil {
		return nil, err
	}
	return &Memory{ID: r.ID, Text: r.Text, Topics: topics, Created: r.Created}, nil
}

// memoryTopics follows a memory's ABOUT edges, in the order they were given.
func (s *Store) memoryTopics(id ID) ([]string, error) {
	prefix := memoryEdgesPrefix(id)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var edges []topicEdge
	for iter.First(); iter.Valid(); iter.Next() {
		edges = append(edges, topicEdge{string(iter.Key()[len(prefix):]), binary.BigEndian.Uint64(iter.Value())})
	}
	return sortedTopics(edges), iter.Error()
}

type topicEdge struct {
	topic string
	pos   uint64
}

func sortedTopics(edges []topicEdge) []string {
	sort.Slice(edges, func(i, j int) bool { return edges[i].pos < edges[j].pos })
	out := make([]string, len(edges))
	for i, e := range edges {
		out[i] = e.topic
	}
	return out
}

// Forget deletes a memory and leaves a tombstone, so the id stays deleted when
// stores are merged.
func (s *Store) Forget(id ID) (bool, error) {
	m, err := s.Get(id)
	if err != nil || m == nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = s.deleteMemory(batch, m, now().Unix()); err != nil {
		return false, err
	}
	return true, batch.Commit(pebble.Sync)
}

// deleteMemory adds to batch the removal of a memory's node, edges, topic
// counts and postings, and writes its tombstone.
func (s *Store) deleteMemory(batch *pebble.Batch, m *Memory, deletedAt int64) error {
	if err := batch.Delete(memoryKey(m.ID), nil); err != nil {
		return err
	}
	for _, topic := range m.Topics {
		if err := batch.Delete(edgeKey(topic, m.ID), nil); err != nil {
			return err
		}
		if err := batch.Delete(reverseKey(m.ID, topic), nil); err != nil {
			return err
		}
		count, err := s.getUint(topicKey(topic))
		if err != nil {
			return err
		}
		if count <= 1 {
			err = batch.Delete(topicKey(topic), nil)
		} else {
			err = batch.Set(topicKey(topic), uint64Bytes(count-1), nil)
		}
		if err != nil {
			return err
		}
	}
	if err := unindexMemory(batch, m); err != nil {
		return err
	}
	if err := s.addDocCount(batch, -1); err != nil {
		return err
	}
	return putTombstone(batch, m.ID, deletedAt)
}

// putTombstone records that the memory with id was deleted at deletedAt.
func putTombstone(batch *pebble.Batch, id ID, deletedAt int64) error {
	tombstone, err := json.Marshal(tombstoneRecord{DeletedAt: deletedAt})
	if err != nil {
		return err
	}
	return batch.Set(tombstoneKey(id), tombstone, nil)
}

// hasTombstone reports whether the memory with id was forgotten.
func (s *Store) hasTombstone(id ID) (bool, error) {
	t, err := s.tombstoneTime(id)
	return t != nil, err
}

// tombstoneTime returns when the memory with id was forgotten, or nil if it
// has no tombstone.
func (s *Store) tombstoneTime(id ID) (*int64, error) {
	value, closer, err := s.db.Get(tombstoneKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r tombstoneRecord
	err = json.Unmarshal(value, &r)
	closer.Close()
	if err != nil {
		return nil, fmt.Errorf("read tombstone %s: %w", id, err)
	}
	return &r.DeletedAt, nil
}

// Tombstones returns every forgotten memory's id with the time it was deleted.
func (s *Store) Tombstones() (map[ID]int64, error) {
	out := map[ID]int64{}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: tombstonePrefix, UpperBound: prefixEnd(tombstonePrefix)})
	if err != nil {
		return nil, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var id ID
		if len(iter.Key()) != len(tombstonePrefix)+len(id) {
			iter.Close()
			return nil, fmt.Errorf("corrupt tombstone key %q", iter.Key())
		}
		copy(id[:], iter.Key()[len(tombstonePrefix):])
		var r tombstoneRecord
		if err = json.Unmarshal(iter.Value(), &r); err != nil {
			iter.Close()
			return nil, fmt.Errorf("read tombstone %s: %w", id, err)
		}
		out[id] = r.DeletedAt
	}
	return out, errors.Join(iter.Error(), iter.Close())
}

// Memories returns every memory, oldest first, reading the memory nodes and
// the memory -> topic edges in two ordered scans.
func (s *Store) Memories() ([]Memory, error) {
	topics := map[ID][]topicEdge{}
	edges, err := s.db.NewIter(&pebble.IterOptions{LowerBound: reversePrefix, UpperBound: prefixEnd(reversePrefix)})
	if err != nil {
		return nil, err
	}
	for edges.First(); edges.Valid(); edges.Next() {
		key := edges.Key()[len(reversePrefix):]
		var id ID
		copy(id[:], key[:len(id)])
		topics[id] = append(topics[id], topicEdge{string(key[len(id):]), binary.BigEndian.Uint64(edges.Value())})
	}
	if err = errors.Join(edges.Error(), edges.Close()); err != nil {
		return nil, err
	}
	var out []Memory
	err = s.scanMemories(false, func(r memoryRecord) bool {
		out = append(out, Memory{ID: r.ID, Text: r.Text, Topics: sortedTopics(topics[r.ID]), Created: r.Created})
		return true
	})
	return out, err
}

// scanMemories visits memory nodes in id order, newest first if reverse,
// until visit returns false.
func (s *Store) scanMemories(reverse bool, visit func(memoryRecord) bool) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: memoryPrefix, UpperBound: prefixEnd(memoryPrefix)})
	if err != nil {
		return err
	}
	defer iter.Close()
	valid, step := iter.First(), iter.Next
	if reverse {
		valid, step = iter.Last(), iter.Prev
	}
	for ; valid; valid = step() {
		var r memoryRecord
		if err = json.Unmarshal(iter.Value(), &r); err != nil {
			return err
		}
		if !visit(r) {
			break
		}
	}
	return iter.Error()
}

// About follows a topic's ABOUT edges, newest memory first. Ids are assigned
// in creation order, so walking the edges backwards needs no sort.
func (s *Store) About(topic string, limit int) ([]Memory, error) {
	prefix := topicEdgesPrefix(normalizeTopic(topic))
	ids, err := s.edgeIDs(prefix, true, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Memory, 0, len(ids))
	for _, id := range ids {
		m, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		if m != nil {
			out = append(out, *m)
		}
	}
	return out, nil
}

// edgeIDs lists the memory ids at the end of a topic's edges; limit 0 means all.
func (s *Store) edgeIDs(prefix []byte, reverse bool, limit int) ([]ID, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	valid, step := iter.First(), iter.Next
	if reverse {
		valid, step = iter.Last(), iter.Prev
	}
	var ids []ID
	for ; valid && (limit == 0 || len(ids) < limit); valid = step() {
		if key := iter.Key(); len(key) == len(prefix)+len(ID{}) {
			var id ID
			copy(id[:], key[len(prefix):])
			ids = append(ids, id)
		}
	}
	return ids, iter.Error()
}

// Recent returns the newest memories, walking memory nodes from the end.
func (s *Store) Recent(limit int) ([]Memory, error) {
	var records []memoryRecord
	err := s.scanMemories(true, func(r memoryRecord) bool {
		records = append(records, r)
		return len(records) < limit
	})
	if err != nil {
		return nil, err
	}
	out := make([]Memory, 0, len(records))
	for _, r := range records {
		topics, err := s.memoryTopics(r.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, Memory{ID: r.ID, Text: r.Text, Topics: topics, Created: r.Created})
	}
	return out, nil
}

// Topics lists topics with how many memories each has, from the counts on
// topic nodes. With relatedTo, it counts the topics of memories about that
// one, which includes relatedTo itself.
func (s *Store) Topics(relatedTo string) ([][2]string, error) {
	counts := make(map[string]int)
	if relatedTo = normalizeTopic(relatedTo); relatedTo != "" {
		ids, err := s.edgeIDs(topicEdgesPrefix(relatedTo), false, 0)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			topics, err := s.memoryTopics(id)
			if err != nil {
				return nil, err
			}
			for _, t := range topics {
				counts[t]++
			}
		}
	} else {
		iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: topicPrefix, UpperBound: prefixEnd(topicPrefix)})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			counts[string(iter.Key()[len(topicPrefix):])] = int(binary.BigEndian.Uint64(iter.Value()))
		}
		if err = errors.Join(iter.Error(), iter.Close()); err != nil {
			return nil, err
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

// normalizeTopic lowercases and trims a topic name and drops control
// characters, which key layouts rely on topics not containing.
func normalizeTopic(topic string) string {
	topic = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, topic)
	return strings.ToLower(strings.TrimSpace(topic))
}

func normalizeTopics(topics []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(topics))
	for _, topic := range topics {
		topic = normalizeTopic(topic)
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
