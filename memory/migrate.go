package memory

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/cockroachdb/pebble"
)

var legacyNextIDKey = []byte("meta/next-id")

// legacyMemory is a memory as schemas v1 to v3 numbered it.
type legacyMemory struct {
	ID      int64
	Text    string
	Topics  []string
	Created int64
}

// v1Memory is the v1 memory record, which carried its own topic list. In v1
// those lists were authoritative: t/ keys could be left behind by forget, and
// e/<topic>/<id> keys were ambiguous for topics containing "/".
type v1Memory struct {
	ID      int64    `json:"id"`
	Text    string   `json:"text"`
	Topics  []string `json:"topics"`
	Created int64    `json:"created"`
}

// v3Record is a memory node in schemas v2 and v3. Their topics are edges.
type v3Record struct {
	ID      int64  `json:"id"`
	Text    string `json:"text"`
	Created int64  `json:"created"`
}

// migrateV1 reads v1 memories and migrates them to the current schema.
func (s *Store) migrateV1() error {
	var old []v1Memory
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: memoryPrefix, UpperBound: prefixEnd(memoryPrefix)})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var m v1Memory
		if err = json.Unmarshal(iter.Value(), &m); err != nil {
			iter.Close()
			return fmt.Errorf("read v1 memory %q: %w", iter.Key(), err)
		}
		old = append(old, m)
	}
	if err = errors.Join(iter.Error(), iter.Close()); err != nil {
		return err
	}
	legacy := make([]legacyMemory, len(old))
	for i, m := range old {
		legacy[i] = legacyMemory{ID: m.ID, Text: m.Text, Topics: m.Topics, Created: m.Created}
	}
	return s.migrateLegacy(1, legacy)
}

// migrateV3 reads the graph of schema v2 or v3 and migrates it.
func (s *Store) migrateV3(from uint64) error {
	records := map[int64]v3Record{}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: memoryPrefix, UpperBound: prefixEnd(memoryPrefix)})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var r v3Record
		if err = json.Unmarshal(iter.Value(), &r); err != nil {
			iter.Close()
			return fmt.Errorf("read memory %q: %w", iter.Key(), err)
		}
		records[r.ID] = r
	}
	if err = errors.Join(iter.Error(), iter.Close()); err != nil {
		return err
	}
	edges := map[int64][]topicEdge{}
	reverse, err := s.db.NewIter(&pebble.IterOptions{LowerBound: reversePrefix, UpperBound: prefixEnd(reversePrefix)})
	if err != nil {
		return err
	}
	for reverse.First(); reverse.Valid(); reverse.Next() {
		key := reverse.Key()[len(reversePrefix):]
		id := int64(binary.BigEndian.Uint64(key[:8]))
		edges[id] = append(edges[id], topicEdge{string(key[8:]), binary.BigEndian.Uint64(reverse.Value())})
	}
	if err = errors.Join(reverse.Error(), reverse.Close()); err != nil {
		return err
	}
	legacy := make([]legacyMemory, 0, len(records))
	for id, r := range records {
		legacy = append(legacy, legacyMemory{ID: id, Text: r.Text, Topics: sortedTopics(edges[id]), Created: r.Created})
	}
	slices.SortFunc(legacy, func(a, b legacyMemory) int { return int(a.ID - b.ID) })
	return s.migrateLegacy(from, legacy)
}

// migrateLegacy rewrites a database from schema from as the current schema,
// with every memory given a new id. It runs with the database lock held,
// first saves a checkpoint beside the database, and applies the whole rewrite,
// including the version bump, in one batch: an interrupted migration leaves
// the old database intact and runs again on the next open.
func (s *Store) migrateLegacy(from uint64, old []legacyMemory) error {
	backup := fmt.Sprintf("%s.v%d-backup-%s", s.path, from, now().UTC().Format("20060102T150405Z"))
	if err := s.db.Checkpoint(backup, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("back up before migrating to schema v%d: %w", schemaVersion, err)
	}
	if err := os.Chmod(backup, 0o700); err != nil {
		return fmt.Errorf("secure migration backup: %w", err)
	}

	migrated := make([]Memory, 0, len(old))
	counts := map[string]uint64{}
	for _, m := range old {
		topics := normalizeTopics(m.Topics)
		id, err := legacyID(m.Created, m.ID, m.Text, topics)
		if err != nil {
			return err
		}
		migrated = append(migrated, Memory{ID: id, Text: m.Text, Topics: topics, Created: m.Created})
		for _, topic := range topics {
			counts[topic]++
		}
	}
	slices.SortFunc(migrated, func(a, b Memory) int {
		if a.ID.Less(b.ID) {
			return -1
		}
		if b.ID.Less(a.ID) {
			return 1
		}
		return 0
	})

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, prefix := range [][]byte{memoryPrefix, topicPrefix, edgePrefix, reversePrefix} {
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return err
		}
	}
	if err := batch.Delete(legacyNextIDKey, nil); err != nil {
		return err
	}
	for _, m := range migrated {
		encoded, err := json.Marshal(memoryRecord{ID: m.ID, Text: m.Text, Created: m.Created})
		if err != nil {
			return err
		}
		if err = batch.Set(memoryKey(m.ID), encoded, nil); err != nil {
			return err
		}
		for pos, topic := range m.Topics {
			if err = putEdge(batch, m.ID, topic, pos); err != nil {
				return err
			}
		}
	}
	for topic, count := range counts {
		if err := batch.Set(topicKey(topic), uint64Bytes(count), nil); err != nil {
			return err
		}
	}
	// Builds the search index and stamps the schema version.
	if err := writeIndex(batch, migrated); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	if err := s.verifyMigration(migrated, counts); err != nil {
		return fmt.Errorf("migration to schema v%d did not verify (%w); the v%d database is preserved at %s", schemaVersion, err, from, backup)
	}
	fmt.Fprintf(os.Stderr, "malachi-memory: migrated %d memories from schema v%d to v%d; backup at %s\n", len(old), from, schemaVersion, backup)
	return nil
}

// verifyMigration reads the new graph back and checks it holds exactly the
// expected memories, topics in order, and topic counts.
func (s *Store) verifyMigration(expected []Memory, counts map[string]uint64) error {
	got, err := s.Memories()
	if err != nil {
		return err
	}
	if len(got) != len(expected) {
		return fmt.Errorf("%d memories, want %d", len(got), len(expected))
	}
	for i, m := range expected {
		g := got[i]
		if g.ID != m.ID || g.Text != m.Text || g.Created != m.Created || !slices.Equal(g.Topics, m.Topics) {
			return fmt.Errorf("memory %s differs", m.ID)
		}
	}
	topics, err := s.Topics("")
	if err != nil {
		return err
	}
	if len(topics) != len(counts) {
		return fmt.Errorf("%d topics, want %d", len(topics), len(counts))
	}
	for _, t := range topics {
		if fmt.Sprint(counts[t[0]]) != t[1] {
			return fmt.Errorf("topic %q has count %s, want %d", t[0], t[1], counts[t[0]])
		}
	}
	return nil
}
