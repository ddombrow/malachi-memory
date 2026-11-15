package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/cockroachdb/pebble"
)

// v1Memory is the v1 memory record, which carried its own topic list. In v1
// those lists were authoritative: t/ keys could be left behind by forget, and
// e/<topic>/<id> keys were ambiguous for topics containing "/".
type v1Memory struct {
	ID      int64    `json:"id"`
	Text    string   `json:"text"`
	Topics  []string `json:"topics"`
	Created int64    `json:"created"`
}

// migrateV1 rewrites a v1 database as v2. It runs with the database lock
// held, first saves a checkpoint beside the database, and applies the whole
// rewrite, including the version bump, in one batch: an interrupted migration
// leaves v1 intact and runs again on the next open.
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
		m.Topics = normalizeTopics(m.Topics)
		old = append(old, m)
	}
	if err = errors.Join(iter.Error(), iter.Close()); err != nil {
		return err
	}

	backup := fmt.Sprintf("%s.v1-backup-%s", s.path, now().UTC().Format("20060102T150405Z"))
	if err = s.db.Checkpoint(backup, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("back up before migrating to schema v%d: %w", schemaVersion, err)
	}
	if err = os.Chmod(backup, 0o700); err != nil {
		return fmt.Errorf("secure migration backup: %w", err)
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, prefix := range [][]byte{topicPrefix, edgePrefix, reversePrefix} {
		if err = batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return err
		}
	}
	counts := map[string]uint64{}
	var maxID int64
	migrated := make([]Memory, 0, len(old))
	for _, m := range old {
		migrated = append(migrated, Memory{ID: m.ID, Text: m.Text, Topics: m.Topics, Created: m.Created})
		encoded, err := json.Marshal(memoryRecord{ID: m.ID, Text: m.Text, Created: m.Created})
		if err != nil {
			return err
		}
		if err = batch.Set(memoryKey(m.ID), encoded, nil); err != nil {
			return err
		}
		for pos, topic := range m.Topics {
			counts[topic]++
			if err = putEdge(batch, m.ID, topic, pos); err != nil {
				return err
			}
		}
		maxID = max(maxID, m.ID)
	}
	for topic, count := range counts {
		if err = batch.Set(topicKey(topic), uint64Bytes(count), nil); err != nil {
			return err
		}
	}
	next, err := s.getUint(nextIDKey)
	if err != nil {
		return err
	}
	if err = batch.Set(nextIDKey, uint64Bytes(max(next, uint64(maxID)+1)), nil); err != nil {
		return err
	}
	// Builds the search index and stamps the schema version.
	if err = writeIndex(batch, migrated); err != nil {
		return err
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		return err
	}

	if err = s.verifyMigration(old, counts); err != nil {
		return fmt.Errorf("migration to schema v%d did not verify (%w); the v1 database is preserved at %s", schemaVersion, err, backup)
	}
	fmt.Fprintf(os.Stderr, "malachi-memory: migrated %d memories to schema v%d; v1 backup at %s\n", len(old), schemaVersion, backup)
	return nil
}

// verifyMigration reads the new graph back and checks it holds exactly the
// v1 memories, topics in order, and topic counts.
func (s *Store) verifyMigration(old []v1Memory, counts map[string]uint64) error {
	got, err := s.Memories()
	if err != nil {
		return err
	}
	if len(got) != len(old) {
		return fmt.Errorf("%d memories, want %d", len(got), len(old))
	}
	for i, m := range old {
		g := got[i]
		if g.ID != m.ID || g.Text != m.Text || g.Created != m.Created || !slices.Equal(g.Topics, m.Topics) {
			return fmt.Errorf("memory #%d differs", m.ID)
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
