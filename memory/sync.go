package memory

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/cockroachdb/pebble"
)

// A sync stream is newline-delimited JSON, one item per line: a memory with
// its topics, or a tombstone. Export writes every live memory and every
// tombstone; Import merges such a stream into a store.
//
// Merge rules: a memory is added unless its id has a tombstone or it is
// already present; a tombstone deletes the memory with that id, if any, and
// is kept. Both delivery orders converge, and repeating a line changes
// nothing.
type syncLine struct {
	Memory    *syncMemory `json:"memory,omitempty"`
	Tombstone *syncTomb   `json:"tombstone,omitempty"`
}

type syncMemory struct {
	ID      ID       `json:"id"`
	Text    string   `json:"text"`
	Topics  []string `json:"topics"`
	Created int64    `json:"created"`
}

type syncTomb struct {
	ID        ID    `json:"id"`
	DeletedAt int64 `json:"deleted_at"`
}

// Export writes the store's memories and tombstones to w as a sync stream.
func (s *Store) Export(w io.Writer) error {
	memories, err := s.Memories()
	if err != nil {
		return err
	}
	tombstones, err := s.Tombstones()
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, m := range memories {
		if err := enc.Encode(syncLine{Memory: &syncMemory{ID: m.ID, Text: m.Text, Topics: m.Topics, Created: m.Created}}); err != nil {
			return err
		}
	}
	ids := make([]ID, 0, len(tombstones))
	for id := range tombstones {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b ID) int {
		if a.Less(b) {
			return -1
		}
		if b.Less(a) {
			return 1
		}
		return 0
	})
	for _, id := range ids {
		if err := enc.Encode(syncLine{Tombstone: &syncTomb{ID: id, DeletedAt: tombstones[id]}}); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Import merges a sync stream from r into the store and reports how many
// memories were added and how many deletions applied.
//
// Each line commits on its own. Topic counts and the document count are read
// from the database while a batch is built, so a batch could not hold two
// memories about the same new topic.
func (s *Store) Import(r io.Reader) (added, deleted int, err error) {
	dec := json.NewDecoder(r)
	for {
		var line syncLine
		if err = dec.Decode(&line); errors.Is(err, io.EOF) {
			return added, deleted, nil
		} else if err != nil {
			return added, deleted, fmt.Errorf("read sync stream: %w", err)
		}
		switch {
		case line.Memory != nil && line.Tombstone == nil:
			ok, err := s.importMemory(line.Memory)
			if err != nil {
				return added, deleted, err
			}
			if ok {
				added++
			}
		case line.Tombstone != nil && line.Memory == nil:
			ok, err := s.importTombstone(line.Tombstone)
			if err != nil {
				return added, deleted, err
			}
			if ok {
				deleted++
			}
		default:
			return added, deleted, errors.New("sync line must hold exactly one of memory or tombstone")
		}
	}
}

func (s *Store) importMemory(m *syncMemory) (bool, error) {
	text := m.Text
	if text == "" || m.ID == (ID{}) {
		return false, errors.New("sync memory needs an id and text")
	}
	gone, err := s.hasTombstone(m.ID)
	if err != nil || gone {
		return false, err
	}
	existing, err := s.Get(m.ID)
	if err != nil || existing != nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = s.putMemory(batch, memoryRecord{ID: m.ID, Text: text, Created: m.Created}, normalizeTopics(m.Topics)); err != nil {
		return false, err
	}
	return true, batch.Commit(pebble.Sync)
}

// importTombstone applies a deletion. Two replicas can forget the same memory
// at different times, so when a tombstone already exists the earlier time is
// kept: then every merge order stores the same bytes.
func (s *Store) importTombstone(t *syncTomb) (bool, error) {
	if t.ID == (ID{}) {
		return false, errors.New("sync tombstone needs an id")
	}
	existing, err := s.tombstoneTime(t.ID)
	if err != nil {
		return false, err
	}
	m, err := s.Get(t.ID)
	if err != nil {
		return false, err
	}
	deletedAt := t.DeletedAt
	if existing != nil && *existing < deletedAt {
		deletedAt = *existing
	}
	if existing != nil && *existing == deletedAt && m == nil {
		return false, nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if m != nil {
		if err = s.deleteMemory(batch, m, deletedAt); err != nil {
			return false, err
		}
	} else if err = putTombstone(batch, t.ID, deletedAt); err != nil {
		return false, err
	}
	return true, batch.Commit(pebble.Sync)
}
