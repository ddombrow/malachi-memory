package memory

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/pebble"
)

// The search index is an inverted index derived from memory nodes and their
// topics. It can be deleted and rebuilt at any time; see rebuildIndex.
//
//	p/<term>\x00<id>    posting: uvarint tf, uvarint doc length, varint created
//	meta/doc-count      number of memories (uint64)
//	meta/index-version  version of the tokenizer and posting format (uint64)
//
// A memory's terms are tokenize(text + " " + topics), as BM25 scores them.
// Terms are letters and digits only, so \x00 ends a term unambiguously.
// Memories are never edited, so forget re-tokenizes a memory to find its
// postings instead of storing its term list.

// indexVersion changes whenever tokenize or the posting format changes; a
// read-write open that finds another version rebuilds the index.
const indexVersion = 1

var (
	postingPrefix   = []byte("p/")
	docCountKey     = []byte("meta/doc-count")
	indexVersionKey = []byte("meta/index-version")
)

func termPrefix(term string) []byte { return concat(postingPrefix, []byte(term), []byte{0}) }
func postingKey(term string, id int64) []byte {
	return concat(termPrefix(term), uint64Bytes(uint64(id)))
}

// memoryTerms returns the term frequencies and document length (in terms)
// BM25 uses for a memory.
func memoryTerms(text string, topics []string) (map[string]int, int) {
	words := tokenize(text + " " + strings.Join(topics, " "))
	tf := make(map[string]int, len(words))
	for _, w := range words {
		tf[w]++
	}
	return tf, len(words)
}

// indexMemory adds a memory's postings to batch. It does not change the
// document count.
func indexMemory(batch *pebble.Batch, id int64, text string, topics []string, created int64) error {
	tf, length := memoryTerms(text, topics)
	for term, count := range tf {
		value := binary.AppendUvarint(nil, uint64(count))
		value = binary.AppendUvarint(value, uint64(length))
		value = binary.AppendVarint(value, created)
		if err := batch.Set(postingKey(term, id), value, nil); err != nil {
			return err
		}
	}
	return nil
}

func unindexMemory(batch *pebble.Batch, m *Memory) error {
	tf, _ := memoryTerms(m.Text, m.Topics)
	for term := range tf {
		if err := batch.Delete(postingKey(term, m.ID), nil); err != nil {
			return err
		}
	}
	return nil
}

// addDocCount adjusts the document count in batch by delta.
func (s *Store) addDocCount(batch *pebble.Batch, delta int) error {
	n, err := s.getUint(docCountKey)
	if err != nil {
		return err
	}
	if delta < 0 && n < uint64(-delta) {
		n = 0
	} else {
		n = uint64(int64(n) + int64(delta))
	}
	return batch.Set(docCountKey, uint64Bytes(n), nil)
}

// writeIndex replaces the whole index in batch with postings for memories,
// and stamps the index and schema versions.
func writeIndex(batch *pebble.Batch, memories []Memory) error {
	if err := batch.DeleteRange(postingPrefix, prefixEnd(postingPrefix), nil); err != nil {
		return err
	}
	for _, m := range memories {
		if err := indexMemory(batch, m.ID, m.Text, m.Topics, m.Created); err != nil {
			return err
		}
	}
	for key, value := range map[string]uint64{
		string(docCountKey):      uint64(len(memories)),
		string(indexVersionKey):  indexVersion,
		string(schemaVersionKey): schemaVersion,
	} {
		if err := batch.Set([]byte(key), uint64Bytes(value), nil); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndex rebuilds the index from the graph in one batch. It changes
// nothing else, so it needs no backup.
func (s *Store) rebuildIndex() error {
	memories, err := s.Memories()
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = writeIndex(batch, memories); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

// checkIndex rebuilds an index of another version, or reports that a
// read-write open is needed to.
func (s *Store) checkIndex(readOnly bool) error {
	v, err := s.getUint(indexVersionKey)
	if err != nil || v == indexVersion {
		return err
	}
	if readOnly {
		return errNeedsMigration
	}
	return s.rebuildIndex()
}

// posting is one memory in a term's posting list.
type posting struct {
	id      int64
	tf      int
	length  int
	created int64
}

func (s *Store) postings(term string) ([]posting, error) {
	prefix := termPrefix(term)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []posting
	for iter.First(); iter.Valid(); iter.Next() {
		key, value := iter.Key(), iter.Value()
		if len(key) != len(prefix)+8 {
			continue
		}
		tf, n1 := binary.Uvarint(value)
		length, n2 := binary.Uvarint(value[max(n1, 0):])
		created, n3 := binary.Varint(value[max(n1+n2, 0):])
		if n1 <= 0 || n2 <= 0 || n3 <= 0 {
			return nil, fmt.Errorf("corrupt posting %q", key)
		}
		out = append(out, posting{int64(binary.BigEndian.Uint64(key[len(prefix):])), int(tf), int(length), created})
	}
	return out, iter.Error()
}

// Search ranks memories by BM25 over their text and topics, reading only the
// posting lists of the query's terms. Scores and ordering match a scan of
// every memory exactly (see scanSearch in the tests): each memory's score
// sums the same terms in the same order, so even float rounding agrees.
func (s *Store) Search(query string, limit int) ([]Memory, error) {
	terms := tokenize(query)
	if len(terms) == 0 || limit <= 0 {
		return nil, nil
	}
	n, err := s.getUint(docCountKey)
	if err != nil {
		return nil, err
	}
	lists := map[string][]posting{}
	scores := map[int64]*searchHit{}
	for _, term := range terms {
		list, ok := lists[term]
		if !ok {
			if list, err = s.postings(term); err != nil {
				return nil, err
			}
			lists[term] = list
		}
		df := float64(len(list))
		idf := math.Log(1 + (float64(n)-df+0.5)/(df+0.5))
		for _, p := range list {
			h := scores[p.id]
			if h == nil {
				h = &searchHit{id: p.id, created: p.created}
				scores[p.id] = h
			}
			tf := float64(p.tf)
			h.score += idf * tf * 2.2 / (tf + 1.2*(0.25+0.75*float64(p.length)/80))
		}
	}

	// Keep the best limit hits in a min-heap whose root is the worst kept.
	top := &hitHeap{}
	for _, h := range scores {
		if h.score <= 0 {
			continue
		}
		if top.Len() < limit {
			heap.Push(top, *h)
		} else if better(*h, (*top)[0]) {
			(*top)[0] = *h
			heap.Fix(top, 0)
		}
	}
	out := make([]Memory, top.Len())
	for i := len(out) - 1; i >= 0; i-- {
		h := heap.Pop(top).(searchHit)
		m, err := s.Get(h.id)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, fmt.Errorf("search index is out of date: memory #%d is missing", h.id)
		}
		out[i] = *m
	}
	return out, nil
}

type searchHit struct {
	id      int64
	created int64
	score   float64
}

// better orders hits by score, then newest, then highest id.
func better(a, b searchHit) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	if a.created != b.created {
		return a.created > b.created
	}
	return a.id > b.id
}

type hitHeap []searchHit

func (h hitHeap) Len() int           { return len(h) }
func (h hitHeap) Less(i, j int) bool { return better(h[j], h[i]) }
func (h hitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hitHeap) Push(x any)        { *h = append(*h, x.(searchHit)) }
func (h *hitHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
