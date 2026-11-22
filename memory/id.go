package memory

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// ID identifies a memory. It is a ULID: 48 bits of millisecond time followed
// by 80 random bits, written as 26 Crockford base32 characters. Ids are made
// locally, so stores can be merged without a central counter, and their byte
// order is creation order (see newID).
type ID [16]byte

const idAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// String returns the 26-character form that the tools show and accept.
func (id ID) String() string {
	var out [26]byte
	var acc uint32
	var bits uint
	idx := 25
	for i := len(id) - 1; i >= 0; i-- {
		acc |= uint32(id[i]) << bits
		bits += 8
		for bits >= 5 {
			out[idx] = idAlphabet[acc&31]
			idx--
			acc >>= 5
			bits -= 5
		}
	}
	out[0] = idAlphabet[acc&31]
	return string(out[:])
}

// ParseID reads an id written by String. Case is ignored, and the Crockford
// look-alikes I, L and O read as 1, 1 and 0.
func ParseID(s string) (ID, error) {
	var id ID
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) != 26 {
		return id, errors.New("memory ids are 26 characters")
	}
	var acc uint32
	var bits uint
	pos := len(id) - 1
	for i := 25; i >= 0; i-- {
		v := strings.IndexByte(idAlphabet, s[i])
		switch s[i] {
		case 'I', 'L':
			v = 1
		case 'O':
			v = 0
		}
		if v < 0 {
			return ID{}, errors.New("memory ids use only Crockford base32 characters")
		}
		acc |= uint32(v) << bits
		bits += 5
		for bits >= 8 {
			if pos < 0 {
				return ID{}, errors.New("memory id is out of range")
			}
			id[pos] = byte(acc)
			pos--
			acc >>= 8
			bits -= 8
		}
	}
	if acc != 0 {
		return ID{}, errors.New("memory id is out of range")
	}
	return id, nil
}

// Less orders ids by creation time, then by the random part.
func (id ID) Less(other ID) bool { return bytes.Compare(id[:], other[:]) < 0 }

// MarshalText and UnmarshalText make ids JSON strings.
func (id ID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

func (id *ID) UnmarshalText(text []byte) error {
	parsed, err := ParseID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// randomSource supplies the random part of ids. Tests replace it so that
// transcripts are reproducible.
var randomSource io.Reader = rand.Reader

var (
	idMu   sync.Mutex
	lastMS uint64
	lastID ID
)

// newID makes an id for a memory created at created (unix seconds). Ids made
// in the same millisecond by one process count upward, so the order of
// memories remembered together is kept.
func newID(created int64) ID {
	idMu.Lock()
	defer idMu.Unlock()
	ms := uint64(created) * 1000
	var id ID
	putTimestamp(&id, ms)
	if ms == lastMS && incrementRandom(&lastID) {
		copy(id[6:], lastID[6:])
	} else if _, err := io.ReadFull(randomSource, id[6:]); err != nil {
		panic("malachi-memory: no randomness for memory ids: " + err.Error())
	}
	lastMS = ms
	lastID = id
	return id
}

// legacyID maps a memory from schemas v1 to v3 to an id. Those schemas
// numbered memories per database, so two stores can both hold a #1 made in
// the same second. The id therefore carries a hash of the memory's content:
//
//	bytes 0-5    creation time, milliseconds
//	bytes 6-11   the old number: same-second memories of one store keep their order
//	bytes 12-15  first four bytes of sha256(created, old number, text, topics)
//
// Equal memories get equal ids, so merging the same memory twice is harmless,
// and different memories that share a number and second get different ids.
func legacyID(created, oldID int64, text string, topics []string) (ID, error) {
	var id ID
	if oldID < 0 || oldID >= 1<<48 {
		return id, fmt.Errorf("memory number %d is too large to migrate", oldID)
	}
	putTimestamp(&id, uint64(created)*1000)
	for i := 0; i < 6; i++ {
		id[6+i] = byte(uint64(oldID) >> (40 - 8*i))
	}
	h := sha256.New()
	fmt.Fprintf(h, "%d\x00%d\x00%s", created, oldID, text)
	for _, topic := range topics {
		fmt.Fprintf(h, "\x00%s", topic)
	}
	copy(id[12:], h.Sum(nil)[:4])
	return id, nil
}

func putTimestamp(id *ID, ms uint64) {
	for i := 0; i < 6; i++ {
		id[i] = byte(ms >> (40 - 8*i))
	}
}

// incrementRandom adds one to the random part of id. It reports false when
// that part overflows, and the caller then draws fresh randomness.
func incrementRandom(id *ID) bool {
	for i := len(id) - 1; i >= 6; i-- {
		id[i]++
		if id[i] != 0 {
			return true
		}
	}
	return false
}
