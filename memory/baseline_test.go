package memory

import (
	"bytes"
	"flag"
	"io"
	"os"
	"testing"
	"time"
)

// repeatingReader yields 1, 2, 3, ... so every test id is the same run to run.
type repeatingReader struct{ n byte }

func (r *repeatingReader) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

var update = flag.Bool("update", false, "rewrite testdata golden files")

// TestMCPBaseline replays a fixed session and compares every reply byte for
// byte with testdata/baseline.golden, so any change to tool names, schemas or
// response text is deliberate. Regenerate with: go test -run MCPBaseline -update
func TestMCPBaseline(t *testing.T) {
	defer func(old func() time.Time) { now = old }(now)
	clock := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).Unix()
	now = func() time.Time { clock += 86400; return time.Unix(clock, 0) }
	// Ids are random, so the transcript uses a fixed byte stream and starts the
	// same-millisecond counter from nothing.
	defer func(old io.Reader, ms uint64, last ID) { randomSource, lastMS, lastID = old, ms, last }(randomSource, lastMS, lastID)
	randomSource = &repeatingReader{}
	lastMS, lastID = 0, ID{}

	input, err := os.ReadFile("testdata/baseline.requests.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	Serve(testDB(t), bytes.NewReader(input), &output)
	if *update {
		if err = os.WriteFile("testdata/baseline.golden", output.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile("testdata/baseline.golden")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), want) {
		gotLines, wantLines := bytes.Split(output.Bytes(), []byte("\n")), bytes.Split(want, []byte("\n"))
		for i := 0; i < len(gotLines) || i < len(wantLines); i++ {
			var g, w []byte
			if i < len(gotLines) {
				g = gotLines[i]
			}
			if i < len(wantLines) {
				w = wantLines[i]
			}
			if !bytes.Equal(g, w) {
				t.Fatalf("reply %d differs\n got: %s\nwant: %s", i+1, g, w)
			}
		}
	}
}
