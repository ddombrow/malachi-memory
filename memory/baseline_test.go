package memory

import (
	"bytes"
	"flag"
	"os"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite testdata golden files")

// TestMCPBaseline replays a fixed session and compares every reply byte for
// byte with testdata/baseline.golden, so any change to tool names, schemas or
// response text is deliberate. Regenerate with: go test -run MCPBaseline -update
func TestMCPBaseline(t *testing.T) {
	defer func(old func() time.Time) { now = old }(now)
	clock := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).Unix()
	now = func() time.Time { clock += 86400; return time.Unix(clock, 0) }

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
