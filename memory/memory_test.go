package memory

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func testDB(t *testing.T) *Db {
	t.Helper()
	db, err := NewDb(t.TempDir() + "/memory.pebble")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func withStore(t *testing.T, db *Db, fn func(*Store)) {
	t.Helper()
	_, err := db.With(func(s *Store) (string, error) { fn(s); return "", nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestServeReturnsScannerError(t *testing.T) {
	input := strings.NewReader(strings.Repeat("x", 8*1024*1024+1))
	var output bytes.Buffer
	if err := Serve(nil, input, &output); err == nil {
		t.Fatal("serve returned nil for an oversized input line")
	}
}

func TestKnowledgeGraphOperations(t *testing.T) {
	db := testDB(t)
	var first, second int64
	withStore(t, db, func(s *Store) {
		var err error
		first, err = s.Remember("Prefers table-driven tests in Go", []string{"Testing", "Go", "go"})
		if err != nil {
			t.Fatal(err)
		}
		second, err = s.Remember("malachi is a Go coding agent harness", []string{"malachi", "go"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Remember("Uses plain git", []string{"git"}); err != nil {
			t.Fatal(err)
		}
	})
	withStore(t, db, func(s *Store) {
		hits, err := s.Search("table driven tests", 5)
		if err != nil || len(hits) == 0 || hits[0].ID != first {
			t.Fatalf("search hits=%v err=%v", hits, err)
		}
		if strings.Join(hits[0].Topics, ",") != "testing,go" {
			t.Fatalf("topics = %v", hits[0].Topics)
		}
		about, err := s.About("GO", 10)
		if err != nil || len(about) != 2 {
			t.Fatalf("about=%v err=%v", about, err)
		}
		related, err := s.Topics("go")
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, pair := range related {
			got[pair[0]] = pair[1]
		}
		if got["testing"] != "1" || got["malachi"] != "1" {
			t.Fatalf("related topics=%v", related)
		}
		deleted, err := s.Forget(first)
		if err != nil || !deleted {
			t.Fatalf("forget deleted=%v err=%v", deleted, err)
		}
		if m, err := s.Get(first); err != nil || m != nil {
			t.Fatalf("get after forget=%v err=%v", m, err)
		}
		if m, err := s.Get(second); err != nil || m == nil {
			t.Fatalf("get second=%v err=%v", m, err)
		}
	})
}

func TestMCPInterface(t *testing.T) {
	db := testDB(t)
	requests := []map[string]any{
		{"jsonrpc": "2.0", "id": 1, "method": "server/discover"},
		{"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
		{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": map[string]any{"name": "remember", "arguments": map[string]any{"text": "Prefers tabs", "topics": []any{"style"}}}},
		{"jsonrpc": "2.0", "method": "notifications/initialized"},
		{"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": map[string]any{"name": "recall", "arguments": map[string]any{"topic": "style"}}},
		{"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": map[string]any{"name": "forget", "arguments": map[string]any{"id": 999}}},
	}
	var input bytes.Buffer
	for _, request := range requests {
		encoded, _ := json.Marshal(request)
		input.Write(encoded)
		input.WriteByte('\n')
	}
	var output bytes.Buffer
	Serve(db, &input, &output)
	var replies []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var reply map[string]any
		if err := json.Unmarshal([]byte(line), &reply); err != nil {
			t.Fatal(err)
		}
		replies = append(replies, reply)
	}
	if len(replies) != 5 {
		t.Fatalf("got %d replies: %s", len(replies), output.String())
	}
	if replies[0]["result"].(map[string]any)["supportedVersions"].([]any)[0] != modern {
		t.Fatal("discovery did not return modern protocol")
	}
	if len(replies[1]["result"].(map[string]any)["tools"].([]any)) != 4 {
		t.Fatal("wrong tool list")
	}
	if !strings.Contains(replies[3]["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string), "[style] Prefers tabs") {
		t.Fatal("recall response omitted memory")
	}
	if replies[4]["result"].(map[string]any)["isError"] != true {
		t.Fatal("missing tool error")
	}
}

func TestNormalizeTopics(t *testing.T) {
	got := normalizeTopics([]string{" Go ", "go", "", "Testing"})
	if strings.Join(got, ",") != "go,testing" {
		t.Fatalf("got %v", got)
	}
}

func TestTopicControlCharactersDropped(t *testing.T) {
	db := testDB(t)
	withStore(t, db, func(s *Store) {
		id := mustRemember(t, s, "nul in topic", "a\x00b", "ab\n")
		m, err := s.Get(id)
		if err != nil || strings.Join(m.Topics, ",") != "ab" {
			t.Fatalf("topics = %v, %v", m, err)
		}
		if about, _ := s.About("a\x00b", 10); len(about) != 1 {
			t.Fatalf("about = %v", about)
		}
	})
}
