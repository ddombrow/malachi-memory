package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// When this variable is set, the test binary acts as a server process so tests
// can run several real processes against one database.
const serveEnv = "MALACHI_MEMORY_TEST_SERVE"

func TestMain(m *testing.M) {
	if path := os.Getenv(serveEnv); path != "" {
		db, err := NewDb(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		serve(db, os.Stdin, os.Stdout)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func mustRemember(t *testing.T, s *Store, text string, topics ...string) int64 {
	t.Helper()
	id, err := s.Remember(text, topics)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := t.TempDir() + "/memory.pebble"
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	withStore(t, db, func(s *Store) { id = mustRemember(t, s, "Deploys on Fridays are banned", "deploy") })

	reopened, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, reopened, func(s *Store) {
		m, err := s.Get(id)
		if err != nil || m == nil || m.Text != "Deploys on Fridays are banned" {
			t.Fatalf("get after reopen = %v, %v", m, err)
		}
		// IDs keep increasing after reopen rather than restarting.
		if next := mustRemember(t, s, "second"); next != id+1 {
			t.Fatalf("next id = %d, want %d", next, id+1)
		}
	})
}

func TestForgetRemovesFromEveryView(t *testing.T) {
	db := testDB(t)
	withStore(t, db, func(s *Store) {
		gone := mustRemember(t, s, "Uses zsh with starship", "shell", "prompt")
		kept := mustRemember(t, s, "Uses fish at work", "shell")
		if ok, err := s.Forget(gone); err != nil || !ok {
			t.Fatalf("forget = %v, %v", ok, err)
		}
		if ok, err := s.Forget(gone); err != nil || ok {
			t.Fatalf("second forget = %v, %v", ok, err)
		}
		if hits, _ := s.Search("starship", 10); len(hits) != 0 {
			t.Fatalf("search still finds forgotten memory: %v", hits)
		}
		if about, _ := s.About("shell", 10); len(about) != 1 || about[0].ID != kept {
			t.Fatalf("about = %v", about)
		}
		if about, _ := s.About("prompt", 10); len(about) != 0 {
			t.Fatalf("about prompt = %v", about)
		}
		if recent, _ := s.Recent(10); len(recent) != 1 || recent[0].ID != kept {
			t.Fatalf("recent = %v", recent)
		}
		topics, _ := s.Topics("")
		if len(topics) != 1 || topics[0] != [2]string{"shell", "1"} {
			t.Fatalf("topics = %v", topics)
		}
	})
}

func TestTopicRecallOrderAndLimit(t *testing.T) {
	db := testDB(t)
	defer func(old func() time.Time) { now = old }(now)
	clock := int64(1_700_000_000)
	now = func() time.Time { clock += 86400; return time.Unix(clock, 0) }
	withStore(t, db, func(s *Store) {
		var ids []int64
		for i := 0; i < 5; i++ {
			ids = append(ids, mustRemember(t, s, fmt.Sprintf("go fact %d", i), "Go"))
		}
		mustRemember(t, s, "unrelated", "rust")
		about, err := s.About(" go ", 3)
		if err != nil || len(about) != 3 {
			t.Fatalf("about = %v, %v", about, err)
		}
		for i, m := range about { // newest first
			if m.ID != ids[4-i] {
				t.Fatalf("about[%d] = #%d, want #%d", i, m.ID, ids[4-i])
			}
		}
	})
}

func TestSchemaVersion(t *testing.T) {
	path := t.TempDir() + "/memory.pebble"
	if _, err := NewDb(path); err != nil {
		t.Fatal(err)
	}
	raw, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value, closer, err := raw.Get(schemaVersionKey)
	if err != nil {
		t.Fatalf("new database has no schema version: %v", err)
	}
	if string(value) != string(uint64Bytes(schemaVersion)) {
		t.Fatalf("schema version = %x", value)
	}
	closer.Close()
	if err = raw.Set(schemaVersionKey, uint64Bytes(schemaVersion+1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if _, err = NewDb(path); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("opening a newer schema: err = %v", err)
	}
}

func TestBackupRestores(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDb(dir + "/memory.pebble")
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	withStore(t, db, func(s *Store) { id = mustRemember(t, s, "Backups are checkpoints", "ops") })
	if err = db.Backup(dir + "/backup"); err != nil {
		t.Fatal(err)
	}
	if err = db.Backup(dir + "/backup"); err == nil {
		t.Fatal("backup over an existing directory succeeded")
	}
	// Changes after the backup are not in it.
	withStore(t, db, func(s *Store) { mustRemember(t, s, "after backup") })

	restored, err := NewDb(dir + "/backup")
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, restored, func(s *Store) {
		all, err := s.Memories()
		if err != nil || len(all) != 1 || all[0].ID != id {
			t.Fatalf("restored memories = %v, %v", all, err)
		}
		if about, _ := s.About("ops", 10); len(about) != 1 {
			t.Fatalf("restored edges = %v", about)
		}
	})
}

// Several server processes share one database, taking turns on its lock.
// Every write must land exactly once with its edges.
func TestConcurrentServerProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	path := t.TempDir() + "/memory.pebble"
	const procs, writes = 4, 15
	var wg sync.WaitGroup
	errs := make(chan error, procs)
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			errs <- runServerProcess(path, p, writes)
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, db, func(s *Store) {
		all, err := s.Memories()
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != procs*writes {
			t.Fatalf("%d memories, want %d", len(all), procs*writes)
		}
		for i, m := range all {
			if m.ID != int64(i+1) {
				t.Fatalf("memory %d has id %d: ids must be unique and dense", i, m.ID)
			}
		}
		for p := 0; p < procs; p++ {
			about, err := s.About(fmt.Sprintf("proc%d", p), 100)
			if err != nil || len(about) != writes {
				t.Fatalf("proc%d has %d memories, want %d (%v)", p, len(about), writes, err)
			}
		}
	})
}

func runServerProcess(path string, p, writes int) error {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), serveEnv+"="+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return err
	}
	// Interleave writes with reads so readers and writers overlap.
	go func() {
		enc := json.NewEncoder(stdin)
		for i := 0; i < writes; i++ {
			_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": 2 * i, "method": "tools/call", "params": map[string]any{
				"name": "remember", "arguments": map[string]any{"text": fmt.Sprintf("process %d write %d", p, i), "topics": []string{fmt.Sprintf("proc%d", p)}}}})
			_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": 2*i + 1, "method": "tools/call", "params": map[string]any{
				"name": "recall", "arguments": map[string]any{"query": "process write"}}})
		}
		stdin.Close()
	}()
	replies := 0
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		var reply struct {
			Result struct {
				IsError bool `json:"isError"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"result"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &reply); err != nil {
			return err
		}
		if reply.Result.IsError {
			return fmt.Errorf("process %d: tool error: %s", p, reply.Result.Content[0].Text)
		}
		replies++
	}
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("process %d: %v: %s", p, err, stderr.String())
	}
	if replies != 2*writes {
		return fmt.Errorf("process %d: %d replies, want %d", p, replies, 2*writes)
	}
	return nil
}

func TestBackupFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "malachi-memory")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	db, err := NewDb(dir + "/memory.pebble")
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, db, func(s *Store) { mustRemember(t, s, "flag backup") })
	if out, err := exec.Command(bin, "--db", dir+"/memory.pebble", "--backup", dir+"/copy").CombinedOutput(); err != nil {
		t.Fatalf("backup: %v\n%s", err, out)
	}
	copyDb, err := NewDb(dir + "/copy")
	if err != nil {
		t.Fatal(err)
	}
	withStore(t, copyDb, func(s *Store) {
		if all, _ := s.Memories(); len(all) != 1 {
			t.Fatalf("copy has %d memories", len(all))
		}
	})
}

func TestViewIsReadOnly(t *testing.T) {
	db := testDB(t)
	withStore(t, db, func(s *Store) { mustRemember(t, s, "written before the view", "x") })
	out, err := db.View(func(s *Store) (string, error) { return callTool(s, "recall", map[string]any{"topic": "x"}) })
	if err != nil || !strings.Contains(out, "written before the view") {
		t.Fatalf("view recall = %q, %v", out, err)
	}
	_, err = db.View(func(s *Store) (string, error) { return callTool(s, "remember", map[string]any{"text": "nope"}) })
	if !errors.Is(err, pebble.ErrReadOnly) {
		t.Fatalf("write through View: err = %v, want ErrReadOnly", err)
	}
	for _, tool := range []string{"recall", "topics"} {
		if !readOnlyTool(tool) {
			t.Fatalf("%s should open read-only", tool)
		}
	}
	for _, tool := range []string{"remember", "forget", "nope"} {
		if readOnlyTool(tool) {
			t.Fatalf("%s must not open read-only", tool)
		}
	}
}
