package memory

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
		if err := Serve(db, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func mustRemember(t *testing.T, s *Store, text string, topics ...string) ID {
	t.Helper()
	id, err := s.Remember(text, topics)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestNewDbEnforcesPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.pebble")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDb(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("database directory mode = %04o, want 0700", got)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := t.TempDir() + "/memory.pebble"
	db, err := NewDb(path)
	if err != nil {
		t.Fatal(err)
	}
	var id ID
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
		// New ids sort after the ones already stored.
		if next := mustRemember(t, s, "second"); !id.Less(next) {
			t.Fatalf("next id %s does not follow %s", next, id)
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
		var ids []ID
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
				t.Fatalf("about[%d] = #%s, want #%s", i, m.ID, ids[4-i])
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
	var id ID
	withStore(t, db, func(s *Store) { id = mustRemember(t, s, "Backups are checkpoints", "ops") })
	if err = db.Backup(dir + "/backup"); err != nil {
		t.Fatal(err)
	}
	backupInfo, err := os.Stat(dir + "/backup")
	if err != nil {
		t.Fatal(err)
	}
	if got := backupInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("backup directory mode = %04o, want 0700", got)
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
		seen := map[ID]bool{}
		for i, m := range all {
			if seen[m.ID] {
				t.Fatalf("memory %d repeats id %s", i, m.ID)
			}
			seen[m.ID] = true
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
	scanErr := scanner.Err()
	if scanErr != nil {
		_ = stdout.Close()
	}
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("process %d: %v: %s", p, err, stderr.String())
	}
	if scanErr != nil {
		return fmt.Errorf("process %d: read replies: %w", p, scanErr)
	}
	if replies != 2*writes {
		return fmt.Errorf("process %d: %d replies, want %d", p, replies, 2*writes)
	}
	return nil
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

func TestToolsAndCall(t *testing.T) {
	tools := Tools()
	if len(tools) != 4 {
		t.Fatalf("%d tools", len(tools))
	}
	readOnly := map[string]bool{}
	for _, tool := range tools {
		readOnly[tool.Name] = tool.ReadOnly
		if tool.Description == "" || tool.InputSchema["type"] != "object" {
			t.Fatalf("tool %s is incomplete: %+v", tool.Name, tool)
		}
	}
	if !readOnly["recall"] || !readOnly["topics"] || readOnly["remember"] || readOnly["forget"] {
		t.Fatalf("read-only flags = %v", readOnly)
	}
	db := testDB(t)
	if out, err := db.Call("remember", map[string]any{"text": "Go callers pass string slices", "topics": []string{"api"}}); err != nil || !strings.HasPrefix(out, "Remembered #") {
		t.Fatalf("remember = %q, %v", out, err)
	}
	if out, err := db.Call("recall", map[string]any{"topic": "api"}); err != nil || !strings.Contains(out, "[api] Go callers") {
		t.Fatalf("recall = %q, %v", out, err)
	}
	if _, err := db.Call("forget", map[string]any{"id": ID{15: 7}.String()}); err == nil {
		t.Fatal("forgetting a missing memory should be a tool error")
	}

	if _, err := db.Call("remember", map[string]any{"text": "second", "topics": []string{"api"}}); err != nil {
		t.Fatal(err)
	}
	if out, err := db.Call("recall", map[string]any{"topic": "api", "limit": 1}); err != nil || strings.Count(out, "[api]") != 1 {
		t.Fatalf("recall with limit = %q, %v", out, err)
	}
	// Ids are strings, so a number is not a valid id.
	if _, err := db.Call("forget", map[string]any{"id": 1}); err == nil {
		t.Fatal("forget with a numeric id should be a tool error")
	}
	var id string
	withStore(t, db, func(s *Store) {
		all, err := s.Memories()
		if err != nil || len(all) == 0 {
			t.Fatal(err)
		}
		id = all[0].ID.String()
	})
	if out, err := db.Call("forget", map[string]any{"id": id}); err != nil || out != "Forgot #"+id+"." {
		t.Fatalf("forget = %q, %v", out, err)
	}
}

func TestPanicReleasesLock(t *testing.T) {
	db := testDB(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected the panic to propagate")
			}
		}()
		db.With(func(*Store) (string, error) { panic("boom") })
	}()
	start := time.Now()
	if _, err := db.Call("remember", map[string]any{"text": "after the panic"}); err != nil {
		t.Fatalf("call after recovered panic: %v", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("waited %s for the lock", waited)
	}
}
