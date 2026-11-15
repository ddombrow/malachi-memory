package main

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/ddombrow/malachi-memory/memory"
)

func TestBackupFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "malachi-memory")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	db, err := memory.NewDb(dir + "/memory.pebble")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Call("remember", map[string]any{"text": "flag backup"}); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "--db", dir+"/memory.pebble", "--backup", dir+"/copy").CombinedOutput(); err != nil {
		t.Fatalf("backup: %v\n%s", err, out)
	}
	copyDb, err := memory.NewDb(dir + "/copy")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := copyDb.Call("recall", nil); err != nil || out == "No memories found." {
		t.Fatalf("copy recall = %q, %v", out, err)
	}
}
