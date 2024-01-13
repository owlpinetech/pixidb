package pixidb

import (
	"os"
	"testing"
)

func TestOpenDatabase(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "pixidb_database_basic_open")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	//orig, err := NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}

	//orig.Create("hello", NewProjectionlessIndexer(10, 10, true))
}
