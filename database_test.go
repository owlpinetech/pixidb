package pixidb

import (
	"os"
	"slices"
	"testing"

	"github.com/owlpinetech/healpix"
)

func TestOpenDatabase(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_database_basic_open")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	orig, err := NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = orig.Create("hello", NewProjectionlessIndexer(10, 10, true), NewColumnInt32("col1", 6))
	if err != nil {
		t.Fatal(err)
	}
	err = orig.Create("goodbye", NewFlatHealpixIndexer(1, healpix.NestScheme), NewColumnUint16("col1", 3))
	if err != nil {
		t.Fatal(err)
	}

	opened, err := OpenDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}

	tables, err := opened.GetTableNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected to have 2 tables, but had %d", len(tables))
	}
	if !slices.Contains(tables, "hello") {
		t.Errorf("expected table hello to be in database, but wasn't")
	}
	if !slices.Contains(tables, "goodbye") {
		t.Errorf("expected table goodbye to be in database, but wasn't")
	}
}
