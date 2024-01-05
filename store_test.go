package pixidb

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestBasicCreate(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "pixidb_store_basic_create")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testCases := []struct {
		name              string
		rows              int
		columns           []Column
		expectRowSize     int
		expectRowsPerPage int
	}{
		{"simple", 1, []Column{NewColumn("hello", FieldTypeInt32, []byte{1, 2, 3, 4})}, 4, (os.Getpagesize() - ChecksumSize) / 4},
		{"twocolumn", 10, []Column{
			NewColumn("one", FieldTypeInt16, []byte{0, 1}),
			NewColumn("two", FieldTypeInt64, []byte{9, 8, 7, 1, 2, 3, 4, 5}),
		}, 10, (os.Getpagesize() - ChecksumSize) / 10},
		{"fourcolumn", 1000, []Column{
			NewColumn("one", FieldTypeInt32, []byte{0, 1, 2, 3}),
			NewColumn("two", FieldTypeInt32, []byte{5, 6, 7, 8}),
			NewColumn("three", FieldTypeInt32, []byte{4, 9, 2, 9}),
			NewColumn("four", FieldTypeInt32, []byte{6, 6, 6, 6}),
		}, 16, (os.Getpagesize() - ChecksumSize) / 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.columns)
			if err != nil {
				t.Fatal(err)
			}
			if store.Name != tc.name {
				t.Errorf("expected name %s, got %s", tc.name, store.Name)
			}
			if store.Rows != tc.rows {
				t.Errorf("expected rows %d, got %d", tc.rows, store.Rows)
			}
			if store.RowSize() != tc.expectRowSize {
				t.Errorf("expected row size %d, got %d", tc.expectRowSize, store.RowSize())
			}
			if store.RowsPerPage() != tc.expectRowsPerPage {
				t.Errorf("expected rows per page %d, got %d", tc.expectRowsPerPage, store.RowsPerPage())
			}

			defRow := store.DefaultRow()

			compareRow(t, store, 0, defRow)
			compareRow(t, store, store.Rows-1, defRow)
			compareRow(t, store, store.Rows/2, defRow)
		})
	}
}

func TestBasicOpen(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "pixidb_store_basic_open")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testCases := []struct {
		name              string
		rows              int
		columns           []Column
		expectRowSize     int
		expectRowsPerPage int
	}{
		{"simple", 1, []Column{NewColumn("hello", FieldTypeInt32, []byte{1, 2, 3, 4})}, 4, (os.Getpagesize() - ChecksumSize) / 4},
		{"twocolumn", 10, []Column{
			NewColumn("one", FieldTypeInt16, []byte{0, 1}),
			NewColumn("two", FieldTypeInt64, []byte{9, 8, 7, 1, 2, 3, 4, 5}),
		}, 10, (os.Getpagesize() - ChecksumSize) / 10},
		{"fourcolumn", 1000, []Column{
			NewColumn("one", FieldTypeInt32, []byte{0, 1, 2, 3}),
			NewColumn("two", FieldTypeInt32, []byte{5, 6, 7, 8}),
			NewColumn("three", FieldTypeInt32, []byte{4, 9, 2, 9}),
			NewColumn("four", FieldTypeInt32, []byte{6, 6, 6, 6}),
		}, 16, (os.Getpagesize() - ChecksumSize) / 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.columns)
			if err != nil {
				t.Fatal(err)
			}

			store, err := OpenStore(filepath.Join(dir, tc.name))
			if err != nil {
				t.Fatal(err)
			}
			if store.Name != tc.name {
				t.Errorf("expected name %s, got %s", tc.name, store.Name)
			}
			if store.Rows != tc.rows {
				t.Errorf("expected rows %d, got %d", tc.rows, store.Rows)
			}
			if store.RowSize() != tc.expectRowSize {
				t.Errorf("expected row size %d, got %d", tc.expectRowSize, store.RowSize())
			}
			if store.RowsPerPage() != tc.expectRowsPerPage {
				t.Errorf("expected rows per page %d, got %d", tc.expectRowsPerPage, store.RowsPerPage())
			}

			defRow := store.DefaultRow()

			compareRow(t, store, 0, defRow)
			compareRow(t, store, store.Rows-1, defRow)
			compareRow(t, store, store.Rows/2, defRow)
		})
	}
}

func TestBasicSetPersist(t *testing.T) {

}

func compareRow(t *testing.T, store *Store, row int, expect []byte) {
	actual, err := store.GetRowAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Compare(expect, actual) != 0 {
		t.Errorf("expected first row to equal default row %v, got %v", expect, actual)
	}
}