package pixidb

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestBasicCreate(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(wd, "pixidb_store_basic_create")
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
		{"simple", 1, []Column{NewColumnEncoded("hello", ColumnTypeInt32, []byte{1, 2, 3, 4})}, 4, (os.Getpagesize() - ChecksumSize) / 4},
		{"twocolumn", 10, []Column{
			NewColumnEncoded("one", ColumnTypeInt16, []byte{0, 1}),
			NewColumnEncoded("two", ColumnTypeInt64, []byte{9, 8, 7, 1, 2, 3, 4, 5}),
		}, 10, (os.Getpagesize() - ChecksumSize) / 10},
		{"fourcolumn", 1000, []Column{
			NewColumnEncoded("one", ColumnTypeInt32, []byte{0, 1, 2, 3}),
			NewColumnEncoded("two", ColumnTypeInt32, []byte{5, 6, 7, 8}),
			NewColumnEncoded("three", ColumnTypeInt32, []byte{4, 9, 2, 9}),
			NewColumnEncoded("four", ColumnTypeInt32, []byte{6, 6, 6, 6}),
		}, 16, (os.Getpagesize() - ChecksumSize) / 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.columns...)
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
	dir, err := os.MkdirTemp(".", "pixidb_store_basic_open")
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
		{"simple", 1, []Column{NewColumnEncoded("hello", ColumnTypeInt32, []byte{1, 2, 3, 4})}, 4, (os.Getpagesize() - ChecksumSize) / 4},
		{"twocolumn", 10, []Column{
			NewColumnEncoded("one", ColumnTypeInt16, []byte{0, 1}),
			NewColumnEncoded("two", ColumnTypeInt64, []byte{9, 8, 7, 1, 2, 3, 4, 5}),
		}, 10, (os.Getpagesize() - ChecksumSize) / 10},
		{"fourcolumn", 1000, []Column{
			NewColumnEncoded("one", ColumnTypeInt32, []byte{0, 1, 2, 3}),
			NewColumnEncoded("two", ColumnTypeInt32, []byte{5, 6, 7, 8}),
			NewColumnEncoded("three", ColumnTypeInt32, []byte{4, 9, 2, 9}),
			NewColumnEncoded("four", ColumnTypeInt32, []byte{6, 6, 6, 6}),
		}, 16, (os.Getpagesize() - ChecksumSize) / 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.columns...)
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
	dir, err := os.MkdirTemp(".", "pixidb_store_basic_set_persist")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testCases := []struct {
		name    string
		rows    int
		columns []Column
		setRow  []byte
	}{
		{"simple", 1, []Column{NewColumnEncoded("hello", ColumnTypeInt32, []byte{1, 2, 3, 4})}, []byte{9, 9, 9, 9}},
		{"twocolumn", 10, []Column{
			NewColumnEncoded("one", ColumnTypeInt16, []byte{0, 1}),
			NewColumnEncoded("two", ColumnTypeInt64, []byte{9, 8, 7, 1, 2, 3, 4, 5}),
		}, []byte{7, 7, 4, 4, 5, 5, 6, 6, 7, 7}},
		{"fourcolumn", 1000, []Column{
			NewColumnEncoded("one", ColumnTypeInt32, []byte{0, 1, 2, 3}),
			NewColumnEncoded("two", ColumnTypeInt32, []byte{5, 6, 7, 8}),
			NewColumnEncoded("three", ColumnTypeInt32, []byte{4, 9, 2, 9}),
			NewColumnEncoded("four", ColumnTypeInt32, []byte{6, 6, 6, 6}),
		}, []byte{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.columns...)
			if err != nil {
				t.Fatal(err)
			}

			store.SetRowAt(0, tc.setRow)
			store.SetRowAt(store.Rows-1, tc.setRow)
			store.Checkpoint()

			saved, err := OpenStore(filepath.Join(dir, tc.name))
			if err != nil {
				t.Fatal(err)
			}

			defRow := store.DefaultRow()

			compareRow(t, saved, 0, tc.setRow)
			compareRow(t, saved, saved.Rows-1, tc.setRow)
			if saved.Rows > 2 {
				compareRow(t, saved, saved.Rows/2, defRow)
			}
		})
	}
}

func TestSetValuePersist(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_store_set_value_persist")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testCases := []struct {
		name   string
		rows   int
		column Column
		setRow []byte
	}{
		{"simple", 1, NewColumnInt32("one", 3), []byte{0, 0, 0, 9}},
		{"twocolumn", 10, NewColumnInt16("one", 2), []byte{0, 7}},
		{"fourcolumn", 1000, NewColumnInt8("one", 4), []byte{90}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(filepath.Join(dir, tc.name), tc.rows, tc.column)
			if err != nil {
				t.Fatal(err)
			}

			store.SetValueAt("one", 0, tc.setRow)
			store.SetValueAt("one", store.Rows-1, tc.setRow)
			store.Checkpoint()

			saved, err := OpenStore(filepath.Join(dir, tc.name))
			if err != nil {
				t.Fatal(err)
			}

			defRow := store.DefaultRow()

			compareRow(t, saved, 0, tc.setRow)
			compareRow(t, saved, saved.Rows-1, tc.setRow)
			if saved.Rows > 2 {
				compareRow(t, saved, saved.Rows/2, defRow)
			}
		})
	}
}

func TestStoreColumnProjection(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_store_projection")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewStore(filepath.Join(dir, "projections"), 1,
		NewColumnInt16("col1", int16(3)),
		NewColumnInt32("col2", int32(8)),
		NewColumnInt8("col3", 7),
		NewColumnFloat64("col4", 1.4),
		NewColumnUint16("col5", 1))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name    string
		columns []string
		expect  Projection
	}{
		{"firstcol", []string{"col1"}, []ColumnProjection{{0, 0, 2}}},
		{"lastcol", []string{"col5"}, []ColumnProjection{{4, 15, 2}}},
		{"middle", []string{"col2", "col3", "col4"}, []ColumnProjection{{1, 2, 4}, {2, 6, 1}, {3, 7, 8}}},
		{"firstlast", []string{"col5", "col1"}, []ColumnProjection{{4, 15, 2}, {0, 0, 2}}},
		{"doubled", []string{"col2", "col2"}, []ColumnProjection{{1, 2, 4}, {1, 2, 4}}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proj, err := store.Projection(tc.columns...)
			if err != nil {
				t.Fatal(err)
			}

			for i, c := range proj {
				expected := tc.expect[i]
				if c.index != expected.index {
					t.Errorf("expected column projection index %d, but got %d", expected.index, c.index)
				}
				if c.start != expected.start {
					t.Errorf("expected column projection start %d, but got %d", expected.start, c.start)
				}
				if c.size != expected.size {
					t.Errorf("expected column projection start %d, but got %d", expected.size, c.size)
				}
			}
		})
	}
}

func compareRow(t *testing.T, store *Store, row int, expect []byte) {
	actual, err := store.GetRowAt(row)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Compare(expect, actual) != 0 {
		t.Errorf("expected row %d to equal row %v, got %v", row, expect, actual)
	}
}
