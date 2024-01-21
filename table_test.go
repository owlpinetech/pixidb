package pixidb

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/owlpinetech/healpix"
	"golang.org/x/exp/maps"
)

func TestTableOpen(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_table_basic_open")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testCases := []struct {
		name     string
		indexer  LocationIndexer
		metadata map[string]string
	}{
		{"mercatortagless", NewMercatorCutoffIndexer(math.Pi/4, -math.Pi/4, 10, 10, true), map[string]string{}},
		{"cyleqtags", NewCylindricalEquirectangularIndexer(0, 10, 10, true), map[string]string{"one": "fish", "two": "fish"}},
		{"healpixtagged", NewFlatHealpixIndexer(2, healpix.NestScheme), map[string]string{"hello": "there"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			orig, err := NewTable(filepath.Join(dir, tc.name), tc.indexer, []Column{{Name: "dummy", Type: ColumnTypeFloat32, Default: []byte{1, 2, 3, 4}}})
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tc.metadata {
				orig.SetMetadata(k, v)
			}

			tbl, err := OpenTable(filepath.Join(dir, tc.name))
			if err != nil {
				t.Fatal(err)
			}
			if !maps.Equal(tbl.Metadata, orig.Metadata) {
				t.Errorf("expected table metadata %v, got %v", orig.Metadata, tbl.Metadata)
			}
			if tbl.IndexerName != orig.IndexerName {
				t.Errorf("expected table indexer name %s, got %s", orig.IndexerName, tbl.IndexerName)
			}
			if reflect.TypeOf(orig.Indexer) != reflect.TypeOf(tbl.Indexer) {
				t.Errorf("expected indexer type %T, got %T", orig.Indexer, tbl.Indexer)
			}
		})
	}
}

func TestTableQuery(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_table_basic_query")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tbl, err := NewTable(filepath.Join(dir, "querytbl"), NewFlatHealpixIndexer(2, healpix.NestScheme),
		[]Column{
			{Name: "col1", Type: ColumnTypeInt32, Default: []byte{0, 0, 0, 3}},
			{Name: "col2", Type: ColumnTypeInt16, Default: []byte{0, 6}}})
	if err != nil {
		t.Fatal(err)
	}

	res, err := tbl.GetRows([]string{"col1"}, IndexLocation(0), IndexLocation(1), IndexLocation(2))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Columns {
		if c.Name != "col1" {
			t.Errorf("expected column name to be col1, got %s", c.Name)
		}
	}
	if len(res.Rows) != 3 {
		t.Errorf("expected to get 3 result rows, got %d", len(res.Rows))
	}
	for _, r := range res.Rows {
		if r[0].AsInt32() != 3 {
			t.Errorf("expected row to equal 3, got %d", r[0].AsInt32())
		}
	}

	res, err = tbl.GetRows([]string{"col2"}, IndexLocation(3), IndexLocation(4), IndexLocation(5))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Columns {
		if c.Name != "col2" {
			t.Errorf("expected column name to be col2, got %s", c.Name)
		}
	}
	if len(res.Rows) != 3 {
		t.Errorf("expected to get 3 result rows, got %d", len(res.Rows))
	}
	for _, r := range res.Rows {
		if r[0].AsInt16() != 6 {
			t.Errorf("expected row to equal 3, got %d", r[0].AsInt16())
		}
	}
}
