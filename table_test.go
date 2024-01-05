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
	dir, err := os.MkdirTemp(os.TempDir(), "pixidb_table_basic_open")
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
