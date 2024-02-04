package pixidb

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/owlpinetech/flatsphere"
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
		proj     flatsphere.Projection
	}{
		{"mercatortagless", NewMercatorCutoffIndexer(math.Pi/4, -math.Pi/4, 10, 10, true), map[string]string{}, flatsphere.NewMercator()},
		{"cyleqtags", NewCylindricalEquirectangularIndexer(0, 10, 10, true), map[string]string{"one": "fish", "two": "fish"}, flatsphere.NewCylindricalEqualArea(0)},
		{"healpixtagged", NewFlatHealpixIndexer(2, healpix.NestScheme), map[string]string{"hello": "there"}, flatsphere.NewHEALPixStandard()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			orig, err := NewTable(filepath.Join(dir, tc.name), tc.indexer, Column{Name: "dummy", Type: ColumnTypeFloat32, Default: []byte{1, 2, 3, 4}})
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
			if tbl.Indexer.Size() != orig.Indexer.Size() {
				t.Errorf("expected table indexer size %d, got %d", orig.Indexer.Size(), tbl.Indexer.Size())
			}
			if tbl.Indexer.Projection() == nil {
				t.Errorf("projection not present for deserialize table")
			}

			if reflect.TypeOf(orig.Indexer) != reflect.TypeOf(tbl.Indexer) {
				t.Errorf("expected indexer type %T, got %T", orig.Indexer, tbl.Indexer)
			}
			if reflect.TypeOf(orig.Indexer.Projection()) != reflect.TypeOf(tbl.Indexer.Projection()) {
				t.Errorf("expected indexer type %T, got %T", orig.Indexer.Projection(), tbl.Indexer.Projection())
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
		Column{Name: "col1", Type: ColumnTypeInt32, Default: []byte{0, 0, 0, 3}},
		Column{Name: "col2", Type: ColumnTypeInt16, Default: []byte{0, 6}})
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

func TestTableSetGet(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_table_set_get")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tbl, err := NewTable(filepath.Join(dir, "querytbl"), NewCylindricalEquirectangularIndexer(0, 10, 10, true),
		Column{Name: "col1", Type: ColumnTypeInt32, Default: []byte{0, 0, 0, 3}})
	if err != nil {
		t.Fatal(err)
	}

	res, err := tbl.GetRows([]string{"col1"}, GridLocation{X: 0, Y: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 3 {
		t.Errorf("expected value to equal 3, got %d", res.Rows[0][0].AsInt32())
	}

	n, err := tbl.SetRows([]string{"col1"}, []Location{GridLocation{X: 0, Y: 0}}, [][]Value{{NewInt32Value(5)}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected to only update one row, got %d", n)
	}

	// verify we see the updated value
	res, err = tbl.GetRows([]string{"col1"}, GridLocation{X: 0, Y: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 5 {
		t.Errorf("expected value to equal 5, got %d", res.Rows[0][0].AsInt32())
	}

	// verify that further gets on different pixels don't have an updated value
	res, err = tbl.GetRows([]string{"col1"}, GridLocation{X: 1, Y: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 3 {
		t.Errorf("expected unchanged value to equal 3, got %d", res.Rows[0][0].AsInt32())
	}

	res, err = tbl.GetRows([]string{"col1"}, GridLocation{X: 0, Y: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 3 {
		t.Errorf("expected unchanged value to equal 3, got %d", res.Rows[0][0].AsInt32())
	}

	// verify again that we see the updated value
	res, err = tbl.GetRows([]string{"col1"}, GridLocation{X: 0, Y: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 5 {
		t.Errorf("expected value to equal 5, got %d", res.Rows[0][0].AsInt32())
	}

	// set the middle pixel
	n, err = tbl.SetRows([]string{"col1"}, []Location{GridLocation{X: 5, Y: 5}}, [][]Value{{NewInt32Value(8)}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected to only update one row, got %d", n)
	}

	// verify again that we see the updated value
	res, err = tbl.GetRows([]string{"col1"}, GridLocation{X: 5, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].AsInt32() != 8 {
		t.Errorf("expected value to equal 8, got %d", res.Rows[0][0].AsInt32())
	}
}

func TestSmallIterateGetSetGet(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_table_set_get")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tbl, err := NewTable(filepath.Join(dir, "querytbl"), NewCylindricalEquirectangularIndexer(0, 10, 10, true),
		Column{Name: "col1", Type: ColumnTypeInt16, Default: NewInt16Value(math.MaxInt16)})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < tbl.store.Rows; i++ {
		loc := GridLocation{X: i % 10, Y: i / 10}

		results, err := tbl.GetRows([]string{"col1"}, loc)
		if err != nil {
			t.Fatal(err)
		}
		if results.Rows[0][0].AsInt16() != math.MaxInt16 {
			t.Errorf("expected anti-set value to max-int, got %d", results.Rows[0][0].AsInt16())
		}

		n, err := tbl.SetRows([]string{"col1"}, []Location{loc}, [][]Value{{NewInt16Value(int16(i))}})
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("expected to only set 1 row, but set %d", n)
		}

		results, err = tbl.GetRows([]string{"col1"}, loc)
		if err != nil {
			t.Fatal(err)
		}
		if results.Rows[0][0].AsInt16() != int16(i) {
			t.Errorf("expected post-set value to max-int, got %d", results.Rows[0][0].AsInt16())
		}
	}
}

func TestTableSetAllPersist(t *testing.T) {
	dir, err := os.MkdirTemp(".", "pixidb_table_set_get")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tbl, err := NewTable(filepath.Join(dir, "querytbl"), NewCylindricalEquirectangularIndexer(0, 10, 10, true),
		Column{Name: "col1", Type: ColumnTypeInt16, Default: NewInt16Value(math.MaxInt16)})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < tbl.store.Rows; i++ {
		loc := GridLocation{X: i % 10, Y: i / 10}

		n, err := tbl.SetRows([]string{"col1"}, []Location{loc}, [][]Value{{NewInt16Value(int16(i))}})
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("expected to only set 1 row, but set %d", n)
		}
	}

	if err = tbl.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	opened, err := OpenTable(filepath.Join(dir, "querytbl"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < opened.store.Rows; i++ {
		loc := GridLocation{X: i % 10, Y: i / 10}

		rs, err := opened.GetRows([]string{"col1"}, loc)
		if err != nil {
			t.Fatal(err)
		}
		if rs.Rows[0][0].AsInt16() != int16(i) {
			t.Errorf("expected to get %d at index %d, but got %d", i, i, rs.Rows[0][0].AsInt16())
		}
	}
}
