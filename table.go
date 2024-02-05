package pixidb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const TableFileExt string = ".tbl.json"

const (
	ProjectionKey string = "projection"
	CreatedAt     string = "created-at"
)

type ResultSet struct {
	Columns []Column
	Rows    [][]Value
}

type Table struct {
	store       *Store
	Indexer     LocationIndexer   `json:"indexer"`
	IndexerName string            `json:"indexerName"`
	Metadata    map[string]string `json:"metadata"`
}

func NewTable(path string, indexer LocationIndexer, columns ...Column) (*Table, error) {
	store, err := NewStore(path, indexer.Size(), columns...)
	if err != nil {
		return nil, err
	}

	table := &Table{
		store:       store,
		Indexer:     indexer,
		IndexerName: indexer.Name(),
		Metadata:    map[string]string{},
	}

	created, _ := time.Now().UTC().MarshalText()
	table.Metadata[ProjectionKey] = indexer.Name()
	table.Metadata[CreatedAt] = string(created)

	if err := table.saveTableMetadata(); err != nil {
		return nil, err
	}
	return table, nil
}

func OpenTable(path string) (*Table, error) {
	store, err := OpenStore(path)
	if err != nil {
		return nil, err
	}

	// load the table metadata too
	metaFilePath := filepath.Join(path, store.Name+TableFileExt)
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, err
	}
	defer metaFile.Close()

	jsonText, err := io.ReadAll(metaFile)
	if err != nil {
		return nil, err
	}
	table := &Table{store: store}
	err = json.Unmarshal(jsonText, table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (t *Table) Path() string {
	return t.store.Path()
}

func (t *Table) Name() string {
	return t.store.Name
}

func (t *Table) SetMetadata(key string, value string) error {
	t.Metadata[key] = value
	return t.saveTableMetadata()
}

// Save the table metadata alongside the store metadata and data file.
func (t *Table) saveTableMetadata() error {
	jsonData, err := json.Marshal(t)
	if err != nil {
		return err
	}
	tableFilePath := filepath.Join(t.store.path, t.store.Name+TableFileExt)
	tableFile, err := os.OpenFile(tableFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer tableFile.Close()
	if _, err = tableFile.Write(jsonData); err != nil {
		return err
	}
	return nil
}

func (t *Table) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		return err
	}

	// get the simple key-value map
	t.Metadata = map[string]string{}
	err = json.Unmarshal(*objMap["metadata"], &t.Metadata)
	if err != nil {
		return err
	}

	// get the name of the indexer used originally
	err = json.Unmarshal(*objMap["indexerName"], &t.IndexerName)
	if err != nil {
		return err
	}

	// now we can construct the right indexer
	switch t.IndexerName {
	case "projectionless":
		var p ProjectionlessIndexer
		err = json.Unmarshal(*objMap["indexer"], &p)
		if err != nil {
			return err
		}
		t.Indexer = p
	case "mercator-cutoff":
		var m MercatorCutoffIndexer
		err = json.Unmarshal(*objMap["indexer"], &m)
		if err != nil {
			return err
		}
		t.Indexer = m
	case "cylindrical-equirectangular":
		var c CylindricalEquirectangularIndexer
		err = json.Unmarshal(*objMap["indexer"], &c)
		if err != nil {
			return err
		}
		t.Indexer = c
	case "flat-healpix":
		var h FlatHealpixIndexer
		err = json.Unmarshal(*objMap["indexer"], &h)
		if err != nil {
			return err
		}
		t.Indexer = h
	default:
		return fmt.Errorf("pixidb: unknown table indexer scheme encountered while loading")
	}

	return nil
}

func (t *Table) Drop() error {
	return t.store.Drop()
}

func (t *Table) GetRows(projectedColumns []string, locations ...Location) (ResultSet, error) {
	columnProj, err := t.store.Projection(projectedColumns...)
	if err != nil {
		return ResultSet{}, err
	}
	rows := make([][]Value, len(locations))
	for i, loc := range locations {
		locIndex, err := t.Indexer.ToIndex(loc)
		if err != nil {
			return ResultSet{}, err
		}
		rawRow, err := t.store.GetRowAt(locIndex)
		if err != nil {
			return ResultSet{}, err
		}
		projRow := rawRow.Project(columnProj)
		rows[i] = projRow
	}
	return ResultSet{
		Columns: t.store.FilterColumns(columnProj),
		Rows:    rows,
	}, nil
}

func (t *Table) SetRows(columns []string, locations []Location, values [][]Value) (int, error) {
	columnProj, err := t.store.Projection(columns...)
	if err != nil {
		return 0, err
	}
	for i, loc := range locations {
		rowInd, err := t.Indexer.ToIndex(loc)
		if err != nil {
			return i, err
		}
		rawRow, err := t.store.GetRowAt(rowInd)
		if err != nil {
			return i, err
		}

		for vInd, c := range columnProj {
			copy(rawRow[c.start:c.start+c.size], values[i][vInd])
		}
		err = t.store.SetRowAt(rowInd, rawRow)
		if err != nil {
			return i, err
		}
	}
	return len(locations), nil
}

func (t *Table) SetValue(column string, location Location, value Value) error {
	rowInd, err := t.Indexer.ToIndex(location)
	if err != nil {
		return err
	}
	return t.store.SetValueAt(column, rowInd, value)
}

func (t *Table) Checkpoint() error {
	return t.store.Checkpoint()
}
