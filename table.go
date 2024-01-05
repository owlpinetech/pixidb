package pixidb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const TableFileExt string = ".tbl.json"

type Table struct {
	store       *Store
	Indexer     LocationIndexer   `json:"indexer"`
	IndexerName string            `json:"indexerName"`
	Metadata    map[string]string `json:"metadata"`
}

func NewTable(path string, indexer LocationIndexer, columns []Column) (*Table, error) {
	store, err := NewStore(path, indexer.Size(), columns)
	if err != nil {
		return nil, err
	}

	table := &Table{
		store:       store,
		Indexer:     indexer,
		IndexerName: indexer.Name(),
		Metadata:    map[string]string{},
	}

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
