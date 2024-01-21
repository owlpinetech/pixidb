package pixidb

import (
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/exp/maps"
)

type Database struct {
	dbPath string
	tables map[string]*Table
	lock   sync.RWMutex
}

func NewDatabase(dbPath string) (*Database, error) {
	// make sure the directory exists
	os.RemoveAll(dbPath)
	if err := os.MkdirAll(dbPath, os.ModeDir); err != nil {
		return nil, err
	}

	return &Database{
		dbPath: dbPath,
		tables: map[string]*Table{},
		lock:   sync.RWMutex{},
	}, nil
}

func OpenDatabase(dbPath string) (*Database, error) {
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	tables := map[string]*Table{}
	for _, e := range entries {
		if e.IsDir() {
			table, err := OpenTable(filepath.Join(dbPath, e.Name()))
			if err != nil {
				return nil, err
			}
			tables[e.Name()] = table
		}
	}

	return &Database{
		dbPath: dbPath,
		tables: tables,
		lock:   sync.RWMutex{},
	}, nil
}

func (d *Database) Create(tableName string, indexer LocationIndexer, columns ...Column) error {
	table, err := NewTable(filepath.Join(d.dbPath, tableName), indexer, columns)
	if err != nil {
		return err
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.tables[tableName] = table
	return nil
}

func (d *Database) Drop(tableName string) error {
	err := d.tables[tableName].Drop()

	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.tables, tableName)
	return err
}

func (d *Database) GetTableNames() ([]string, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return maps.Keys(d.tables), nil
}

func (d *Database) GetColumns(tableName string) ([]Column, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table, ok := d.tables[tableName]; !ok {
		return nil, NewTableNotFoundError(tableName)
	} else {
		return table.store.ColumnSet, nil
	}
}

func (d *Database) GetRows(tableName string, columns []string, locations ...Location) (ResultSet, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table, ok := d.tables[tableName]; !ok {
		return ResultSet{}, NewTableNotFoundError(tableName)
	} else {
		return table.GetRows(columns, locations)
	}
}

func (d *Database) SetRows(tableName string, columns []string, locations []Location, values [][]Value) (int, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table, ok := d.tables[tableName]; !ok {
		return 0, NewTableNotFoundError(tableName)
	} else {
		return table.SetRows(columns, locations, values)
	}
}

func (d *Database) GetMetadata(tableName string, key string) (string, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table, ok := d.tables[tableName]; !ok {
		return "", NewTableNotFoundError(tableName)
	} else {
		if metadata, ok := table.Metadata[key]; !ok {
			return "", nil
		} else {
			return metadata, nil
		}
	}
}

func (d *Database) SetMetadata(tableName string, key string, value string) error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table, ok := d.tables[tableName]; !ok {
		return NewTableNotFoundError(tableName)
	} else {
		return table.SetMetadata(key, value)
	}
}

func (d *Database) Checkpoint() error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	for _, tbl := range d.tables {
		if err := tbl.Checkpoint(); err != nil {
			return err
		}
	}
	return nil
}
