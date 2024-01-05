package pixidb

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

type RawRow []byte

type RawValue []byte

// Type representing the PixiDB 'types' of values that can be stored
// in a field in a table. These types are the 'atomic' types of PixiDB.
type ColumnType int16

const (
	FieldTypeInt8 ColumnType = iota
	FieldTypeUint8
	FieldTypeInt16
	FieldTypeUint16
	FieldTypeInt32
	FieldTypeUint32
	FieldTypeInt64
	FieldTypeUint64
	FieldTypeFloat32
	FieldTypeFloat64
)

// The size in bytes of this particular column type.
func (c ColumnType) Size() int {
	switch c {
	case FieldTypeInt8:
		fallthrough
	case FieldTypeUint8:
		return 1
	case FieldTypeInt16:
		fallthrough
	case FieldTypeUint16:
		return 2
	case FieldTypeInt32:
		fallthrough
	case FieldTypeUint32:
		fallthrough
	case FieldTypeFloat32:
		return 4
	case FieldTypeInt64:
		fallthrough
	case FieldTypeUint64:
		fallthrough
	case FieldTypeFloat64:
		return 8
	}
	return 0
}

// The metadata that describes a column of data in the table.
type Column struct {
	Name    string
	Type    ColumnType
	Default RawValue
}

func NewColumn(name string, ctype ColumnType, defval RawValue) Column {
	if len(defval) != ctype.Size() {
		panic("pixidb: default value size does not match specified column size")
	}
	return Column{
		Name:    name,
		Type:    ctype,
		Default: defval,
	}
}

// The number of bytes that values of this column will consume on disk.
func (c Column) Size() int {
	return c.Type.Size()
}

type ColumnProjection struct {
	start int
	size  int
}

type Projection []ColumnProjection

type Row interface {
	Project(Projection) []Value
}

const (
	DataFileExt     = ".dat"
	MetadataFileExt = ".meta.json"
	MaxPagesInCache = 64
)

// A simple set of rows, divided into fixed-size columns. The number of rows and columns both
// are known ahead of time, and the most efficient access pattern is by row index. A store
// keeps all of its data compact in one flat file, storing variable size metadata in a separate
// structured file.
type Store struct {
	// The name by which the store can be referenced in queries. Also the final folder in the path
	// in which the data file for this store is kept.
	Name      string   `json:"-"`
	ColumnSet []Column `json:"columns"`
	Rows      int      `json:"rows"`
	file      *Pagemaster

	columnMap   map[string]ColumnProjection // A way to quickly access the data mapping for a particular column name
	rowSize     int                         // The precomputed size of each row in the store
	rowsPerPage int                         // The precomputed number of rows in each disk page of the store
}

func NewStore(path string, rows int, columns []Column) (*Store, error) {
	if len(columns) < 1 {
		return nil, ErrZeroColumns
	}

	// make sure the directory exists
	if err := os.MkdirAll(path, os.ModeDir); err != nil {
		return nil, err
	}

	// the name of the store is the folder that it is stored in
	name := filepath.Base(path)

	dataFilePath := filepath.Join(path, name+DataFileExt)
	pagemaster := NewPagemaster(dataFilePath, MaxPagesInCache)

	// determine the size of the data file and other attributes related to it
	rowSize := 0
	defaultRow := make([]byte, 0)
	for _, c := range columns {
		rowSize += c.Size()
		defaultRow = append(defaultRow, c.Default...)
	}
	rowsPerPage := pagemaster.PageSize() / rowSize
	pages := (rows / rowsPerPage) + 1

	// create the metadata file, return early if that fails
	store := &Store{
		Name:      name,
		ColumnSet: columns,
		file:      pagemaster,
		Rows:      rows,

		columnMap:   nil,
		rowSize:     rowSize,
		rowsPerPage: rowsPerPage,
	}
	jsonData, err := json.Marshal(store)
	if err != nil {
		return nil, err
	}
	metaFilePath := filepath.Join(path, name+MetadataFileExt)
	metaFile, err := os.OpenFile(metaFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer metaFile.Close()
	if _, err = metaFile.Write(jsonData); err != nil {
		return nil, err
	}

	// create the data file and populate it with the column defaults
	defaultPage := make([]byte, 0)
	for i := 0; i < rowsPerPage; i++ {
		defaultPage = append(defaultPage, defaultRow...)
	}

	// TODO: check that there is enough disk space here and error out before attempting to create if not
	if err := pagemaster.Initialize(pages, defaultPage); err != nil {
		return nil, err
	}

	// lastly, map the columns to their projection indices in the column list
	store.columnMap = initColumnMap(columns)

	return store, nil
}

func OpenStore(path string) (*Store, error) {
	// the name of the store is the folder that it is stored in
	name := filepath.Base(path)

	// create a new paging layer, but no need to initialize it
	dataFilePath := filepath.Join(path, name+DataFileExt)
	pagemaster := NewPagemaster(dataFilePath, MaxPagesInCache)

	// read from the metadata file first
	metaFilePath := filepath.Join(path, name+MetadataFileExt)
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, err
	}
	defer metaFile.Close()

	jsonText, err := io.ReadAll(metaFile)
	if err != nil {
		return nil, err
	}
	store := &Store{Name: name, file: pagemaster}
	err = json.Unmarshal(jsonText, store)
	if err != nil {
		return nil, err
	}

	// determine the size of the data file and other attributes related to it
	store.rowSize = 0
	for _, c := range store.ColumnSet {
		store.rowSize += c.Size()
	}
	store.rowsPerPage = pagemaster.PageSize() / store.rowSize

	// lastly, map the columns to their projection indices in the column list
	store.columnMap = initColumnMap(store.ColumnSet)
	return store, nil
}

func initColumnMap(columns []Column) map[string]ColumnProjection {
	columnMap := make(map[string]ColumnProjection)
	columnOffset := 0
	for _, c := range columns {
		columnMap[c.Name] = ColumnProjection{columnOffset, c.Size()}
		columnOffset += c.Size()
	}
	return columnMap
}

func (s *Store) RowSize() int {
	return s.rowSize
}

func (s *Store) RowsPerPage() int {
	return s.rowsPerPage
}

func (s *Store) DefaultRow() []byte {
	defaultRow := make([]byte, 0)
	for _, c := range s.ColumnSet {
		defaultRow = append(defaultRow, c.Default...)
	}
	return defaultRow
}

func (s *Store) GetRowAt(index int) (RawRow, error) {
	pageIndex := index / s.rowsPerPage
	rowOffset := index % s.rowsPerPage
	return s.file.GetChunk(pageIndex, rowOffset*s.rowSize, s.rowSize)
}

func (s *Store) SetRowAt(index int, row RawRow) error {
	pageIndex := index / s.rowsPerPage
	rowOffset := index % s.rowsPerPage
	return s.file.SetChunk(pageIndex, rowOffset*s.rowSize, row)
}

func (s *Store) Checkpoint() error {
	return s.file.FlushAllPages()
}

func (s *Store) Projection(columns ...string) (Projection, error) {
	proj := make([]ColumnProjection, len(columns))
	for i, c := range columns {
		if cproj, ok := s.columnMap[c]; !ok {
			return nil, NewColumnNotFoundError(s.Name, c)
		} else {
			proj[i] = cproj
		}
	}
	return proj, nil
}
