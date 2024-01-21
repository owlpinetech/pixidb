package pixidb

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
)

type ColumnProjection struct {
	index int
	start int
	size  int
}

type Projection []ColumnProjection

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
	path      string
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
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
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
		path:      path,
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
	metaFile, err := os.Create(metaFilePath)
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
	for i, c := range columns {
		columnMap[c.Name] = ColumnProjection{i, columnOffset, c.Size()}
		columnOffset += c.Size()
	}
	return columnMap
}

func (s *Store) Path() string {
	return s.path
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

func (s *Store) FilterColumns(proj Projection) []Column {
	columns := make([]Column, len(proj))
	for i, p := range proj {
		columns[i] = s.ColumnSet[p.index]
	}
	return columns
}

func (s *Store) GetRowAt(index int) (Row, error) {
	pageIndex := index / s.rowsPerPage
	rowOffset := (index % s.rowsPerPage) * s.rowSize
	return s.file.GetChunk(pageIndex, rowOffset, s.rowSize)
}

// Cheat method when a store has only a single column and we don't need
// to do any projection (because it's the only column)
func (s *Store) GetValueAt(index int) (Value, error) {
	pageIndex := index / s.rowsPerPage
	rowOffset := (index % s.rowsPerPage) * s.rowSize
	return s.file.GetChunk(pageIndex, rowOffset, s.rowSize)
}

func (s *Store) SetRowAt(index int, row Row) error {
	pageIndex := index / s.rowsPerPage
	rowOffset := (index % s.rowsPerPage) * s.rowSize
	return s.file.SetChunk(pageIndex, rowOffset, row)
}

func (s *Store) Checkpoint() error {
	return s.file.FlushAllPages()
}

func (s *Store) Drop() error {
	s.file.ClearCache()
	return os.RemoveAll(s.path)
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

type Row []byte

func (r Row) Project(proj Projection) []Value {
	vals := make([]Value, len(proj))
	for i, column := range proj {
		vals[i] = Value(r[column.start : column.start+column.size])
	}
	return vals
}

type Value []byte

func (v Value) AsInt8() int8 {
	return int8(v[0])
}

func (v Value) AsUint8() uint8 {
	return uint8(v[0])
}

func (v Value) AsInt16() int16 {
	return int16(binary.BigEndian.Uint16(v))
}

func (v Value) AsUint16() uint16 {
	return binary.BigEndian.Uint16(v)
}

func (v Value) AsInt32() int32 {
	return int32(binary.BigEndian.Uint32(v))
}

func (v Value) AsUint32() uint32 {
	return binary.BigEndian.Uint32(v)
}

func (v Value) AsInt64() int64 {
	return int64(binary.BigEndian.Uint64(v))
}

func (v Value) AsUint64() uint64 {
	return binary.BigEndian.Uint64(v)
}

func (v Value) AsFloat32() float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(v))
}

func (v Value) AsFloat64() float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(v))
}
