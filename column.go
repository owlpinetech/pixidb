package pixidb

import (
	"encoding/binary"
	"math"
)

// Type representing the PixiDB 'types' of values that can be stored
// in a field in a table. These types are the 'atomic' types of PixiDB.
type ColumnType int16

const (
	ColumnTypeInt8 ColumnType = iota
	ColumnTypeUint8
	ColumnTypeInt16
	ColumnTypeUint16
	ColumnTypeInt32
	ColumnTypeUint32
	ColumnTypeInt64
	ColumnTypeUint64
	ColumnTypeFloat32
	ColumnTypeFloat64
)

// The size in bytes of this particular column type.
func (c ColumnType) Size() int {
	switch c {
	case ColumnTypeInt8:
		fallthrough
	case ColumnTypeUint8:
		return 1
	case ColumnTypeInt16:
		fallthrough
	case ColumnTypeUint16:
		return 2
	case ColumnTypeInt32:
		fallthrough
	case ColumnTypeUint32:
		fallthrough
	case ColumnTypeFloat32:
		return 4
	case ColumnTypeInt64:
		fallthrough
	case ColumnTypeUint64:
		fallthrough
	case ColumnTypeFloat64:
		return 8
	}
	return 0
}

// Given a standard Go value, encodes it according to the type of the column. The column
// type must match the type of the Go value.
func (c ColumnType) EncodeValue(val any) Value {
	retArr := make([]byte, c.Size())
	switch c {
	case ColumnTypeInt8:
		retArr[0] = byte(val.(int8))
	case ColumnTypeUint8:
		retArr[1] = byte(val.(uint8))
	case ColumnTypeInt16:
		binary.BigEndian.PutUint16(retArr, uint16(val.(int16)))
	case ColumnTypeUint16:
		binary.BigEndian.PutUint16(retArr, val.(uint16))
	case ColumnTypeInt32:
		binary.BigEndian.PutUint32(retArr, uint32(val.(int32)))
	case ColumnTypeUint32:
		binary.BigEndian.PutUint32(retArr, val.(uint32))
	case ColumnTypeInt64:
		binary.BigEndian.PutUint64(retArr, uint64(val.(int64)))
	case ColumnTypeUint64:
		binary.BigEndian.PutUint64(retArr, val.(uint64))
	case ColumnTypeFloat32:
		binary.BigEndian.PutUint32(retArr, math.Float32bits(val.(float32)))
	case ColumnTypeFloat64:
		binary.BigEndian.PutUint64(retArr, math.Float64bits(val.(float64)))
	default:
		panic("pixidb: invalid column type specification")
	}
	return retArr
}

// The metadata that describes a column of data in the table. Each column has a name used to refer to it
// in queries. The type describes the range of values able to be stored in the column (and their in-memory size),
// and the default value will prepopulate the column's slot in every row when the table is created. There are
// no nullable columns in PixiDB.
type Column struct {
	Name    string
	Type    ColumnType
	Default Value
}

// Create a new column description with the given name, type, and encoded default value for the type.
func NewColumnEncoded(name string, ctype ColumnType, defval Value) Column {
	if len(defval) != ctype.Size() {
		panic("pixidb: default value size does not match specified column size")
	}
	return Column{
		Name:    name,
		Type:    ctype,
		Default: defval,
	}
}

// Create a new column description with the given name and type, and a default Go value that is encoded
// before being assigned to the column.
func NewColumnUnencoded(name string, ctype ColumnType, defval any) Column {
	return NewColumnEncoded(name, ctype, ctype.EncodeValue(defval))
}

// Create a new Int8-sized column with the given name and default value.
func NewColumnInt8(name string, defval int8) Column {
	return NewColumnEncoded(name, ColumnTypeInt8, []byte{byte(defval)})
}

// Create a new Uint8-sized column with the given name and default value.
func NewColumnUint8(name string, defval uint8) Column {
	return NewColumnEncoded(name, ColumnTypeUint8, []byte{defval})
}

// Create a new Int16-sized column with the given name and default value.
func NewColumnInt16(name string, defval int16) Column {
	return NewColumnUnencoded(name, ColumnTypeInt16, defval)
}

// Create a new Uint16-sized column with the given name and default value.
func NewColumnUint16(name string, defval uint16) Column {
	return NewColumnUnencoded(name, ColumnTypeUint16, defval)
}

// Create a new Int32-sized column with the given name and default value.
func NewColumnInt32(name string, defval int32) Column {
	return NewColumnUnencoded(name, ColumnTypeInt32, defval)
}

// Create a new Uint32-sized column with the given name and default value.
func NewColumnUint32(name string, defval uint32) Column {
	return NewColumnUnencoded(name, ColumnTypeUint32, defval)
}

// Create a new Int64-sized column with the given name and default value.
func NewColumnInt64(name string, defval int64) Column {
	return NewColumnUnencoded(name, ColumnTypeInt64, defval)
}

// Create a new Uint64-sized column with the given name and default value.
func NewColumnUint64(name string, defval uint64) Column {
	return NewColumnUnencoded(name, ColumnTypeUint64, defval)
}

// Create a new Float32-sized column with the given name and default value.
func NewColumnFloat32(name string, defval float32) Column {
	return NewColumnUnencoded(name, ColumnTypeFloat32, defval)
}

// Create a new Float64-sized column with the given name and default value.
func NewColumnFloat64(name string, defval float64) Column {
	return NewColumnUnencoded(name, ColumnTypeFloat64, defval)
}

// The number of bytes that values of this column will consume on disk.
func (c Column) Size() int {
	return c.Type.Size()
}

// Encodes a Go value according to the type of the column. The type of the input Go value
// should match the specified type of the column.
func (c Column) EncodeValue(val any) Value {
	return c.Type.EncodeValue(val)
}
