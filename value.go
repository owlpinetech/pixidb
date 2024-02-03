package pixidb

import (
	"encoding/binary"
	"math"
)

type Row []byte

func (r Row) Project(proj Projection) []Value {
	vals := make([]Value, len(proj))
	for i, column := range proj {
		vals[i] = Value(r[column.start : column.start+column.size])
	}
	return vals
}

type Value []byte

func NewInt8Value(val int8) Value {
	return []byte{byte(val)}
}

func NewUint8Value(val uint8) Value {
	return []byte{val}
}

func NewInt16Value(val int16) Value {
	v := make([]byte, 2)
	binary.BigEndian.PutUint16(v, uint16(val))
	return v
}

func NewUint16Value(val uint16) Value {
	v := make([]byte, 2)
	binary.BigEndian.PutUint16(v, val)
	return v
}

func NewInt32Value(val int32) Value {
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v, uint32(val))
	return v
}

func NewUint32Value(val uint32) Value {
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v, val)
	return v
}

func NewInt64Value(val int64) Value {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(val))
	return v
}

func NewUint64Value(val uint64) Value {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, val)
	return v
}

func NewFloat32Value(val float32) Value {
	return NewUint32Value(math.Float32bits(val))
}

func NewFloat64Value(val float64) Value {
	return NewUint64Value(math.Float64bits(val))
}

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
