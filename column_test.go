package pixidb

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"
)

func TestColumnConstructors(t *testing.T) {
	fl32bits := make([]byte, 4)
	fl64bits := make([]byte, 8)
	binary.BigEndian.PutUint32(fl32bits, math.Float32bits(5.0))
	binary.BigEndian.PutUint64(fl64bits, math.Float64bits(5.0))

	testCases := []struct {
		created       Column
		expectType    ColumnType
		expectDefault []byte
		expectSize    int
	}{
		{NewColumnInt8("col1", 1), ColumnTypeInt8, []byte{1}, 1},
		{NewColumnUint8("col2", 2), ColumnTypeUint8, []byte{2}, 1},
		{NewColumnInt16("col3", 1), ColumnTypeInt16, []byte{0, 1}, 2},
		{NewColumnUint16("col4", 2), ColumnTypeUint16, []byte{0, 2}, 2},
		{NewColumnInt32("col5", 1), ColumnTypeInt32, []byte{0, 0, 0, 1}, 4},
		{NewColumnUint32("col6", 2), ColumnTypeUint32, []byte{0, 0, 0, 2}, 4},
		{NewColumnInt64("col7", 1), ColumnTypeInt64, []byte{0, 0, 0, 0, 0, 0, 0, 1}, 8},
		{NewColumnUint64("col8", 2), ColumnTypeUint64, []byte{0, 0, 0, 0, 0, 0, 0, 2}, 8},
		{NewColumnFloat32("col9", 5.0), ColumnTypeFloat32, fl32bits, 4},
		{NewColumnFloat64("col10", 5.0), ColumnTypeFloat64, fl64bits, 8},
	}

	for _, tc := range testCases {
		t.Run(tc.created.Name, func(t *testing.T) {
			if tc.created.Type != tc.expectType {
				t.Errorf("expected type %v, got %v", tc.expectType, tc.created.Type)
			}
			if !slices.Equal(tc.created.Default, tc.expectDefault) {
				t.Errorf("expected default %v, got %v", tc.expectDefault, tc.created.Default)
			}
			if tc.created.Size() != tc.expectSize {
				t.Errorf("expected size %v, got %v", tc.expectSize, tc.created.Size())
			}
		})
	}
}
