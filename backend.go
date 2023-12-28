package pixidb

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

// A non-decomposable value in the table, one 'cell' of memory/storage.
type Field interface {
	AsInt8() int8
	AsUint8() uint8
	AsInt16() int16
	AsUint16() uint16
	AsInt32() int32
	AsUint32() uint32
	AsInt64() int64
	AsUint64() uint64
	AsFloat32() float32
	AsFloat64() float64
}

// The metadata that describes a field in the table.
type Column struct {
	Type ColumnType
	Name string
}

type ResultRow []Field

type ResultSet struct {
	Columns []Column
	Rows    []ResultRow
}

type Backend interface {
	CreateTable(*CreateTableStatement) error
	AlterTable(*AlterTableStatement) error
	DropTable(*DropTableStatement) error
	Select(*SelectStatement) (*ResultSet, error)
	Update(*UpdateStatement) error
	CreateShape(*CreateShapeStatement) error
	DropShape(*DropShapeStatement) error
	AlterShape(*AlterShapeStatement) error
	CreateUser(*CreateUserStatement) error
	DropUser(*DropUserStatement) error
	AlterUser(*AlterUserStatement) error
}

type CreateTableStatement struct {
	Name       string
	Columns    []Column
	Projection Projection
}
