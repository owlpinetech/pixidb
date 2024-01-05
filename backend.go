package pixidb

// A non-decomposable value in the table, one 'cell' of memory/storage.
type Value interface {
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

type ResultRow []Value

type ResultSet struct {
	Columns []Column
	Rows    []ResultRow
}

type Config interface {
	Rows() uint64
}

type SingleResHealpix struct {
	order int
}

//func (h SingleResHealpix) Rows() {
//	return h.order
//}

type Backend interface {
	CreateTable(*CreateTableStatement) error
	//AlterTable(*AlterTableStatement) error
	//DropTable(*DropTableStatement) error
	Select(*SelectStatement) (*ResultSet, error)
	Update(*UpdateStatement) error
	//CreateShape(*CreateShapeStatement) error
	//DropShape(*DropShapeStatement) error
	//AlterShape(*AlterShapeStatement) error
	//CreateUser(*CreateUserStatement) error
	//DropUser(*DropUserStatement) error
	//AlterUser(*AlterUserStatement) error
}

type CreateTableStatement struct {
	Name     string
	Columns  []Column
	PageSize int
	Config   Config
}

type SelectStatement struct {
	Table     string
	Columns   []string
	Locations []LocationClause
	//Filters   []FilterClause
}

type UpdateStatement struct {
	Table   string
	Columns []string
	Values  [][]Value
}

type LocationClause interface {
	//[]Row Access()
}
