package pixidb

import (
	"errors"
	"fmt"
)

var (
	ErrZeroColumns = errors.New("cannot create a table with zero columns")
)

type TableNotFoundError struct {
	Table string
}

func NewTableNotFoundError(tableName string) TableNotFoundError {
	return TableNotFoundError{
		Table: tableName,
	}
}

func (t TableNotFoundError) Error() string {
	return fmt.Sprintf("table '%s' not found in database", t.Table)
}

type ColumnNotFoundError struct {
	Store  string
	Column string
}

func NewColumnNotFoundError(store string, column string) *ColumnNotFoundError {
	return &ColumnNotFoundError{
		Store:  store,
		Column: column,
	}
}

func (c ColumnNotFoundError) Error() string {
	return fmt.Sprintf("column '%s' not found in store '%s'", c.Column, c.Store)
}

type LocationNotSupportedError struct {
	Projection string
	Location   Location
}

func NewLocationNotSupportedError(projection string, location Location) *LocationNotSupportedError {
	return &LocationNotSupportedError{
		Projection: projection,
		Location:   location,
	}
}

func (l LocationNotSupportedError) Error() string {
	return fmt.Sprintf("location %v not supported by projection %s", l.Location, l.Projection)
}

type LocationOutOfBoundsError struct {
	Location Location
}

func NewLocationOutOfBoundsError(location Location) LocationOutOfBoundsError {
	return LocationOutOfBoundsError{Location: location}
}

func (l LocationOutOfBoundsError) Error() string {
	return fmt.Sprintf("location %v was out of bounds", l.Location)
}
