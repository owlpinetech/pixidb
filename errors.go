package pixidb

import (
	"errors"
	"fmt"
)

var (
	ErrZeroColumns = errors.New("cannot create a table with zero columns")
)

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
