package op

import (
	"matrixone/pkg/container/types"
)

type OP interface {
	Name() string
	Rename(string)
	String() string
	Columns() []string
	Attribute() map[string]types.Type
}
