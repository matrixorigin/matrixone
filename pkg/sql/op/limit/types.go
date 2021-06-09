package limit

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Limit struct {
	Prev  op.OP
	Limit int64
	ID    string
	Attrs map[string]types.Type
}
