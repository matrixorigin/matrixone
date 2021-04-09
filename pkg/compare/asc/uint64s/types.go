package uint64s

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]uint64
	ns []*nulls.Nulls
	vs []*vector.Vector
}
