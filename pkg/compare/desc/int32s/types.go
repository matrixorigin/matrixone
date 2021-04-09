package int32s

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]int32
	ns []*nulls.Nulls
	vs []*vector.Vector
}
