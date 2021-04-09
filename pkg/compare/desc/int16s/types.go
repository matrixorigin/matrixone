package int16s

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]int16
	ns []*nulls.Nulls
	vs []*vector.Vector
}
