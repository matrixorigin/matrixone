package int8s

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]int8
	ns []*nulls.Nulls
	vs []*vector.Vector
}
