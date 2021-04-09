package uint32s

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]uint32
	ns []*nulls.Nulls
	vs []*vector.Vector
}
