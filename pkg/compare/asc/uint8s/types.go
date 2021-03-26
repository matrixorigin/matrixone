package uint8s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]uint8
	ns []*nulls.Nulls
	vs []*vector.Vector
}
