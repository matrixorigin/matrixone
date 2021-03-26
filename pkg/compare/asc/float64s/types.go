package float64s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]float64
	ns []*nulls.Nulls
	vs []*vector.Vector
}
