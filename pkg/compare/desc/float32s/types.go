package float32s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]float32
	ns []*nulls.Nulls
	vs []*vector.Vector
}
