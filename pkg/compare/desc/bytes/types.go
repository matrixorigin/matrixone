package bytes

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	ns []*nulls.Nulls
	vs []*vector.Bytes
}
