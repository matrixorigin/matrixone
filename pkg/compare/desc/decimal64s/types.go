package decimal64s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]types.Decimal64
	ns []*nulls.Nulls
	vs []*vector.Vector
}
