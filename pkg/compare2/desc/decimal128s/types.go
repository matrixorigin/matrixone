package decimal128s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type compare struct {
	xs [][]types.Decimal128
	ns []*nulls.Nulls
	vs []*vector.Vector
}
