package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func MoFlushTable(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vectors) != 1 {
		return nil, moerr.NewInvalidInput("no name")
	}
	return nil, nil
}
