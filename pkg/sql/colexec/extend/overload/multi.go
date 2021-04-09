package overload

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

func MultiEval(op int, typ types.T, _ []bool, _ []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	return nil, fmt.Errorf("%s not yet implemented for %s", OpName[op], typ)
}

var MultiOps = map[int][]*MultiOp{}
