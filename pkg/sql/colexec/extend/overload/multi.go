package overload

import (
	"fmt"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func MultiEval(op int, typ types.T, _ []bool, _ []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	return nil, fmt.Errorf("%s not yet implemented for %s", OpName[op], typ)
}

var MultiOps = map[int][]*MultiOp{}
