package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TableFunctionArgument struct {
	Rets   []*plan.ColDef
	Args   []*plan.Expr
	Attrs  []string
	Params []byte
	Name   string
}

func (arg *TableFunctionArgument) Free(proc *process.Process, pipelineFailed bool) {

}
