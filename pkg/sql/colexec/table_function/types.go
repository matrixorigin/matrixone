package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

type unnestParam struct {
	Filters []string `json:"filters"`
	ColName string   `json:"colName"`
}

var (
	unnestDeniedFilters = []string{"col", "seq"}
)

const (
	unnestMode      = "both"
	unnestRecursive = false
)

type Number interface {
	int32 | int64 | types.Datetime
}
