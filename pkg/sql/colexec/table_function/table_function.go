package table_function

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	tblArg := arg.(*TableFunctionArgument)
	switch tblArg.Name {
	case "unnest":
		return unnestCall(idx, proc, tblArg)
	case "generate_series":
		return generateSeriesCall(idx, proc, tblArg)
	default:
		return true, moerr.NewNotSupported(fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString(arg.(*TableFunctionArgument).Name)
}

func Prepare(proc *process.Process, arg any) error {
	tblArg := arg.(*TableFunctionArgument)
	switch tblArg.Name {
	case "unnest":
		return unnestPrepare(proc, tblArg)
	case "generate_series":
		return generateSeriesPrepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}
