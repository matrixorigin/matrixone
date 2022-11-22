package table_function

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function/generate_series"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function/unnest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	tblArg := arg.(*colexec.TableFunctionArgument)
	switch tblArg.Name {
	case "unnest":
		return unnest.Call(idx, proc, tblArg)
	case "generate_series":
		return generate_series.Call(idx, proc, tblArg)
	default:
		return true, moerr.NewNotSupported(fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString(arg.(*colexec.TableFunctionArgument).Name)
}

func Prepare(proc *process.Process, arg any) error {
	tblArg := arg.(*colexec.TableFunctionArgument)
	switch tblArg.Name {
	case "unnest":
		return unnest.Prepare(proc, tblArg)
	case "generate_series":
		return generate_series.Prepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}
