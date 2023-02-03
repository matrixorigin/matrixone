// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	tblArg := arg.(*Argument)
	switch tblArg.Name {
	case "unnest":
		return unnestCall(idx, proc, tblArg)
	case "generate_series":
		return generateSeriesCall(idx, proc, tblArg)
	case "meta_scan":
		return metaScanCall(idx, proc, tblArg)
	case "current_account":
		return currentAccountCall(idx, proc, tblArg)
	default:
		return true, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString(arg.(*Argument).Name)
}

func Prepare(proc *process.Process, arg any) error {
	tblArg := arg.(*Argument)
	switch tblArg.Name {
	case "unnest":
		return unnestPrepare(proc, tblArg)
	case "generate_series":
		return generateSeriesPrepare(proc, tblArg)
	case "meta_scan":
		return metaScanPrepare(proc, tblArg)
	case "current_account":
		return currentAccountPrepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale, typ.Precision)
}
