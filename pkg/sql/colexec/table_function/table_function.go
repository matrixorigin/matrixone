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

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	tblArg := arg.(*Argument)
	var (
		f bool
		e error
	)

	switch tblArg.Name {
	case "unnest":
		f, e = unnestCall(idx, proc, tblArg)
	case "generate_series":
		f, e = generateSeriesCall(idx, proc, tblArg)
	case "meta_scan":
		f, e = metaScanCall(idx, proc, tblArg)
	case "current_account":
		f, e = currentAccountCall(idx, proc, tblArg)
	case "metadata_scan":
		f, e = metadataScan(idx, proc, tblArg)
	case "processlist":
		f, e = processlist(idx, proc, tblArg)
	default:
		return process.ExecStop, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
	if e != nil || f {
		if f {
			return process.ExecStop, e
		}
		return process.ExecNext, e
	}

	bat := proc.InputBatch()
	if bat == nil {
		return process.ExecStop, e
	}
	if bat.IsEmpty() {
		return process.ExecNext, e
	}

	if bat.VectorCount() != len(tblArg.retSchema) {
		return process.ExecStop, moerr.NewInternalError(proc.Ctx, "table function %s return length mismatch", tblArg.Name)
	}
	for i := range tblArg.retSchema {
		if proc.InputBatch().GetVector(int32(i)).GetType().Oid != tblArg.retSchema[i].Oid {
			return process.ExecStop, moerr.NewInternalError(proc.Ctx, "table function %s return type mismatch", tblArg.Name)
		}
	}

	if f {
		return process.ExecStop, e
	}
	return process.ExecNext, e
}

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString(arg.(*Argument).Name)
}

func Prepare(proc *process.Process, arg any) error {
	tblArg := arg.(*Argument)

	retSchema := make([]types.Type, len(tblArg.Rets))
	for i := range tblArg.Rets {
		retSchema[i] = dupType(tblArg.Rets[i].Typ)
	}
	tblArg.retSchema = retSchema

	switch tblArg.Name {
	case "unnest":
		return unnestPrepare(proc, tblArg)
	case "generate_series":
		return generateSeriesPrepare(proc, tblArg)
	case "meta_scan":
		return metaScanPrepare(proc, tblArg)
	case "current_account":
		return currentAccountPrepare(proc, tblArg)
	case "metadata_scan":
		return metadataScanPrepare(proc, tblArg)
	case "processlist":
		return processlistPrepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.Name))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}
