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

	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "table_function"

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	tblArg := arg
	var (
		f bool
		e error
	)
	idx := arg.GetIdx()

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	switch tblArg.FuncName {
	case "unnest":
		f, e = unnestCall(idx, proc, tblArg, &result)
	case "generate_series":
		f, e = generateSeriesCall(idx, proc, tblArg, &result)
	case "meta_scan":
		f, e = metaScanCall(idx, proc, tblArg, &result)
	case "current_account":
		f, e = currentAccountCall(idx, proc, tblArg, &result)
	case "metadata_scan":
		f, e = metadataScan(idx, proc, tblArg, &result)
	case "processlist":
		f, e = processlist(idx, proc, tblArg, &result)
	case "mo_locks":
		f, e = moLocksCall(idx, proc, tblArg, &result)
	case "mo_configurations":
		f, e = moConfigurationsCall(idx, proc, tblArg, &result)
	case "mo_transactions":
		f, e = moTransactionsCall(idx, proc, tblArg, &result)
	case "mo_cache":
		f, e = moCacheCall(idx, proc, tblArg, &result)
	default:
		result.Status = vm.ExecStop
		return result, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}
	if e != nil || f {
		if f {
			result.Status = vm.ExecStop
			return result, e
		}
		return result, e
	}

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}
	arg.buf = result.Batch
	if arg.buf == nil {
		result.Status = vm.ExecStop
		return result, e
	}
	if arg.buf.IsEmpty() {
		return result, e
	}

	if arg.buf.VectorCount() != len(tblArg.retSchema) {
		result.Status = vm.ExecStop
		return result, moerr.NewInternalError(proc.Ctx, "table function %s return length mismatch", tblArg.FuncName)
	}
	for i := range tblArg.retSchema {
		if arg.buf.GetVector(int32(i)).GetType().Oid != tblArg.retSchema[i].Oid {
			result.Status = vm.ExecStop
			return result, moerr.NewInternalError(proc.Ctx, "table function %s return type mismatch", tblArg.FuncName)
		}
	}

	if f {
		result.Status = vm.ExecStop
		return result, e
	}
	return result, e
}

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(arg.FuncName)
}

func (arg *Argument) Prepare(proc *process.Process) error {
	tblArg := arg
	tblArg.ctr = new(container)

	retSchema := make([]types.Type, len(tblArg.Rets))
	for i := range tblArg.Rets {
		retSchema[i] = dupType(&tblArg.Rets[i].Typ)
	}
	tblArg.retSchema = retSchema

	switch tblArg.FuncName {
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
	case "mo_locks":
		return moLocksPrepare(proc, tblArg)
	case "mo_configurations":
		return moConfigurationsPrepare(proc, tblArg)
	case "mo_transactions":
		return moTransactionsPrepare(proc, tblArg)
	case "mo_cache":
		return moCachePrepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}
