// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (err error) {
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)

	logutil.Infof("FULLTEXTINDESCSCAN PREPARE")
	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return err
}

// run SQL here
func fulltextIndexScanCall(_ int, proc *process.Process, tableFunction *TableFunction, result *vm.CallResult) (bool, error) {

	var (
		err  error
		rbat *batch.Batch
	)
	bat := result.Batch
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()
	if bat == nil {
		return true, nil
	}

	logutil.Infof("FULLTEXTINDEXSCAN CALL")

	for i, arg := range tableFunction.Args {
		logutil.Infof("ARG %d: %s", i, arg.String())
	}

	logutil.Infof("PARAM : %s", string(tableFunction.Params))

	for i, attr := range tableFunction.Attrs {
		logutil.Infof("ATTRS %d: %s", i, attr)
	}

	v, err := tableFunction.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: first argument must be string, but got %s", v.GetType().String()))
	}

	index_table := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[1].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: second argument must be string, but got %s", v.GetType().String()))
	}

	pk_json := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[2].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: third argument must be string, but got %s", v.GetType().String()))
	}

	keys := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[3].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: fourth argument must be string, but got %s", v.GetType().String()))
	}

	pattern := v.UnsafeGetStringAt(0)

	logutil.Infof("index %s, pk_json %s, key %s, pattern %s", index_table, pk_json, keys, pattern)

	result.Batch.SetRowCount(0)
	return false, nil
}
