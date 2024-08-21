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
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// prepare
func stageListPrepare(proc *process.Process, tableFunction *TableFunction) (err error) {
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)

	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return err
}

// call
func stageListCall(_ int, proc *process.Process, tableFunction *TableFunction, result *vm.CallResult) (bool, error) {

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

	v, err := tableFunction.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("stage_list: filepath must be string, but got %s", v.GetType().String()))
	}

	filepath := v.UnsafeGetStringAt(0)

	rbat, err = stageList(proc, tableFunction, filepath)
	if err != nil {
		return false, err
	}

	result.Batch = rbat
	return false, nil
}

func stageList(proc *process.Process, tableFunction *TableFunction, filepath string) (bat *batch.Batch, err error) {

	if len(tableFunction.Attrs) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "stage_list: number of output column must be 1.")
	}

	bat = batch.NewWithSize(len(tableFunction.Attrs))
	bat.Attrs = tableFunction.Attrs
	bat.Cnt = 1
	for i := range tableFunction.ctr.retSchema {
		bat.Vecs[i] = proc.GetVector(tableFunction.ctr.retSchema[i])
	}

	rs := bat.GetVector(0)

	if len(filepath) == 0 {
		if err := vector.AppendBytes(rs, nil, true, proc.Mp()); err != nil {
			return nil, err
		}
		return bat, nil
	}

	s, err := function.UrlToStageDef(string(filepath), proc)
	if err != nil {
		return nil, err
	}

	fspath, _, err := s.ToPath()
	if err != nil {
		return nil, err
	}

	idx := strings.LastIndex(fspath, fileservice.ServiceNameSeparator)

	var service, pattern string
	if idx == -1 {
		service = ""
		pattern = fspath
	} else {
		service = fspath[:idx]
		pattern = fspath[idx+1:]
	}

	pattern = path.Clean("/" + pattern)

	fileList, err := function.StageListWithPattern(service, pattern, proc)
	if err != nil {
		return nil, err
	}

	for _, f := range fileList {
		if err := vector.AppendBytes(rs, []byte(f), false, proc.Mp()); err != nil {
			return nil, err
		}
	}

	bat.SetRowCount(len(fileList))
	return bat, nil
}
