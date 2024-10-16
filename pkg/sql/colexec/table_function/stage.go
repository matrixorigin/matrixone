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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type stagelistState struct {
	simpleOneBatchState
}

// prepare
func stageListPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return &stagelistState{}, err
}

// call
func (s *stagelistState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)

	var (
		err error
	)

	v := tf.ctr.argVecs[0]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("stage_list: filepath must be string, but got %s", v.GetType().String()))
	}

	filepath := v.UnsafeGetStringAt(0)

	err = stageList(proc, tf, filepath, s.batch)
	if err != nil {
		return err
	}

	return nil
}

func stageList(proc *process.Process, tableFunction *TableFunction, filepath string, bat *batch.Batch) (err error) {

	if len(tableFunction.Attrs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "stage_list: number of output column must be 1.")
	}

	rs := bat.GetVector(0)

	if len(filepath) == 0 {
		if err := vector.AppendBytes(rs, nil, true, proc.Mp()); err != nil {
			return err
		}
		return nil
	}

	s, err := function.UrlToStageDef(string(filepath), proc)
	if err != nil {
		return err
	}

	fspath, _, err := s.ToPath()
	if err != nil {
		return err
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
		return err
	}

	for _, f := range fileList {
		if err := vector.AppendBytes(rs, []byte(f), false, proc.Mp()); err != nil {
			return err
		}
	}

	bat.SetRowCount(len(fileList))
	return nil
}
