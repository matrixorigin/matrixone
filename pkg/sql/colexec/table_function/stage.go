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
	"github.com/matrixorigin/matrixone/pkg/stage/stageutil"
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
	return &stagelistState{}, err
}

// call
func (s *stagelistState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
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

	service, pattern, err := stageServicePattern(proc, filepath)
	if err != nil {
		return err
	}

	fileList, err := stageutil.StageListWithPattern(service, pattern, proc)
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

// stageServicePattern returns the file service and the path pattern of the stage or file URL
// filepath.
func stageServicePattern(proc *process.Process, filepath string) (service, pattern string, err error) {
	s, err := stageutil.UrlToStageDef(filepath, proc)
	if err != nil {
		return "", "", err
	}

	fspath, _, err := s.ToPath()
	if err != nil {
		return "", "", err
	}

	idx := strings.LastIndex(fspath, fileservice.ServiceNameSeparator)
	if idx == -1 {
		pattern = fspath
	} else {
		service = fspath[:idx]
		pattern = fspath[idx+1:]
	}
	return service, path.Clean("/" + pattern), nil
}

type descFileState struct {
	simpleOneBatchState
}

func descFilePrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	return &descFileState{}, err
}

func (s *descFileState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	s.startPreamble(tf, proc, nthRow)

	if len(tf.ctr.argVecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "desc_file: requires one argument, the file path")
	}
	v := tf.ctr.argVecs[0]
	row := nthRow
	if v.IsConst() {
		row = 0
	}
	if v.IsConstNull() || v.GetNulls().Contains(uint64(row)) {
		return nil
	}
	if !v.GetType().Oid.IsMySQLString() {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("desc_file: filepath must be string, but got %s", v.GetType().String()))
	}
	filepath := v.UnsafeGetStringAt(row)
	if len(filepath) == 0 {
		return nil
	}

	entries, err := descFileEntries(proc, filepath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		for i, attr := range tf.Attrs {
			vec := s.batch.Vecs[i]
			switch attr {
			case "file":
				err = vector.AppendBytes(vec, []byte(entry.Name), false, proc.Mp())
			case "size":
				err = vector.AppendFixed(vec, entry.Size, false, proc.Mp())
			case "is_dir":
				err = vector.AppendFixed(vec, entry.IsDir, false, proc.Mp())
			default:
				err = moerr.NewInvalidInputf(proc.Ctx, "%v is not supported by desc_file()", attr)
			}
			if err != nil {
				return err
			}
		}
	}
	s.batch.SetRowCount(len(entries))
	return nil
}

// descFileEntries returns the entries of the directory or of the files matching the pattern
// filepath names, or the entry of the file it names; a directory has size 0.
func descFileEntries(proc *process.Process, filepath string) ([]fileservice.DirEntry, error) {
	service, pattern, err := stageServicePattern(proc, filepath)
	if err != nil {
		return nil, err
	}
	entries, listErr := stageutil.StageListEntriesWithPattern(service, pattern, proc)
	for i := range entries {
		if entries[i].IsDir {
			entries[i].Size = 0
		}
	}
	if listErr == nil && len(entries) > 0 || strings.ContainsAny(pattern, "*?[") || strings.HasSuffix(filepath, "/") {
		return entries, listErr
	}
	entry, err := stageutil.StageStatFile(service, pattern, proc)
	if err == nil {
		return []fileservice.DirEntry{*entry}, nil
	}
	if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return nil, err
	}
	return nil, listErr
}
