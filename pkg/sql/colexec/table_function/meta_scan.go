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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func metaScanPrepare(proc *process.Process, arg *Argument) (err error) {
	arg.ctr = new(container)
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	return err
}

func metaScanCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
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

	v, err := arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}
	uuid := vector.MustFixedCol[types.Uuid](v)[0]
	// get file size
	path := catalog.BuildQueryResultMetaPath(proc.SessionInfo.Account, uuid.ToString())
	// read meta's meta
	reader, err := blockio.NewFileReader(proc.FileService, path)
	if err != nil {
		return false, err
	}
	var idxs []uint16
	for i, name := range catalog.MetaColNames {
		for _, attr := range arg.Attrs {
			if name == attr {
				idxs = append(idxs, uint16(i))
			}
		}
	}
	// read meta's data
	bats, err := reader.LoadAllColumns(proc.Ctx, idxs, common.DefaultAllocator)
	if err != nil {
		return false, err
	}

	rbat = batch.NewWithSize(len(bats[0].Vecs))
	metaVecs := rbat.Vecs
	for i, vec := range bats[0].Vecs {
		if vec.NeedDup() {
			metaVecs[i], err = vec.Dup(proc.Mp())
			if err != nil {
				rbat.Clean(proc.Mp())
				return false, err
			}
		} else {
			metaVecs[i] = vec
		}
	}
	rbat.SetAttributes(catalog.MetaColNames)
	rbat.SetRowCount(1)
	result.Batch = rbat
	return false, nil
}
