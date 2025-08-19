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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var hnsw_runSql = sqlexec.RunSql

type hnswCreateState struct {
	inited bool
	build  *hnsw.HnswBuild
	tblcfg vectorindex.IndexTableCfg
	idxcfg vectorindex.IndexConfig
	offset int

	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func (u *hnswCreateState) end(tf *TableFunction, proc *process.Process) error {
	if u.build == nil {
		return nil
	}

	sqls, err := u.build.ToInsertSql(time.Now().UnixMicro())
	if err != nil {
		return err
	}

	for _, s := range sqls {
		res, err := hnsw_runSql(proc, s)
		if err != nil {
			return err
		}
		res.Close()
	}

	return nil
}

func (u *hnswCreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *hnswCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	// write the batch
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *hnswCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}

	if u.build != nil {
		u.build.Destroy()
	}
}

func hnswCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &hnswCreateState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *hnswCreateState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	analyzer process.Analyzer,
) (err error) {

	if !u.inited {
		if err = InitHNSWCfgFromParam(tf.Params, &u.idxcfg); err != nil {
			return
		}

		// IndexTableConfig
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			err = moerr.NewInvalidInput(proc.Ctx, "First argument (IndexTableConfig must be a string")
			return
		}
		if !cfgVec.IsConst() {
			err = moerr.NewInternalError(proc.Ctx, "IndexTableConfig must be a String constant")
			return
		}
		cfgstr := cfgVec.GetStringAt(0)
		if len(cfgstr) == 0 {
			err = moerr.NewInternalError(proc.Ctx, "IndexTableConfig is empty")
			return
		}
		if u.tblcfg, err = vectorindex.TryeConvertIndexTableCfgV1(cfgstr); err != nil {
			return
		}

		if u.tblcfg.IndexCapacity() <= int64(0) {
			err = moerr.NewInvalidInput(proc.Ctx, "Index Capacity must be greater than 0")
			return
		}

		idVec := tf.ctr.argVecs[1]
		if idVec.GetType().Oid != types.T_int64 {
			err = moerr.NewInvalidInput(proc.Ctx, "Second argument (pkid must be a bigint")
			return
		}

		f32aVec := tf.ctr.argVecs[2]
		if f32aVec.GetType().Oid != types.T_array_float32 {
			err = moerr.NewInvalidInput(proc.Ctx, "Third argument (vector must be a vecfs32 type")
			return
		}
		dimension := f32aVec.GetType().Width
		// dimension
		u.idxcfg.Usearch.Dimensions = uint(dimension)

		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)
		if u.build, err = hnsw.NewHnswBuild(
			proc, uid, tf.MaxParallel, u.idxcfg, u.tblcfg,
		); err != nil {
			return
		}

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// reset slice
	u.offset = 0

	// cleanup the batch
	u.batch.CleanOnlyData()

	idVec := tf.ctr.argVecs[1]
	id := vector.GetFixedAtNoTypeCheck[int64](idVec, nthRow)

	f32aVec := tf.ctr.argVecs[2]
	if f32aVec.IsNull(uint64(nthRow)) {
		return nil
	}

	f32a := types.BytesToArray[float32](f32aVec.GetBytesAt(nthRow))

	if uint(len(f32a)) != u.idxcfg.Usearch.Dimensions {
		return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
	}

	err = u.build.Add(id, f32a)
	if err != nil {
		return err
	}
	return nil
}
