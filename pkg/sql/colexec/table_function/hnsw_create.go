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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	usearch "github.com/unum-cloud/usearch/golang"
)

type hnswCreateState struct {
	inited bool
	build  *hnsw.HnswBuild
	param  vectorindex.HnswParam
	tblcfg vectorindex.IndexTableConfig
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

	/*
		val, err := proc.GetResolveVariableFunc()("experimental_hnsw_index", true, false)
		if err != nil {
			return nil, err
		}
		//os.Stderr.WriteString(fmt.Sprintf("Prepare ef_search %d\n", val.(int64)))
		os.Stderr.WriteString(fmt.Sprintf("Prepare ef_search %v\n", val))
	*/

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *hnswCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {
		if len(tf.Params) > 0 {
			err = json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

		if len(u.param.Quantization) > 0 {
			var ok bool
			u.idxcfg.Usearch.Quantization, ok = vectorindex.QuantizationValid(u.param.Quantization)
			if !ok {
				return moerr.NewInternalError(proc.Ctx, "Invalid quantization value")
			}
		}

		if len(u.param.M) > 0 {
			val, err := strconv.Atoi(u.param.M)
			if err != nil {
				return err
			}
			u.idxcfg.Usearch.Connectivity = uint(val)
		}

		// default L2Sq
		if u.param.OpType != "vector_l2_ops" {
			return moerr.NewInternalError(proc.Ctx, "invalid optype")
		}

		u.idxcfg.Usearch.Metric = usearch.L2sq

		if len(u.param.EfConstruction) > 0 {
			val, err := strconv.Atoi(u.param.EfConstruction)
			if err != nil {
				return err
			}
			u.idxcfg.Usearch.ExpansionAdd = uint(val)
		}

		// ef_search
		if len(u.param.EfSearch) > 0 {
			val, err := strconv.Atoi(u.param.EfSearch)
			if err != nil {
				return err
			}
			u.idxcfg.Usearch.ExpansionSearch = uint(val)
		}

		// IndexTableConfig
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "First argument (IndexTableConfig must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig must be a String constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig is empty")
		}
		err := json.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		idVec := tf.ctr.argVecs[1]
		if idVec.GetType().Oid != types.T_int64 {
			return moerr.NewInvalidInput(proc.Ctx, "Second argument (pkid must be a bigint")
		}

		f32aVec := tf.ctr.argVecs[2]
		if f32aVec.GetType().Oid != types.T_array_float32 {
			return moerr.NewInvalidInput(proc.Ctx, "Third argument (vector must be a vecfs32 type")
		}
		dimension := f32aVec.GetType().Width

		// dimension
		u.idxcfg.Usearch.Dimensions = uint(dimension)
		u.idxcfg.Type = "hnsw"

		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)
		u.build, err = hnsw.NewHnswBuild(proc, uid, tf.MaxParallel, u.idxcfg, u.tblcfg)
		if err != nil {
			return err
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

	err = u.build.Add(id, f32a)
	if err != nil {
		return err
	}
	return nil
}

var hnsw_runSql = hnsw_runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func hnsw_runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId)
	return exec.Exec(topContext, sql, opts)
}
