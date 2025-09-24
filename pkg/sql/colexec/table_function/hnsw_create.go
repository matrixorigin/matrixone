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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

var hnsw_runSql = sqlexec.RunSql

type hnswCreateState struct {
	inited   bool
	buildf32 *hnsw.HnswBuild[float32]
	buildf64 *hnsw.HnswBuild[float64]
	param    vectorindex.HnswParam
	tblcfg   vectorindex.IndexTableConfig
	idxcfg   vectorindex.IndexConfig
	offset   int

	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func (u *hnswCreateState) end(tf *TableFunction, proc *process.Process) error {

	var (
		sqls []string
		err  error
	)

	switch u.idxcfg.Usearch.Quantization {
	case usearch.F32:
		if u.buildf32 == nil {
			return nil
		}
		sqls, err = u.buildf32.ToInsertSql(time.Now().UnixMicro())
		if err != nil {
			return err
		}
	case usearch.F64:
		if u.buildf64 == nil {
			return nil
		}
		sqls, err = u.buildf64.ToInsertSql(time.Now().UnixMicro())
		if err != nil {
			return err
		}
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

	if u.buildf32 != nil {
		u.buildf32.Destroy()
	}
	if u.buildf64 != nil {
		u.buildf64.Destroy()
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
func (u *hnswCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {

		if len(tf.Params) > 0 {
			err = json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

		if len(u.param.M) > 0 {
			val, err := strconv.Atoi(u.param.M)
			if err != nil {
				return err
			}
			u.idxcfg.Usearch.Connectivity = uint(val)
		}

		metrictype, ok := metric.OpTypeToUsearchMetric[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "Invalid op_type")
		}
		u.idxcfg.OpType = u.param.OpType
		u.idxcfg.Usearch.Metric = metrictype

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
		err = json.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		if u.tblcfg.IndexCapacity <= 0 {
			return moerr.NewInvalidInput(proc.Ctx, "Index Capacity must be greater than 0")
		}

		idVec := tf.ctr.argVecs[1]
		if idVec.GetType().Oid != types.T_int64 {
			return moerr.NewInvalidInput(proc.Ctx, "Second argument (pkid must be a bigint")
		}

		faVec := tf.ctr.argVecs[2]
		// quantization
		u.idxcfg.Usearch.Quantization, err = hnsw.QuantizationToUsearch(int32(faVec.GetType().Oid))
		if err != nil {
			return err
		}

		// dimension
		dimension := faVec.GetType().Width

		u.idxcfg.Usearch.Dimensions = uint(dimension)
		u.idxcfg.Type = vectorindex.HNSW

		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)

		switch u.idxcfg.Usearch.Quantization {
		case usearch.F32:
			u.buildf32, err = hnsw.NewHnswBuild[float32](proc, uid, tf.MaxParallel, u.idxcfg, u.tblcfg)
		case usearch.F64:
			u.buildf64, err = hnsw.NewHnswBuild[float64](proc, uid, tf.MaxParallel, u.idxcfg, u.tblcfg)
		}
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

	faVec := tf.ctr.argVecs[2]
	if faVec.IsNull(uint64(nthRow)) {
		return nil
	}

	switch u.idxcfg.Usearch.Quantization {
	case usearch.F32:
		f32a := types.BytesToArray[float32](faVec.GetBytesAt(nthRow))

		if uint(len(f32a)) != u.idxcfg.Usearch.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}

		err = u.buildf32.Add(id, f32a)
		if err != nil {
			return err
		}
		return nil
	case usearch.F64:
		f64a := types.BytesToArray[float64](faVec.GetBytesAt(nthRow))

		if uint(len(f64a)) != u.idxcfg.Usearch.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}

		err = u.buildf64.Add(id, f64a)
		if err != nil {
			return err
		}
		return nil
	default:
		// should not go here
		panic("invalid quantization")
	}
}
