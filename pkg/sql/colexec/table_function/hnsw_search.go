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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type hnswSearchState struct {
	inited    bool
	param     vectorindex.HnswParam
	tblcfg    vectorindex.IndexTableConfig
	idxcfg    vectorindex.IndexConfig
	offset    int
	limit     uint64
	keys      []int64
	distances []float64
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

// stub function
var newHnswAlgo = newHnswAlgoFn

func newHnswAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return hnsw.NewHnswSearch(idxcfg, tblcfg)
}

func (u *hnswSearchState) end(tf *TableFunction, proc *process.Process) error {

	return nil
}

func (u *hnswSearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *hnswSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	nkeys := len(u.keys)
	n := 0

	for i := u.offset; i < nkeys && n < 8192; i++ {
		vector.AppendFixed[int64](u.batch.Vecs[0], u.keys[i], false, proc.Mp())
		vector.AppendFixed[float64](u.batch.Vecs[1], u.distances[i], false, proc.Mp())
		n++
	}

	u.offset += n

	u.batch.SetRowCount(n)

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	// write the batch
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *hnswSearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func hnswSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &hnswSearchState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	if arg.Limit != nil {
		if cExpr, ok := arg.Limit.Expr.(*plan.Expr_Lit); ok {
			if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				st.limit = c.U64Val
			}
		}
	} else {
		st.limit = uint64(1)
	}

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *hnswSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

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
		metrictype, ok := metric.OpTypeToUsearchMetric[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "Invalid op_type")
		}
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
		err := json.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		// f32vec
		f32aVec := tf.ctr.argVecs[1]
		if f32aVec.GetType().Oid != types.T_array_float32 {
			return moerr.NewInvalidInput(proc.Ctx, "Third argument (vector must be a vecfs32 type")
		}
		dimension := f32aVec.GetType().Width

		// dimension
		u.idxcfg.Usearch.Dimensions = uint(dimension)
		u.idxcfg.Type = "hnsw"

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// reset slice
	u.offset = 0
	u.keys = nil
	u.distances = nil

	// cleanup the batch
	u.batch.CleanOnlyData()

	f32aVec := tf.ctr.argVecs[1]
	if f32aVec.IsNull(uint64(nthRow)) {
		return nil
	}

	f32a := types.BytesToArray[float32](f32aVec.GetBytesAt(nthRow))
	if uint(len(f32a)) != u.idxcfg.Usearch.Dimensions {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("vector ops between different dimensions (%d, %d) is not permitted.", u.idxcfg.Usearch.Dimensions, len(f32a)))
	}

	veccache.Cache.Once()

	algo := newHnswAlgo(u.idxcfg, u.tblcfg)

	var keys any
	keys, u.distances, err = veccache.Cache.Search(proc, u.tblcfg.IndexTable, algo, f32a, vectorindex.RuntimeConfig{Limit: uint(u.limit)})
	if err != nil {
		return err
	}

	var ok bool
	u.keys, ok = keys.([]int64)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "keys is not []int64")
	}
	return nil
}
