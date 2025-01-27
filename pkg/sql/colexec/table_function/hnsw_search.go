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
	"os"
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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

type hnswSearchState struct {
	inited    bool
	param     vectorindex.HnswParam
	tblcfg    vectorindex.IndexTableConfig
	idxcfg    vectorindex.IndexConfig
	offset    int
	limit     uint64
	keys      []int64
	distances []float32
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

// stub function
var newHnswAlgo = newHnswAlgoFn

func newHnswAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return &hnsw.HnswSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
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
		vector.AppendFixed[float32](u.batch.Vecs[1], u.distances[i], false, proc.Mp())
		n++
	}

	u.offset += n

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
	}

	/*
	   veccache.VectorIndexCacheTTL = 30 * time.Second
	   veccache.Cache.TickerInterval = 5 * time.Second
	*/
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
		u.idxcfg.Usearch.Metric = usearch.L2sq

		if len(u.param.EfConstruction) > 0 {
			val, err := strconv.Atoi(u.param.EfConstruction)
			if err != nil {
				return err
			}
			u.idxcfg.Usearch.ExpansionAdd = uint(val)
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

		// ef_search
		val, err := proc.GetResolveVariableFunc()("hnsw_ef_search", true, false)
		if err != nil {
			return err
		}
		u.idxcfg.Usearch.ExpansionSearch = uint(val.(int64))

		os.Stderr.WriteString(fmt.Sprintf("Param %v\n", u.param))
		os.Stderr.WriteString(fmt.Sprintf("Cfg %v\n", u.tblcfg))
		os.Stderr.WriteString(fmt.Sprintf("USearch Cfg %v\n", u.idxcfg))

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

	veccache.Cache.Once()

	algo := newHnswAlgo(u.idxcfg, u.tblcfg)
	u.keys, u.distances, err = veccache.Cache.Search(proc, u.tblcfg.IndexTable, algo, f32a, uint(u.limit))
	if err != nil {
		return err
	}

	os.Stderr.WriteString(fmt.Sprintf("keys %v, distances %v\n", u.keys, u.distances))

	return nil
}
