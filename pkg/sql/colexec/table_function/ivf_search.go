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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ivfSearchState struct {
	inited    bool
	param     vectorindex.IvfParam
	tblcfg    vectorindex.IndexTableConfig
	idxcfg    vectorindex.IndexConfig
	offset    int
	limit     uint64
	keys      any
	distances []float64
	version   int
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

// stub function
var newIvfAlgo = newIvfAlgoFn

func newIvfAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return ivfflat.NewIvfflatSearch(idxcfg, tblcfg)
}

func (u *ivfSearchState) end(tf *TableFunction, proc *process.Process) error {

	return nil
}

func (u *ivfSearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *ivfSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	keys, ok := u.keys.([]any)
	if !ok {
		return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "keys is not []int64")
	}

	nkeys := len(keys)
	n := 0

	for i := u.offset; i < nkeys && n < 8192; i++ {
		vector.AppendAny(u.batch.Vecs[0], keys[i], false, proc.Mp())
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

func (u *ivfSearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func ivfSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &ivfSearchState{}

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
func (u *ivfSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {
		if len(tf.Params) > 0 {
			err = json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

		if len(u.param.Lists) > 0 {
			lists, err := strconv.Atoi(u.param.Lists)
			if err != nil {
				return err
			}
			u.idxcfg.Ivfflat.Lists = uint(lists)
		} else {
			return moerr.NewInternalError(proc.Ctx, "Invalid Lists value")
		}

		metric, ok := distTypeStrToEnum[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "invalid optype")
		}
		u.idxcfg.Ivfflat.Metric = uint16(metric)

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
		faVec := tf.ctr.argVecs[1]
		if faVec.GetType().Oid != types.T_array_float32 && faVec.GetType().Oid != types.T_array_float64 {
			return moerr.NewInvalidInput(proc.Ctx, "Third argument (vector must be a vecf32 or vecf64 type")
		}
		dimension := faVec.GetType().Width

		// dimension
		u.idxcfg.Ivfflat.Dimensions = uint(dimension)
		u.idxcfg.Type = "ivfflat"

		// get version
		version, err := ivfflat.GetVersion(proc, u.tblcfg)
		if err != nil {
			return err
		}
		u.idxcfg.Ivfflat.Version = version

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// reset slice
	u.offset = 0
	u.keys = nil
	u.distances = nil

	// cleanup the batch
	u.batch.CleanOnlyData()

	// vector cache
	veccache.Cache.Once()
	algo := newIvfAlgo(u.idxcfg, u.tblcfg)
	key := fmt.Sprintf("%s:%d", u.tblcfg.IndexTable, u.version)

	faVec := tf.ctr.argVecs[1]
	if faVec.IsNull(uint64(nthRow)) {
		return nil
	}

	if faVec.GetType().Oid == types.T_array_float32 {
		f32a := types.BytesToArray[float32](faVec.GetBytesAt(nthRow))
		u.keys, u.distances, err = veccache.Cache.Search(proc, key, algo, f32a, vectorindex.RuntimeConfig{Limit: uint(u.limit), Probe: uint(u.tblcfg.Nprobe)})
		if err != nil {
			return err
		}
	} else {
		f64a := types.BytesToArray[float64](faVec.GetBytesAt(nthRow))
		u.keys, u.distances, err = veccache.Cache.Search(proc, key, algo, f64a, vectorindex.RuntimeConfig{Limit: uint(u.limit), Probe: uint(u.tblcfg.Nprobe)})
		if err != nil {
			return err
		}
	}

	return nil
}
