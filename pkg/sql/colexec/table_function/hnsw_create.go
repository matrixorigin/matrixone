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
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	hnswrt "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

// hnswCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var hnswCatalogHooks = hnswrt.CatalogHooks{}

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
	sqlproc := sqlexec.NewSqlProcess(proc)

	// The TVF owns clear+rebuild (the compile layer no longer pre-deletes on the sync path).
	// tblcfg is populated in prepare (const-fold) so it is available even on a zero-row rebuild.
	// Guard anyway: without table names we cannot clear (a bare "DELETE FROM " with an empty
	// identifier is invalid), so skip rather than run malformed SQL.
	if u.tblcfg.MetadataTable == "" || u.tblcfg.IndexTable == "" {
		return nil
	}

	// Clear the old index unconditionally — even when nothing was built (REBUILD to zero docs must
	// still empty the index), so the DELETEs run before the early-out on a nil builder. Cross-CN
	// cache freshness is a per-model checksum multiset (see hnsw generation.go), so the metadata
	// timestamp plays no role in freshness — plain wall-clock is fine here.
	sqls := hnsw.ClearIndexSqls(u.tblcfg)

	ts := time.Now().UnixMicro()
	switch u.idxcfg.Usearch.Quantization {
	case usearch.F32:
		if u.buildf32 != nil {
			insertSqls, err := u.buildf32.ToInsertSql(ts)
			if err != nil {
				return err
			}
			sqls = append(sqls, insertSqls...)
		}
	case usearch.F64:
		if u.buildf64 != nil {
			insertSqls, err := u.buildf64.ToInsertSql(ts)
			if err != nil {
				return err
			}
			sqls = append(sqls, insertSqls...)
		}
	}

	for _, s := range sqls {
		res, err := hnsw_runSql(sqlproc, s)
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
	if err != nil {
		return nil, err
	}
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	// Parse the IndexTableConfig (arg 0, a constant string) up front by const-folding it, so end()
	// can clear the hidden tables and read the generation floor even on a ZERO-row rebuild — start()
	// normally parses it but never runs when the source is empty, yet a REBUILD to zero docs must
	// still clear the old index. Best-effort: on any failure st.tblcfg stays zero and end() skips
	// the clear (start() re-parses it for the non-empty path).
	if len(arg.ctr.executorsForArgs) > 0 {
		if vec, e := arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil); e == nil &&
			vec != nil && vec.IsConst() && !vec.IsConstNull() && vec.GetType().Oid == types.T_varchar {
			if cfgstr := vec.UnsafeGetStringAt(0); len(cfgstr) > 0 {
				_ = sonic.Unmarshal([]byte(cfgstr), &st.tblcfg)
			}
		}
	}

	return st, err
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *hnswCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {

		if len(tf.Params) > 0 {
			err = sonic.Unmarshal([]byte(tf.Params), &u.param)
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
		err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		// max_index_capacity: flat algo_params key (set in CREATE INDEX) wins;
		// otherwise the session variable controls it, then the hardcoded
		// default. Sourced only when the cfg didn't already carry one.
		if u.idxcfg.IndexCapacity <= 0 {
			u.idxcfg.IndexCapacity, err = indexplugin.AlgoParamInt(u.param.MaxIndexCapacity,
				proc.GetResolveVariableFunc(), "hnsw_max_index_capacity", hnswrt.DefaultMaxIndexCapacity)
			if err != nil {
				return err
			}
		}

		if u.idxcfg.IndexCapacity <= 0 {
			return moerr.NewInvalidInput(proc.Ctx, "Index Capacity must be greater than 0")
		}

		idVec := tf.ctr.argVecs[1]
		if !catalogplugin.SupportsPrimaryKeyType(hnswCatalogHooks, idVec.GetType().Oid) {
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
			u.buildf32, err = hnsw.NewHnswBuild[float32](sqlexec.NewSqlProcess(proc), uid, tf.MaxParallel, u.idxcfg, u.tblcfg)
		case usearch.F64:
			u.buildf64, err = hnsw.NewHnswBuild[float64](sqlexec.NewSqlProcess(proc), uid, tf.MaxParallel, u.idxcfg, u.tblcfg)
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
