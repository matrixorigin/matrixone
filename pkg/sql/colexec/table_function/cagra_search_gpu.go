//go:build gpu

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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cagraPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type cagraSearchState struct {
	inited    bool
	param     vectorindex.CagraParam
	tblcfg    vectorindex.IndexTableConfig
	idxcfg    vectorindex.IndexConfig
	offset    int
	limit     uint64
	keys      []int64
	distances []float64
	// holding one call batch, cagraSearchState owns it.
	batch *batch.Batch
}

// newCagraAlgo is the factory used by the search; it can be replaced in tests.
var newCagraAlgo = newCagraAlgoFn

func newCagraAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	devices, _ := cuvs.GetGpuDeviceList()
	switch metric.QuantizationType(idxcfg.CuvsCagra.Quantization) {
	case metric.Quantization_F16:
		return cagraPkg.NewCagraSearch[cuvs.Float16](idxcfg, tblcfg, devices)
	case metric.Quantization_INT8:
		return cagraPkg.NewCagraSearch[int8](idxcfg, tblcfg, devices)
	case metric.Quantization_UINT8:
		return cagraPkg.NewCagraSearch[uint8](idxcfg, tblcfg, devices)
	default: // Quantization_F32 and unknown
		return cagraPkg.NewCagraSearch[float32](idxcfg, tblcfg, devices)
	}
}

func (u *cagraSearchState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (u *cagraSearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *cagraSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
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
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *cagraSearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func cagraSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &cagraSearchState{}

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

// start is called once per query vector row.
func (u *cagraSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if !u.inited {
		// ---- parse Params ----
		if len(tf.Params) > 0 {
			if err = sonic.Unmarshal([]byte(tf.Params), &u.param); err != nil {
				return err
			}
		}

		// metric
		metricType, ok := metric.OpTypeToIvfMetric[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "invalid op_type for CAGRA")
		}
		u.idxcfg.CuvsCagra.Metric = uint16(metricType)
		u.idxcfg.OpType = u.param.OpType

		// intermediate_graph_degree
		if len(u.param.IntermediateGraphDegee) > 0 {
			val, err := strconv.ParseUint(u.param.IntermediateGraphDegee, 10, 64)
			if err != nil {
				return err
			}
			u.idxcfg.CuvsCagra.IntermediateGraphDegree = val
		}

		// graph_degree
		if len(u.param.GraphDegee) > 0 {
			val, err := strconv.ParseUint(u.param.GraphDegee, 10, 64)
			if err != nil {
				return err
			}
			u.idxcfg.CuvsCagra.GraphDegree = val
		}

		// distribution mode
		switch u.param.Distribution {
		case vectorindex.DistributionMode_REPLICATED_Str:
			u.idxcfg.CuvsCagra.DistributionMode = uint16(vectorindex.DistributionMode_REPLICATED)
		case vectorindex.DistributionMode_SHARDED_Str:
			u.idxcfg.CuvsCagra.DistributionMode = uint16(vectorindex.DistributionMode_SHARDED)
		default:
			u.idxcfg.CuvsCagra.DistributionMode = uint16(vectorindex.DistributionMode_SINGLE_GPU)
		}

		// quantization
		switch u.param.Quantization {
		case metric.Quantization_F16_Str:
			u.idxcfg.CuvsCagra.Quantization = uint16(metric.Quantization_F16)
		case metric.Quantization_INT8_Str:
			u.idxcfg.CuvsCagra.Quantization = uint16(metric.Quantization_INT8)
		case metric.Quantization_UINT8_Str:
			u.idxcfg.CuvsCagra.Quantization = uint16(metric.Quantization_UINT8)
		default:
			u.idxcfg.CuvsCagra.Quantization = uint16(metric.Quantization_F32)
		}

		// ---- IndexTableConfig ----
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "first argument (IndexTableConfig) must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig is empty")
		}
		if err = sonic.Unmarshal([]byte(cfgstr), &u.tblcfg); err != nil {
			return err
		}

		// ---- vector argument ----
		faVec := tf.ctr.argVecs[1]
		u.idxcfg.CuvsCagra.Dimensions = uint(faVec.GetType().Width)
		u.idxcfg.Type = vectorindex.CAGRA

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// ---- per-row search ----
	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.batch.CleanOnlyData()

	faVec := tf.ctr.argVecs[1]
	if faVec.IsNull(uint64(nthRow)) {
		return nil
	}

	veccache.Cache.Once()

	return runCagraSearch[float32](proc, u, faVec, nthRow)
}

func runCagraSearch[T types.RealNumbers](proc *process.Process, u *cagraSearchState, faVec *vector.Vector, nthRow int) (err error) {
	fa := types.BytesToArray[T](faVec.GetBytesAt(nthRow))
	if uint(len(fa)) != u.idxcfg.CuvsCagra.Dimensions {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("vector ops between different dimensions (%d, %d) is not permitted.", u.idxcfg.CuvsCagra.Dimensions, len(fa)))
	}

	algo := newCagraAlgo(u.idxcfg, u.tblcfg)

	rt := vectorindex.RuntimeConfig{
		Limit:        uint(u.limit),
		OrigFuncName: u.tblcfg.OrigFuncName,
	}
	var keys any
	keys, u.distances, err = veccache.Cache.Search(sqlexec.NewSqlProcess(proc), u.tblcfg.IndexTable, algo, fa, rt)
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
