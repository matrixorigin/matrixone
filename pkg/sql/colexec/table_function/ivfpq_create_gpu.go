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
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	ivfpqPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var ivfpq_runSql = sqlexec.RunSql

type ivfpqCreateState struct {
	inited   bool
	buildf32 *ivfpqPkg.IvfpqBuild[float32]
	buildf16 *ivfpqPkg.IvfpqBuild[cuvs.Float16]
	buildi8  *ivfpqPkg.IvfpqBuild[int8]
	buildui8 *ivfpqPkg.IvfpqBuild[uint8]
	param    vectorindex.IvfpqParam
	tblcfg   vectorindex.IndexTableConfig
	idxcfg   vectorindex.IndexConfig
	offset   int

	// holding one call batch, ivfpqCreateState owns it.
	batch *batch.Batch
}

func (u *ivfpqCreateState) end(tf *TableFunction, proc *process.Process) error {
	var (
		sqls []string
		err  error
	)

	ts := time.Now().UnixMicro()
	switch {
	case u.buildf32 != nil:
		sqls, err = u.buildf32.ToInsertSql(ts)
	case u.buildf16 != nil:
		sqls, err = u.buildf16.ToInsertSql(ts)
	case u.buildi8 != nil:
		sqls, err = u.buildi8.ToInsertSql(ts)
	case u.buildui8 != nil:
		sqls, err = u.buildui8.ToInsertSql(ts)
	default:
		return nil
	}
	if err != nil {
		return err
	}

	for _, s := range sqls {
		res, err := ivfpq_runSql(sqlexec.NewSqlProcess(proc), s)
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

func (u *ivfpqCreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *ivfpqCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *ivfpqCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
	if u.buildf32 != nil {
		u.buildf32.Destroy()
	}
	if u.buildf16 != nil {
		u.buildf16.Destroy()
	}
	if u.buildi8 != nil {
		u.buildi8.Destroy()
	}
	if u.buildui8 != nil {
		u.buildui8.Destroy()
	}
}

func ivfpqCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &ivfpqCreateState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	return st, err
}

// start is called once per input row.
func (u *ivfpqCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
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
			return moerr.NewInternalError(proc.Ctx, "invalid op_type for IVF-PQ")
		}
		u.idxcfg.CuvsIvfpq.Metric = uint16(metricType)
		u.idxcfg.OpType = u.param.OpType

		// lists (n_lists)
		if len(u.param.Lists) > 0 {
			val, err := strconv.ParseUint(u.param.Lists, 10, 64)
			if err != nil {
				return err
			}
			u.idxcfg.CuvsIvfpq.Lists = uint(val)
		}

		// m (sub-vectors / pq_dim)
		if len(u.param.M) > 0 {
			val, err := strconv.ParseUint(u.param.M, 10, 64)
			if err != nil {
				return err
			}
			u.idxcfg.CuvsIvfpq.M = uint(val)
		}

		// bits_per_code
		if len(u.param.BitsPerCode) > 0 {
			val, err := strconv.ParseUint(u.param.BitsPerCode, 10, 64)
			if err != nil {
				return err
			}
			u.idxcfg.CuvsIvfpq.BitsPerCode = uint(val)
		}

		// distribution mode
		switch u.param.Distribution {
		case vectorindex.DistributionMode_REPLICATED_Str:
			u.idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_REPLICATED)
		case vectorindex.DistributionMode_SHARDED_Str:
			u.idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_SHARDED)
		default:
			u.idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_SINGLE_GPU)
		}

		// quantization
		var qt metric.QuantizationType
		switch u.param.Quantization {
		case metric.Quantization_F16_Str:
			qt = metric.Quantization_F16
		case metric.Quantization_INT8_Str:
			qt = metric.Quantization_INT8
		case metric.Quantization_UINT8_Str:
			qt = metric.Quantization_UINT8
		default:
			qt = metric.Quantization_F32
		}
		u.idxcfg.CuvsIvfpq.Quantization = uint16(qt)

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
		if u.tblcfg.IndexCapacity <= 0 {
			return moerr.NewInvalidInput(proc.Ctx, "index capacity must be greater than 0")
		}

		// ---- validate argument types ----
		if len(tf.Args) < 3 || tf.Args[1].Typ.Id != int32(types.T_int64) {
			return moerr.NewInvalidInput(proc.Ctx, "second argument (pkid) must be an int64")
		}

		faVec := tf.ctr.argVecs[2]
		if faVec.GetType().Oid != types.T_array_float32 {
			return moerr.NewInvalidInput(proc.Ctx, "third argument (vector) must be a float32 array")
		}

		// dimension
		u.idxcfg.CuvsIvfpq.Dimensions = uint(faVec.GetType().Width)
		u.idxcfg.Type = vectorindex.IVFPQ

		// ---- GPU devices ----
		devices, _ := cuvs.GetGpuDeviceList()

		nthread := uint32(vectorindex.GetConcurrency(u.tblcfg.ThreadsBuild))
		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)

		// ---- create builder ----
		switch qt {
		case metric.Quantization_F16:
			u.buildf16, err = ivfpqPkg.NewIvfpqBuild[cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices)
		case metric.Quantization_INT8:
			u.buildi8, err = ivfpqPkg.NewIvfpqBuild[int8](uid, u.idxcfg, u.tblcfg, nthread, devices)
		case metric.Quantization_UINT8:
			u.buildui8, err = ivfpqPkg.NewIvfpqBuild[uint8](uid, u.idxcfg, u.tblcfg, nthread, devices)
		default:
			u.buildf32, err = ivfpqPkg.NewIvfpqBuild[float32](uid, u.idxcfg, u.tblcfg, nthread, devices)
		}
		if err != nil {
			return err
		}

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// ---- per-row: append one vector ----
	u.offset = 0
	u.batch.CleanOnlyData()

	faVec := tf.ctr.argVecs[2]
	if faVec.IsNull(uint64(nthRow)) {
		return nil
	}

	id := vector.GetFixedAtNoTypeCheck[int64](tf.ctr.argVecs[1], nthRow)
	fa := types.BytesToArray[float32](faVec.GetBytesAt(nthRow))

	if uint(len(fa)) != u.idxcfg.CuvsIvfpq.Dimensions {
		return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
	}

	switch {
	case u.buildf32 != nil:
		err = u.buildf32.AddFloat(id, fa)
	case u.buildf16 != nil:
		err = u.buildf16.AddFloat(id, fa)
	case u.buildi8 != nil:
		err = u.buildi8.AddFloat(id, fa)
	case u.buildui8 != nil:
		err = u.buildui8.AddFloat(id, fa)
	}
	return err
}
