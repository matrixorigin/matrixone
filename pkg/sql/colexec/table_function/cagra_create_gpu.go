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
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cagraPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	cagrart "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// cagraCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var cagraCatalogHooks = cagrart.CatalogHooks{}

var cagra_runSql = sqlexec.RunSql

type cagraCreateState struct {
	inited   bool
	buildf32 *cagraPkg.CagraBuild[float32]
	buildf16 *cagraPkg.CagraBuild[cuvs.Float16]
	buildi8  *cagraPkg.CagraBuild[int8]
	buildui8 *cagraPkg.CagraBuild[uint8]
	param    vectorindex.CagraParam
	tblcfg   vectorindex.IndexTableConfig
	idxcfg   vectorindex.IndexConfig
	offset   int

	// filterCols is the INCLUDE column metadata derived at start() from
	// param.IncludedColumns (names) + argVecs[3:] (types). Empty when the
	// index has no INCLUDE columns.
	filterCols []cuvsfilter.ColumnMeta

	// Small-tail CDC fallback. cuvs CAGRA build needs at least
	// intermediate_graph_degree rows per sub-index. When the source
	// has a partial trailing chunk smaller than that — or the whole
	// dataset is too small — those rows can't go through cuvs.
	// rowsSeen >= cdcCutoff routes them into cdcTail, which end() emits
	// as tag=1 CDC records under vectorindex.CdcTailId. Search-side
	// brute-force replay serves them until a future rebuild grows the
	// tail back above threshold.
	cdcCutoff int64
	rowsSeen  int64
	cdcTail   []cuvscdc.PendingRecord

	// srcEmpty short-circuits the per-row code when SELECT COUNT(*)
	// at init time returned zero — nothing to build, nothing to CDC.
	srcEmpty bool

	// holding one call batch, cagraCreateState owns it.
	batch *batch.Batch
}

func (u *cagraCreateState) end(tf *TableFunction, proc *process.Process) error {
	if u.srcEmpty {
		return nil
	}

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
		// No builder selected → init didn't set one. Nothing to do for
		// the cuvs side; the CDC tail (if any) below still emits.
	}
	if err != nil {
		return err
	}

	// Emit any buffered CDC tail records as tag=1 INSERTs under
	// vectorindex.CdcTailId. Search-side brute-force replay picks
	// them up alongside (or in place of) the cuvs sub-indexes.
	if len(u.cdcTail) > 0 {
		ibpr := includeBytesPerRowFromCols(u.filterCols)
		// colMetaJSON rides as a CdcOpHeader record at chunk_id=0,
		// record 0. Search-side can recover the INCLUDE-column layout
		// for tag=1 replay even when no tag=0 sub-index exists.
		colMetaJSON := colMetaJSONFromCols(u.filterCols)
		tailSqls, err := cuvscdc.SaveSmallTailAsCdc(
			u.tblcfg, u.cdcTail,
			int(u.idxcfg.CuvsCagra.Dimensions), ibpr, colMetaJSON)
		if err != nil {
			return err
		}
		sqls = append(sqls, tailSqls...)
		logutil.Infof("CAGRA create: emitted %d CDC tail records for `%s`.`%s` index `%s`",
			len(u.cdcTail), u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.IndexTable)
	}

	for _, s := range sqls {
		res, err := cagra_runSql(sqlexec.NewSqlProcess(proc), s)
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

func (u *cagraCreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *cagraCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	u.batch.CleanOnlyData()
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *cagraCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
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

func cagraCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &cagraCreateState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	return st, err
}

// start is called once per input row.  On the first call the index builder is initialised;
// subsequent calls append one vector to the builder.
func (u *cagraCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
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
		u.idxcfg.CuvsCagra.Quantization = uint16(qt)

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

		// Pre-count source rows; needed both for IndexCapacity auto-
		// detection (when 0) and for the small-tail CDC cutoff
		// computation below. One round trip per build.
		//
		// Snapshot safety: this COUNT(*) runs via NewSqlProcess(proc), i.e. on
		// the SAME proc/transaction as the table function's source scan that
		// streams the build rows. Under MO's per-txn snapshot isolation both
		// observe the same read timestamp, so srcRowCount equals the number of
		// rows actually streamed — the `rowsSeen >= cdcCutoff` split cannot drift
		// even under concurrent writes to the source table.
		srcRowCount, err := fetchSrcTableRowCount(proc, cagra_runSql, u.tblcfg.DbName, u.tblcfg.SrcTable)
		if err != nil {
			return err
		}
		if srcRowCount == 0 {
			// Empty source: nothing to build, nothing to CDC. Mark
			// inited so subsequent (unexpected) per-row calls
			// short-circuit cleanly via srcEmpty.
			u.inited = true
			u.srcEmpty = true
			logutil.Infof("CAGRA create: source `%s`.`%s` is empty; nothing to build",
				u.tblcfg.DbName, u.tblcfg.SrcTable)
			return nil
		}
		if u.tblcfg.IndexCapacity <= 0 {
			u.tblcfg.IndexCapacity = srcRowCount
			logutil.Infof("CAGRA create: auto-detected index capacity = %d from `%s`.`%s`",
				u.tblcfg.IndexCapacity, u.tblcfg.DbName, u.tblcfg.SrcTable)
		}

		// Compute the small-tail cutoff. The trailing partial chunk is
		// total % IndexCapacity. When IndexCapacity is auto-detected
		// (== srcRowCount) the modulo is zero and no fallback fires.
		// When the user explicitly set IndexCapacity and the trailing
		// partial is smaller than the cuvs minimum (or every chunk
		// would be too small because IndexCapacity itself is below the
		// threshold) the tail rows route to CDC instead of cuvs.
		// Threshold = the cuvs CAGRA minimum graph size for a build to
		// succeed. Mirrors cuvs.DefaultCagraBuildParams().IntermediateGraphDegree
		// (128) when the user didn't set it explicitly — same fallback
		// chain the build itself uses.
		threshold := int64(u.idxcfg.CuvsCagra.IntermediateGraphDegree)
		if threshold <= 0 {
			threshold = 128
		}
		u.cdcCutoff = srcRowCount
		if u.tblcfg.IndexCapacity < threshold {
			u.cdcCutoff = 0
			logutil.Infof("CAGRA create: IndexCapacity %d < threshold %d; all %d rows route to CDC tail",
				u.tblcfg.IndexCapacity, threshold, srcRowCount)
		} else {
			lastChunkSize := srcRowCount % u.tblcfg.IndexCapacity
			if lastChunkSize > 0 && lastChunkSize < threshold {
				u.cdcCutoff = srcRowCount - lastChunkSize
				logutil.Infof("CAGRA create: trailing %d rows < threshold %d; routing them to CDC tail (cutoff=%d, total=%d)",
					lastChunkSize, threshold, u.cdcCutoff, srcRowCount)
			}
		}

		// ---- validate argument types ----
		idVec := tf.ctr.argVecs[1]
		if !catalogplugin.SupportsPrimaryKeyType(cagraCatalogHooks, idVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "second argument (pkid) must be an int64")
		}

		faVec := tf.ctr.argVecs[2]
		if !catalogplugin.SupportsVectorType(cagraCatalogHooks, faVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "third argument (vector) must be a float32 array")
		}

		// dimension
		u.idxcfg.CuvsCagra.Dimensions = uint(faVec.GetType().Width)
		u.idxcfg.Type = vectorindex.CAGRA

		// ---- GPU devices ----
		devices, _ := cuvs.GetGpuDeviceList()

		nthread := uint32(vectorindex.GetConcurrency(u.tblcfg.ThreadsBuild))
		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)

		// ---- create builder ----
		switch qt {
		case metric.Quantization_F16:
			u.buildf16, err = cagraPkg.NewCagraBuild[cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices)
		case metric.Quantization_INT8:
			u.buildi8, err = cagraPkg.NewCagraBuild[int8](uid, u.idxcfg, u.tblcfg, nthread, devices)
		case metric.Quantization_UINT8:
			u.buildui8, err = cagraPkg.NewCagraBuild[uint8](uid, u.idxcfg, u.tblcfg, nthread, devices)
		default:
			u.buildf32, err = cagraPkg.NewCagraBuild[float32](uid, u.idxcfg, u.tblcfg, nthread, devices)
		}
		if err != nil {
			return err
		}

		// ---- pre-filter (INCLUDE columns) setup ----
		// Derive filter column metadata from the INCLUDE names stashed in
		// the params JSON paired with the types of the trailing argVecs —
		// the DDL layer emits names, the table-function layer resolves types.
		if u.filterCols, err = buildFilterColumnsFromParam(u.param.IncludedColumns, tf.ctr.argVecs, 3); err != nil {
			return err
		}
		if len(u.filterCols) > 0 {
			logutil.Infof("CAGRA create: INCLUDE columns = %v (from %d arg vectors)",
				u.filterCols, len(tf.ctr.argVecs)-3)
			if err = initFilterColumns(u.activeBuilder(), u.filterCols); err != nil {
				return err
			}
		}

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// Empty source: nothing to do.
	if u.srcEmpty {
		return nil
	}

	// ---- per-row: append one vector ----
	u.offset = 0
	u.batch.CleanOnlyData()

	// Source-stream position (counts every row delivered, including
	// rows that turn out to have a null vector — matches the
	// SELECT COUNT(*) basis cdcCutoff was derived from).
	srcPos := u.rowsSeen
	u.rowsSeen++

	faVec := tf.ctr.argVecs[2]
	if faVec.IsNull(uint64(nthRow)) {
		return nil
	}

	id := vector.GetFixedAtNoTypeCheck[int64](tf.ctr.argVecs[1], nthRow)
	fa := types.BytesToArray[float32](faVec.GetBytesAt(nthRow))

	if uint(len(fa)) != u.idxcfg.CuvsCagra.Dimensions {
		return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
	}

	// Trailing rows below the cuvs threshold route to the CDC tail
	// (search-side brute-force replay) instead of the cuvs builder.
	if srcPos >= u.cdcCutoff {
		vecCopy := append([]float32(nil), fa...)
		var incBytes []byte
		if len(u.filterCols) > 0 {
			incBytes, err = encodeIncludeRowFromArgVecs(u.filterCols, tf.ctr.argVecs, 3, nthRow)
			if err != nil {
				return err
			}
		}
		u.cdcTail = append(u.cdcTail, cuvscdc.PendingRecord{
			Pkid:    id,
			Vec:     vecCopy,
			Include: incBytes,
		})
		return nil
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
	if err != nil {
		return err
	}

	// ---- per-row: append filter column values (if any) ----
	if len(u.filterCols) > 0 {
		if err = appendFilterRow(u.activeBuilder(), u.filterCols, tf.ctr.argVecs, 3, nthRow); err != nil {
			return err
		}
	}
	return nil
}

// activeBuilder returns whichever quantization-specialised builder is live,
// exposed through the narrow filterColumnBuilder interface. Exactly one of
// the four fields is non-nil after a successful NewCagraBuild dispatch.
func (u *cagraCreateState) activeBuilder() filterColumnBuilder {
	switch {
	case u.buildf32 != nil:
		return u.buildf32
	case u.buildf16 != nil:
		return u.buildf16
	case u.buildi8 != nil:
		return u.buildi8
	case u.buildui8 != nil:
		return u.buildui8
	}
	return nil
}
