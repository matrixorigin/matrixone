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
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	ivfpqPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq"
	ivfpqrt "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ivfpqCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var ivfpqCatalogHooks = ivfpqrt.CatalogHooks{}

var ivfpq_runSql = sqlexec.RunSql

// f16ToCuvs reinterprets a []types.Float16 as []cuvs.Float16. Both are uint16
// with identical layout; this is a zero-copy view (the caller does not retain it
// past the GPU add, which copies to device). Shared by the ivfpq/cagra GPU
// table functions for the native f16 (half) path.
func f16ToCuvs(s []types.Float16) []cuvs.Float16 {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*cuvs.Float16)(unsafe.Pointer(&s[0])), len(s))
}

// ivfpqBuilder is the (B, Q)-erased build interface the create state drives.
// *ivfpqPkg.IvfpqBuild[B, Q] satisfies it for every wired (base, storage)
// combo. GetIndexes is [B,Q]-typed and intentionally NOT on the interface —
// end() routes through ToInsertSql instead.
type ivfpqBuilder interface {
	// AddRow takes the raw base-type bytes of one vector (4*dim for an f32 base,
	// 2*dim for an f16 base); the concrete builder reinterprets them to its
	// []B/[]Q with UnsafeSliceCast (the interface can't name B). Passing []byte
	// rather than `any` keeps the per-row build hot path allocation-free.
	AddRow(id int64, vecBytes []byte) error
	SetFilterColumns(colMetaJSON string)
	AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error
	ToInsertSql(ts int64) ([]string, error)
	Destroy() error
}

type ivfpqCreateState struct {
	inited  bool
	builder ivfpqBuilder
	param   vectorindex.IvfpqParam
	tblcfg  vectorindex.IndexTableConfig
	idxcfg  vectorindex.IndexConfig
	offset  int

	// baseOid is the base (source) vector column element type — f32 or f16.
	// The storage/quantization type (which builder is non-nil) may differ:
	// f16 base is stored as half (direct) or quantized to int8/uint8.
	baseOid types.T

	// filterCols is the INCLUDE column metadata derived at start() from
	// param.IncludedColumns (names) + argVecs[3:] (types). Empty when the
	// index has no INCLUDE columns.
	filterCols []cuvsfilter.ColumnMeta

	// Small-tail CDC fallback. cuvs IVF-PQ k-means needs at least
	// `lists` rows per sub-index. When the source has a partial
	// trailing chunk smaller than that — or the whole dataset is too
	// small — those rows can't go through cuvs. rowsSeen >= cdcCutoff
	// routes them into cdcTail, which end() emits as tag=1 CDC
	// records under vectorindex.CdcTailId.
	cdcCutoff int64
	rowsSeen  int64
	// CDC tail records, with each vector stored as raw native base-type bytes
	// (f16 stays 2-byte — no f32 widening). vecBytesPerRow = dim * base elem size.
	cdcTail []cuvscdc.PendingRecord

	// srcEmpty short-circuits the per-row code when SELECT COUNT(*)
	// at init time returned zero — nothing to build, nothing to CDC.
	srcEmpty bool

	// holding one call batch, ivfpqCreateState owns it.
	batch *batch.Batch
}

func (u *ivfpqCreateState) end(tf *TableFunction, proc *process.Process) error {
	if u.srcEmpty {
		return nil
	}

	var (
		sqls []string
		err  error
	)

	ts := time.Now().UnixMicro()
	if u.builder != nil {
		sqls, err = u.builder.ToInsertSql(ts)
	}
	// No builder selected → init didn't set one. Nothing to do for the cuvs
	// side; the CDC tail (if any) below still emits.
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
		// vecBytesPerRow = dim * base element size (2 for vecf16, else 4).
		elemSize := 4
		if u.baseOid == types.T_array_float16 {
			elemSize = 2
		}
		vecBytesPerRow := int(u.idxcfg.CuvsIvfpq.Dimensions) * elemSize
		tailSqls, err := cuvscdc.SaveSmallTailAsCdc(
			u.tblcfg, u.cdcTail, vecBytesPerRow, ibpr, colMetaJSON)
		if err != nil {
			return err
		}
		sqls = append(sqls, tailSqls...)
		logutil.Infof("IVFPQ create: emitted %d CDC tail records for `%s`.`%s` index `%s`",
			len(u.cdcTail), u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.IndexTable)
	}

	totalBytes := 0
	for _, s := range sqls {
		totalBytes += len(s)
	}
	logutil.Infof("IVFPQ create: executing %d SQLs (total %d bytes) for `%s`.`%s`",
		len(sqls), totalBytes, u.tblcfg.DbName, u.tblcfg.IndexTable)
	for i, s := range sqls {
		logutil.Infof("IVFPQ create: SQL %d/%d start (%d bytes)", i+1, len(sqls), len(s))
		t0 := time.Now()
		res, err := ivfpq_runSql(sqlexec.NewSqlProcess(proc), s)
		if err != nil {
			logutil.Errorf("IVFPQ create: SQL %d/%d FAILED after %v: %v", i+1, len(sqls), time.Since(t0), err)
			return err
		}
		logutil.Infof("IVFPQ create: SQL %d/%d done in %v", i+1, len(sqls), time.Since(t0))
		res.Close()
	}
	logutil.Infof("IVFPQ create: all %d SQLs committed for `%s`.`%s`",
		len(sqls), u.tblcfg.DbName, u.tblcfg.IndexTable)
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
	if u.builder != nil {
		u.builder.Destroy()
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

		// max_index_capacity: flat algo_params key (set in CREATE INDEX) wins;
		// otherwise the session variable controls it, then the hardcoded
		// default. (Still 0 → auto-detect from srcRowCount below.)
		if u.idxcfg.IndexCapacity <= 0 {
			u.idxcfg.IndexCapacity, err = indexplugin.AlgoParamInt(u.param.MaxIndexCapacity,
				proc.GetResolveVariableFunc(), "ivfpq_max_index_capacity", ivfpqrt.DefaultMaxIndexCapacity)
			if err != nil {
				return err
			}
		}

		// Pre-count source rows; needed both for IndexCapacity auto-
		// detection (when 0) and for the small-tail CDC cutoff
		// computation below. One round trip per build.
		//
		// Snapshot safety: this COUNT runs via NewSqlProcess(proc), i.e. on
		// the SAME proc/transaction as the table function's source scan that
		// streams the build rows. Under MO's per-txn snapshot isolation both
		// observe the same read timestamp. It counts only indexable (vec IS NOT
		// NULL) rows, matching the build cursor (which advances only on non-NULL
		// rows), so srcRowCount equals the indexable rows actually streamed — the
		// `rowsSeen >= cdcCutoff` split cannot drift even under concurrent writes.
		srcRowCount, err := fetchSrcTableRowCount(proc, ivfpq_runSql, u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.KeyPart)
		if err != nil {
			return err
		}
		if srcRowCount == 0 {
			// Empty source: nothing to build, nothing to CDC. Mark
			// inited so subsequent (unexpected) per-row calls
			// short-circuit cleanly via srcEmpty.
			u.inited = true
			u.srcEmpty = true
			logutil.Infof("IVFPQ create: source `%s`.`%s` is empty; nothing to build",
				u.tblcfg.DbName, u.tblcfg.SrcTable)
			return nil
		}
		// Capacity is resolved further down, once the dimension, storage type and device
		// are known — sizing it against VRAM needs all three.
		requestedCapacity := u.idxcfg.IndexCapacity

		// kmeans training fraction (0-100 percent → 0-1 fraction). Flat
		// algo_params key (set in CREATE INDEX) wins; otherwise the session
		// variable controls it, then the hardcoded default.
		trainPct, err := indexplugin.AlgoParamFloat(u.param.KmeansTrainPercent,
			proc.GetResolveVariableFunc(), "kmeans_train_percent", ivfpqrt.DefaultKmeansTrainPercent)
		if err != nil {
			return err
		}
		if trainPct > 0 {
			u.idxcfg.CuvsIvfpq.KmeansTrainsetFraction = trainPct / 100.0
		}

		// quantizer training-sample limit (rows) for int8/uint8 storage: the prefix of
		// the arrival stream staged to derive the scale+offset. Flat algo_params key set
		// in CREATE INDEX; 0 => C++ default (kDefaultQuantizerTrainLimit = 100000).
		qLimit, err := indexplugin.AlgoParamInt(u.param.QuantizerTrainLimit,
			proc.GetResolveVariableFunc(), "quantizer_train_limit", 0)
		if err != nil {
			return err
		}
		if qLimit > 0 {
			u.idxcfg.CuvsIvfpq.QuantizerTrainLimit = uint64(qLimit)
		}

		// ---- validate argument types ----
		if len(tf.Args) < 3 || !catalogplugin.SupportsPrimaryKeyType(ivfpqCatalogHooks, types.T(tf.Args[1].Typ.Id)) {
			return moerr.NewInvalidInput(proc.Ctx, "second argument (pkid) must be an int64")
		}

		faVec := tf.ctr.argVecs[2]
		if !catalogplugin.SupportsVectorType(ivfpqCatalogHooks, faVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "third argument (vector) must be a float32 / float16 array")
		}
		u.baseOid = faVec.GetType().Oid

		// Derive the storage qtype from the base column type when no QUANTIZATION
		// was given: a vecf16 base with no quantization is stored natively as half.
		// (vecf16 + QUANTIZATION=int8/uint8 keeps qt = int8/uint8 — quantize path.)
		if u.baseOid == types.T_array_float16 && qt == metric.Quantization_F32 {
			qt = metric.Quantization_F16
			u.idxcfg.CuvsIvfpq.Quantization = uint16(qt)
		}

		// dimension
		u.idxcfg.CuvsIvfpq.Dimensions = uint(faVec.GetType().Width)
		u.idxcfg.Type = vectorindex.IVFPQ

		// ---- GPU devices ----
		devices, _ := cuvs.GetGpuDeviceList()
		// test-only: present N logical GPUs (all on device 0) so SHARDED / REPLICATED
		// modes can be built on a single-GPU host. No-op when gpu_multi_simulation < 2.
		devices = vectorindex.SimulateDevices(devices, u.tblcfg.GpuMultiSimulation)

		// ---- capacity, bounded by what the GPU can actually hold ----
		// Every build is bounded, not just the default one: an explicit
		// max_index_capacity is a request, and honouring a request larger than the
		// device can take would reintroduce the OOM this bound exists to prevent.
		//
		// The build dataset no longer costs device memory: build_internal hands cuVS a
		// raft::host_matrix_view, which gathers the k-means trainset on the host and
		// streams the encode in batches. So capacity is bounded by what STAYS
		// resident -- the PQ codes plus their int64 payloads. That is also the honest
		// bound: a search reaches every list, so the whole index must be loaded and
		// splitting into sub-indexes does not shrink the total.
		//
		// Use the EFFECTIVE fraction, not the configured one. ivfpqConfig only forwards
		// the config value when it is > 0 (model_gpu.go), so `kmeans_train_percent = 0`
		// leaves cuVS on its own default of 0.5.
		trainFrac := u.idxcfg.CuvsIvfpq.KmeansTrainsetFraction
		if trainFrac <= 0 {
			trainFrac = cuvs.DefaultIvfPqBuildParams().KmeansTrainsetFraction
		}
		dim := uint64(u.idxcfg.CuvsIvfpq.Dimensions)
		perRow := pqCodeBytes(dim, uint64(u.idxcfg.CuvsIvfpq.M),
			uint64(u.idxcfg.CuvsIvfpq.BitsPerCode))

		// The trainset is budgeted separately because it never coexists with the
		// index: its block closes at ivf_pq_build.cuh:1369, before detail::extend at
		// :1374 allocates the list data. Peak is max(train, index), so folding the
		// trainset into perRow would shrink capacity to buy something that is not
		// competing for the same bytes.
		var rowsFit, maxTrainRows int64
		if len(devices) > 0 {
			var freeBytes uint64
			rowsFit, freeBytes, err = cuvs.RowsFittingFreeMem(devices[0], perRow)
			if err != nil {
				// Never guess. Assuming the whole table fits is precisely the failure
				// being prevented, so say which lever the operator has instead.
				return moerr.NewInternalErrorf(proc.Ctx,
					"ivfpq: cannot size the index build against GPU memory (%v); "+
						"set max_index_capacity explicitly", err)
			}
			logutil.Infof("IVFPQ create: %d MB free on device %d, %d B/row resident -> %d rows fit",
				freeBytes>>20, devices[0], perRow, rowsFit)

			maxTrainRows, _, err = cuvs.RowsFittingFreeMem(devices[0], dim*trainsetBytesPerElem(qt))
			if err != nil {
				return moerr.NewInternalErrorf(proc.Ctx,
					"ivfpq: cannot size the k-means trainset against GPU memory (%v); "+
						"set kmeans_train_percent explicitly", err)
			}
		}

		// Capacity is a host allocation before it is a device one: InitEmpty resizes
		// flattened_host_dataset to capacity*dim up front. Sizing against the PQ codes
		// makes capacity ~7.7x larger than the old dataset-based bound did, so the host
		// side now needs its own limit -- otherwise a 20 GB card derives 63M rows and
		// asks the host for 97 GB.
		hostRowsFit, availBytes, herr := hostRowsFittingMem(dim * quantizationBytes(qt))
		if herr != nil {
			// Non-fatal: the device bound and the srcRowCount clamp still apply.
			logutil.Warnf("IVFPQ create: cannot read host memory (%v); "+
				"capacity bounded by GPU memory only", herr)
		} else if hostRowsFit > 0 {
			logutil.Infof("IVFPQ create: %d MB host available, %d B/row host -> %d rows fit",
				availBytes>>20, dim*quantizationBytes(qt), hostRowsFit)
		}

		// lists defaults to the cuVS default when unset; a 0 threshold would silently
		// disable the k-means minimum check below.
		threshold := int64(u.idxcfg.CuvsIvfpq.Lists)
		if threshold <= 0 {
			threshold = int64(cuvs.DefaultIvfPqBuildParams().NLists)
		}

		// SHARDED distributes ONE sub-index across N devices; see the matching
		// note in cagra_create_gpu.go — planCapacity's split guard needs the
		// aggregate rowsFit for SHARDED or it rejects legitimate N-way shards.
		effectiveRowsFit := rowsFit
		if u.idxcfg.CuvsIvfpq.DistributionMode == uint16(vectorindex.DistributionMode_SHARDED) && len(devices) > 1 {
			effectiveRowsFit = rowsFit * int64(len(devices))
		}
		plan, err := planCapacity(srcRowCount, requestedCapacity, effectiveRowsFit, hostRowsFit, threshold,
			u.idxcfg.CuvsIvfpq.DistributionMode == uint16(vectorindex.DistributionMode_SHARDED),
			"ivfpq", "max_index_capacity")
		if err != nil {
			return err
		}
		u.idxcfg.IndexCapacity = plan.Capacity
		u.cdcCutoff = plan.CdcCutoff
		if plan.NumSubIdx > 1 || plan.VRAMBound || plan.HostBound {
			logutil.Infof("IVFPQ create: capacity=%d (requested=%d, vram_bound=%v, host_bound=%v) -> %d sub-index(es) for %d rows; cdc_cutoff=%d",
				plan.Capacity, requestedCapacity, plan.VRAMBound, plan.HostBound, plan.NumSubIdx, srcRowCount, plan.CdcCutoff)
		}

		// Resolve the training sample against the capacity just chosen, and record the
		// EFFECTIVE fraction rather than the requested one. Without this the clamp is
		// invisible: a request for 20% that the device can only honour at 3.7% still
		// builds, still succeeds, and only shows up as recall nobody can explain.
		tp := planTrainFraction(plan.Capacity, maxTrainRows, threshold, trainFrac)
		if tp.Rows > 0 {
			u.idxcfg.CuvsIvfpq.KmeansTrainsetFraction = tp.Fraction
			if tp.Clamped {
				logutil.Infof("IVFPQ create: kmeans trainset %.4f -> %.4f (%d rows of %d); "+
					"GPU memory allows %d training rows",
					trainFrac, tp.Fraction, tp.Rows, plan.Capacity, maxTrainRows)
			}
			if tp.Thin {
				// Warn, do not refuse. The floor is a rule of thumb, not a cuVS
				// constraint (validate_build_params only checks rows >= n_lists), and
				// rejecting a build on a heuristic would break configurations that
				// work today. Name both levers so the choice stays with the operator.
				logutil.Warnf("IVFPQ create: %d training rows for %d lists is %.0f points "+
					"per centroid, under the ~%d k-means wants; recall may suffer. "+
					"Lower lists, or raise kmeans_train_percent / max_index_capacity.",
					tp.Rows, threshold, float64(tp.Rows)/float64(threshold), kmeansPointsPerCentroid)
			}
		}

		nthread := uint32(vectorindex.GetConcurrency(u.tblcfg.ThreadsBuild))
		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)

		// Packed sub-index tars go to the LOCAL fileservice's scratch dir rather
		// than /tmp: each tar is a whole sub-index, so a large build writes GB
		// through it, and LOCAL is the provisioned data volume. "" when no LOCAL
		// fileservice is attached, which os.MkdirTemp reads as $TMPDIR.
		spillDir := vectorindex.LocalSpillDir(proc.Ctx, proc.Base.FileService)
		if spillDir == "" {
			logutil.Infof("IVFPQ create: no LOCAL fileservice; index tars will use $TMPDIR")
		}

		// ---- create builder ----
		// One real [B, Q] builder keyed on (base column type, storage qtype).
		// The 7 wired combos: f32 base × {f32, f16, int8, uint8}; f16 base ×
		// {f16, int8, uint8}.
		isF16Base := u.baseOid == types.T_array_float16
		switch {
		case isF16Base && qt == metric.Quantization_F16:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[cuvs.Float16, cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case isF16Base && qt == metric.Quantization_INT8:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[cuvs.Float16, int8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case isF16Base && qt == metric.Quantization_UINT8:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[cuvs.Float16, uint8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_F16:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[float32, cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_INT8:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[float32, int8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_UINT8:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[float32, uint8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		default:
			u.builder, err = ivfpqPkg.NewIvfpqBuild[float32, float32](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		}
		if err != nil {
			return err
		}

		// ---- pre-filter (INCLUDE columns) setup ----
		// Derive filter column metadata from the INCLUDE names stashed in
		// the params JSON paired with the types of the trailing argVecs.
		if u.filterCols, err = buildFilterColumnsFromParam(u.param.IncludedColumns, tf.ctr.argVecs, 3); err != nil {
			return err
		}
		if len(u.filterCols) > 0 {
			logutil.Infof("IVFPQ create: INCLUDE columns = %v (from %d arg vectors)",
				u.filterCols, len(tf.ctr.argVecs)-3)
			if err = initFilterColumns(u.builder, u.filterCols); err != nil {
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

	faVec := tf.ctr.argVecs[2]
	if faVec.IsNull(uint64(nthRow)) {
		// NULL vector: not indexed and does NOT advance the build cursor, so the
		// cuVS chunk / small-tail cutoff is computed over non-NULL rows only
		// (matching the COUNT(... WHERE vec IS NOT NULL) basis of cdcCutoff).
		return nil
	}

	// Build-stream position over indexable (non-NULL) rows only — matches the
	// COUNT(... WHERE vec IS NOT NULL) basis that cdcCutoff was derived from.
	srcPos := u.rowsSeen
	u.rowsSeen++

	id := vector.GetFixedAtNoTypeCheck[int64](tf.ctr.argVecs[1], nthRow)

	// Decode the base vector to its native type. f32 base -> []float32 (used by
	// the f32 path and the CDC tail). f16 base -> native []cuvs.Float16 for the
	// direct (half-storage) add; the CDC tail still transports f32 (exact widen)
	// until the CDC pipeline is made native (step 5).
	var fa []float32
	var hf []cuvs.Float16
	if u.baseOid == types.T_array_float16 {
		h := types.BytesToArray[types.Float16](faVec.GetBytesAt(nthRow))
		if uint(len(h)) != u.idxcfg.CuvsIvfpq.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
		hf = f16ToCuvs(h)
		if srcPos >= u.cdcCutoff {
			fa = types.Float16ToFloat32Slice(h)
		}
	} else {
		fa = types.BytesToArray[float32](faVec.GetBytesAt(nthRow))
		if uint(len(fa)) != u.idxcfg.CuvsIvfpq.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
	}

	// Trailing rows below the cuvs k-means threshold (lists) route to
	// the CDC tail (search-side brute-force replay) instead of the
	// cuvs builder.
	if srcPos >= u.cdcCutoff {
		var incBytes []byte
		if len(u.filterCols) > 0 {
			incBytes, err = encodeIncludeRowFromArgVecs(u.filterCols, tf.ctr.argVecs, 3, nthRow)
			if err != nil {
				return err
			}
		}
		// Buffer the tail row as raw native base-type bytes so a vecf16 base is
		// stored as half (2 bytes/elem) in the CDC record — no f32 detour.
		var vecBytes []byte
		if u.baseOid == types.T_array_float16 {
			vecBytes = append([]byte(nil), util.UnsafeSliceToBytes(hf)...)
		} else {
			vecBytes = append([]byte(nil), util.UnsafeSliceToBytes(fa)...)
		}
		u.cdcTail = append(u.cdcTail, cuvscdc.PendingRecord{
			Pkid:    id,
			Vec:     vecBytes,
			Include: incBytes,
		})
		return nil
	}

	// Pass the vector as raw base-type bytes (f32 base -> fa, f16 base -> hf),
	// reinterpreted with UnsafeSliceToBytes (zero-copy); the concrete
	// IvfpqBuild[B,Q] casts them back to its own []B/[]Q. No per-row alloc.
	vecBytes := util.UnsafeSliceToBytes(fa)
	if u.baseOid == types.T_array_float16 {
		vecBytes = util.UnsafeSliceToBytes(hf)
	}
	if err = u.builder.AddRow(id, vecBytes); err != nil {
		return err
	}

	if len(u.filterCols) > 0 {
		if err = appendFilterRow(u.builder, u.filterCols, tf.ctr.argVecs, 3, nthRow); err != nil {
			return err
		}
	}
	return nil
}
