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
	cagraPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	cagrart "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// cagraCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var cagraCatalogHooks = cagrart.CatalogHooks{}

var cagra_runSql = sqlexec.RunSql

// cagraBuilder is the (B, Q)-erased build interface the create state drives.
// *cagraPkg.CagraBuild[B, Q] satisfies it for every wired (base, storage)
// combo. GetIndexes is [B,Q]-typed and intentionally NOT on the interface —
// end() routes through ToInsertSql instead.
type cagraBuilder interface {
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

type cagraCreateState struct {
	inited  bool
	builder cagraBuilder
	param   vectorindex.CagraParam
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
		vecBytesPerRow := int(u.idxcfg.CuvsCagra.Dimensions) * elemSize
		tailSqls, err := cuvscdc.SaveSmallTailAsCdc(
			u.tblcfg, u.cdcTail, vecBytesPerRow, ibpr, colMetaJSON)
		if err != nil {
			return err
		}
		sqls = append(sqls, tailSqls...)
		logutil.Infof("CAGRA create: emitted %d CDC tail records for `%s`.`%s` index `%s`",
			len(u.cdcTail), u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.IndexTable)
	}

	totalBytes := 0
	for _, s := range sqls {
		totalBytes += len(s)
	}
	logutil.Infof("CAGRA create: executing %d SQLs (total %d bytes) for `%s`.`%s`",
		len(sqls), totalBytes, u.tblcfg.DbName, u.tblcfg.IndexTable)
	for i, s := range sqls {
		logutil.Infof("CAGRA create: SQL %d/%d start (%d bytes)", i+1, len(sqls), len(s))
		t0 := time.Now()
		res, err := cagra_runSql(sqlexec.NewSqlProcess(proc), s)
		if err != nil {
			logutil.Errorf("CAGRA create: SQL %d/%d FAILED after %v: %v", i+1, len(sqls), time.Since(t0), err)
			return err
		}
		logutil.Infof("CAGRA create: SQL %d/%d done in %v", i+1, len(sqls), time.Since(t0))
		res.Close()
	}
	logutil.Infof("CAGRA create: all %d SQLs committed for `%s`.`%s`",
		len(sqls), u.tblcfg.DbName, u.tblcfg.IndexTable)
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
	if u.builder != nil {
		u.builder.Destroy()
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

		// quantizer training-sample limit (rows) for int8/uint8 storage: the prefix of
		// the arrival stream staged to derive the scale+offset. Flat algo_params key set
		// in CREATE INDEX; 0 => C++ default (kDefaultQuantizerTrainLimit = 100000).
		if qLimit, err := indexplugin.AlgoParamInt(u.param.QuantizerTrainLimit,
			proc.GetResolveVariableFunc(), "quantizer_train_limit", 0); err != nil {
			return err
		} else if qLimit > 0 {
			u.idxcfg.CuvsCagra.QuantizerTrainLimit = uint64(qLimit)
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

		// max_index_capacity: flat algo_params key (set in CREATE INDEX) wins;
		// otherwise the session variable controls it, then the hardcoded
		// default. (Still 0 → auto-detect from srcRowCount below.)
		if u.idxcfg.IndexCapacity <= 0 {
			u.idxcfg.IndexCapacity, err = indexplugin.AlgoParamInt(u.param.MaxIndexCapacity,
				proc.GetResolveVariableFunc(), "cagra_max_index_capacity", cagrart.DefaultMaxIndexCapacity)
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
		srcRowCount, err := fetchSrcTableRowCount(proc, cagra_runSql, u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.KeyPart)
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
		// Capacity is resolved further down, once the dimension, storage type and device
		// are known — sizing it against VRAM needs all three.
		requestedCapacity := u.idxcfg.IndexCapacity

		// ---- validate argument types ----
		idVec := tf.ctr.argVecs[1]
		if !catalogplugin.SupportsPrimaryKeyType(cagraCatalogHooks, idVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "second argument (pkid) must be an int64")
		}

		faVec := tf.ctr.argVecs[2]
		if !catalogplugin.SupportsVectorType(cagraCatalogHooks, faVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "third argument (vector) must be a float32 / float16 array")
		}
		u.baseOid = faVec.GetType().Oid

		// Derive the storage qtype from the base column type when no QUANTIZATION
		// was given: a vecf16 base with no quantization is stored natively as half.
		// (vecf16 + QUANTIZATION=int8/uint8 keeps qt = int8/uint8 — quantize path.)
		if u.baseOid == types.T_array_float16 && qt == metric.Quantization_F32 {
			qt = metric.Quantization_F16
			u.idxcfg.CuvsCagra.Quantization = uint16(qt)
		}

		// dimension
		u.idxcfg.CuvsCagra.Dimensions = uint(faVec.GetType().Width)
		u.idxcfg.Type = vectorindex.CAGRA

		// ---- GPU devices ----
		devices, _ := cuvs.GetGpuDeviceList()
		// test-only: present N logical GPUs (all on device 0) so SHARDED / REPLICATED
		// modes can be built on a single-GPU host. No-op when gpu_multi_simulation < 2.
		devices = vectorindex.SimulateDevices(devices, u.tblcfg.GpuMultiSimulation)

		// ---- capacity, bounded by what the GPU can actually hold ----
		// As in the ivfpq twin, every build is bounded and not just the default one: an
		// explicit cagra_max_index_capacity is a request rather than an override.
		//
		// The per-row COST, though, is deliberately not the ivfpq one, and the
		// difference is not an oversight. ivfpq dropped the dataset term because it
		// hands cuVS a host view and the vectors are streamed and then discarded --
		// only the PQ codes stay. CAGRA cannot do that: it searches by walking the
		// graph and reading the actual vectors, so its dataset is resident for the
		// index's whole life, not just the build. Streaming the build would not change
		// that, which is why CAGRA still sizes against dim*sizeof(Q) here. On top of it
		// sits the intermediate kNN graph (neighbour ids plus distances).
		perRow := uint64(u.idxcfg.CuvsCagra.Dimensions) * quantizationBytes(qt)
		graphDegree := uint64(u.idxcfg.CuvsCagra.IntermediateGraphDegree)
		if graphDegree == 0 {
			graphDegree = 128
		}
		perRow += graphDegree * 8

		// Size against the SMALLEST participating card, not devices[0].
		// Heterogeneous free VRAM is supported, and SHARDED cuts equal shards:
		// sampling only devices[0] on a 40 GiB + 8 GiB pair sizes every shard for
		// the 40 GiB card and the 8 GiB one OOMs the moment its shard lands.
		// Iterate DISTINCT physical devices — under gpu_multi_simulation the list
		// aliases one card N times, and querying it N times just returns the same
		// number N times while pretending to have surveyed N cards.
		rowsFit, minDev, minFree, derr := vimemory.DeviceMinRowsFitting(devices, perRow, cuvs.RowsFittingFreeMem)
		if derr != nil {
			return moerr.NewInternalErrorf(proc.Ctx,
				"cagra: %v; set cagra_max_index_capacity explicitly", derr)
		}
		if rowsFit > 0 {
			logutil.Infof("CAGRA create: smallest participating device %d has %d MB free, %d B/row -> %d rows fit",
				minDev, minFree>>20, perRow, rowsFit)
		}

		// INCLUDE column metadata is resolved HERE — before memory.HostRowsFitting —
		// so its per-row bytes can be added to the host cost model. FilterStore::init
		// eagerly resizes each INCLUDE column to `capacity * elem_size` up front, so a
		// narrow vector with several fixed-width INCLUDE columns can blow the 60%
		// budget when only the vector width is charged. filterCols is stashed on the
		// state so the later filter setup does not rebuild it.
		if u.filterCols, err = buildFilterColumnsFromParam(u.param.IncludedColumns, tf.ctr.argVecs, 3); err != nil {
			return err
		}
		// vimemory.HostIDBytesPerRow covers host_ids + id_to_index_, which the C++ side reserves
		// and populates for every row regardless of how narrow the vector is.
		hostPerRow := uint64(u.idxcfg.CuvsCagra.Dimensions)*quantizationBytes(qt) +
			uint64(includeBytesPerRowFromCols(u.filterCols)) + vimemory.HostIDBytesPerRow

		// CAGRA's per-row cost still includes the dataset, so its VRAM bound already
		// keeps the host buffer at or under the device budget. The host bound is applied
		// anyway: it costs one syscall, and it stops the two algorithms from drifting
		// apart the next time one of their cost models changes.
		hostRowsFit, availBytes, herr := vimemory.HostRowsFitting(hostPerRow)
		if herr != nil {
			// memory.HostRowsFitting errors ONLY on a successful measurement that
			// cannot hold one row — which now includes a cgroup sitting at its
			// limit (avail==0). An unavailable measurement returns (0,0,nil) and
			// falls through to the GPU-only bound below, so there is no longer an
			// availBytes>0 proxy to test: previously a full cgroup reported 0 and
			// was misread as "unmeasured", disabling the bound it should enforce.
			return moerr.NewInternalErrorf(proc.Ctx, "cagra: %v", herr)
		}
		if hostRowsFit > 0 {
			logutil.Infof("CAGRA create: %d MB host available, %d B/row host (%d vector + %d include + %d ids) -> %d rows fit",
				availBytes>>20, hostPerRow,
				uint64(u.idxcfg.CuvsCagra.Dimensions)*quantizationBytes(qt),
				includeBytesPerRowFromCols(u.filterCols),
				vimemory.HostIDBytesPerRow,
				hostRowsFit)
		} else {
			logutil.Warnf("CAGRA create: host memory unavailable; capacity bounded by GPU memory only")
		}

		// cuVS validate_build_params rejects `n_rows <= intermediate_graph_degree`,
		// not just `<`. If a sub-index ends up with EXACTLY graphDegree rows the
		// build throws "number of vectors per shard must be > intermediate_graph_degree".
		// The threshold passed to planCapacity is what its `tail < threshold` /
		// `capacity < threshold` guards compare against, so bump it by 1 to
		// route any `srcRowCount % capacity == graphDegree` tail to the CDC path.
		threshold := int64(graphDegree) + 1

		// SHARDED distributes ONE sub-index across N devices; the aggregate VRAM
		// budget is N × per-device. planCapacity treats `capacity` as "per-sub-
		// index build rows" and its SHARDED-can't-split guard needs to see the
		// aggregate — otherwise a comfortable N-way shard (each shard fits on
		// its device) gets rejected as if it had to fit on one card. Scale by
		// the DISTINCT physical device count: under gpu_multi_simulation devices
		// may be [0,0,...] (all sim GPUs aliased to physical device 0) and
		// scaling by len(devices) would over-commit that single card by N×.
		effectiveRowsFit := rowsFit
		if u.idxcfg.CuvsCagra.DistributionMode == uint16(vectorindex.DistributionMode_SHARDED) {
			if physN := distinctDeviceCount(devices); physN > 1 {
				effectiveRowsFit = rowsFit * int64(physN)
			}
		}
		plan, err := planCapacity(srcRowCount, requestedCapacity, effectiveRowsFit, hostRowsFit, threshold,
			u.idxcfg.CuvsCagra.DistributionMode == uint16(vectorindex.DistributionMode_SHARDED),
			"cagra", "max_index_capacity")
		if err != nil {
			return err
		}
		u.idxcfg.IndexCapacity = plan.Capacity
		u.cdcCutoff = plan.CdcCutoff
		if plan.NumSubIdx > 1 || plan.VRAMBound {
			logutil.Infof("CAGRA create: capacity=%d (requested=%d, vram_bound=%v) -> %d sub-index(es) for %d rows; cdc_cutoff=%d",
				plan.Capacity, requestedCapacity, plan.VRAMBound, plan.NumSubIdx, srcRowCount, plan.CdcCutoff)
		}

		nthread := uint32(vectorindex.GetConcurrency(u.tblcfg.ThreadsBuild))
		uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)

		// Packed sub-index tars go to the LOCAL fileservice's scratch dir rather
		// than /tmp: each tar is a whole sub-index, so a large build writes GB
		// through it, and LOCAL is the provisioned data volume. "" when no LOCAL
		// fileservice is attached, which os.MkdirTemp reads as $TMPDIR.
		spillDir := vimemory.HostSpillDir(proc.Ctx, proc.Base.FileService)
		if spillDir == "" {
			logutil.Infof("CAGRA create: no LOCAL fileservice; index tars will use $TMPDIR")
		}

		// ---- create builder ----
		// One real [B, Q] builder keyed on (base column type, storage qtype).
		// The 7 wired combos: f32 base × {f32, f16, int8, uint8}; f16 base ×
		// {f16, int8, uint8}.
		isF16Base := u.baseOid == types.T_array_float16
		switch {
		case isF16Base && qt == metric.Quantization_F16:
			u.builder, err = cagraPkg.NewCagraBuild[cuvs.Float16, cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case isF16Base && qt == metric.Quantization_INT8:
			u.builder, err = cagraPkg.NewCagraBuild[cuvs.Float16, int8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case isF16Base && qt == metric.Quantization_UINT8:
			u.builder, err = cagraPkg.NewCagraBuild[cuvs.Float16, uint8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_F16:
			u.builder, err = cagraPkg.NewCagraBuild[float32, cuvs.Float16](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_INT8:
			u.builder, err = cagraPkg.NewCagraBuild[float32, int8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		case qt == metric.Quantization_UINT8:
			u.builder, err = cagraPkg.NewCagraBuild[float32, uint8](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		default:
			u.builder, err = cagraPkg.NewCagraBuild[float32, float32](uid, u.idxcfg, u.tblcfg, nthread, devices, spillDir)
		}
		if err != nil {
			return err
		}

		// ---- pre-filter (INCLUDE columns) setup ----
		// u.filterCols was already resolved above (before memory.HostRowsFitting)
		// so its per-row bytes could be added to the host cost model. Just wire
		// it into the C++ FilterStore here.
		if len(u.filterCols) > 0 {
			logutil.Infof("CAGRA create: INCLUDE columns = %v (from %d arg vectors)",
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

	// Decode the base vector to its native type (see ivfpq_create_gpu.go for the
	// rationale). f16 base -> native []cuvs.Float16 for both the direct (half)
	// add and the CDC tail (stored as native half bytes — no f32 detour).
	var fa []float32
	var hf []cuvs.Float16
	if u.baseOid == types.T_array_float16 {
		h := types.BytesToArray[types.Float16](faVec.GetBytesAt(nthRow))
		if uint(len(h)) != u.idxcfg.CuvsCagra.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
		hf = f16ToCuvs(h)
	} else {
		fa = types.BytesToArray[float32](faVec.GetBytesAt(nthRow))
		if uint(len(fa)) != u.idxcfg.CuvsCagra.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
	}

	// Trailing rows below the cuvs threshold route to the CDC tail
	// (search-side brute-force replay) instead of the cuvs builder.
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
	// CagraBuild[B,Q] casts them back to its own []B/[]Q. No per-row alloc.
	vecBytes := util.UnsafeSliceToBytes(fa)
	if u.baseOid == types.T_array_float16 {
		vecBytes = util.UnsafeSliceToBytes(hf)
	}
	if err = u.builder.AddRow(id, vecBytes); err != nil {
		return err
	}

	// ---- per-row: append filter column values (if any) ----
	if len(u.filterCols) > 0 {
		if err = appendFilterRow(u.builder, u.filterCols, tf.ctr.argVecs, 3, nthRow); err != nil {
			return err
		}
	}
	return nil
}
