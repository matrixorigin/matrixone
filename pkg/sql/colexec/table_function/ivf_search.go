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
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ivfSearchState struct {
	inited         bool
	param          vectorindex.IvfParam
	tblcfg         vectorindex.IndexTableConfig
	idxcfg         vectorindex.IndexConfig
	offset         int
	limit          uint64
	emptyResult    bool
	keys           []any
	distances      []float64
	includeColumns []string
	// slots caches the pk/score/include output positions for the current result layout.
	slots vectorSearchSlots
	// includeData stays keyed by column name for round-merge lookups and test
	// assertions; output order still comes from includeColumns, not map iteration.
	includeData          map[string][]any
	includeNulls         map[string][]bool
	pushdownFilterSQL    string
	cursor               *vectorindex.IvfSearchCursor
	multiRoundEnabled    bool
	baseSearchRoundLimit uint
	baseBucketExpandStep uint
	searchRoundLimit     uint
	bucketExpandStep     uint
	emittedCandidates    uint64
	nthRow               int
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch

	// Raw runtime-filter payload from the hash build side (optional).
	// IVF code converts it into an exact-pk filter or entries membership filter.
	runtimeFilterData []byte
	indexReaderParam  *plan.IndexReaderParam
}

// stub function
var (
	newIvfAlgo = newIvfAlgoFn
	getVersion = ivfflat.GetVersion
)

func newIvfAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (veccache.VectorIndexSearchIf, error) {
	// The centroid search index is typed by the CENTROID storage type, not the
	// entry/input type. For narrow entries the centroids are f32 (decoupled), so
	// this returns IvfflatSearch[float32]. CentroidType == 0 (old indexes) means
	// "same as entry".
	ct := idxcfg.Ivfflat.CentroidType
	if ct == 0 {
		ct = idxcfg.Ivfflat.VectorType
	}
	switch ct {
	case int32(types.T_array_float32):
		return ivfflat.NewIvfflatSearch[float32](idxcfg, tblcfg), nil
	case int32(types.T_array_float64):
		return ivfflat.NewIvfflatSearch[float64](idxcfg, tblcfg), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("newIvfAlgoFn: invalid centroid type")
	}
}

func (u *ivfSearchState) end(tf *TableFunction, proc *process.Process) error {

	return nil
}

func (u *ivfSearchState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.includeData = nil
	u.includeNulls = nil
	u.cursor = nil
	u.emittedCandidates = 0
	// Note: runtimeFilterData is kept across resets as it's only set once during initialization
	// It will be cleared in free() method
}

func (u *ivfSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	n := 0
	batchTargetRows := int(colexec.DefaultBatchSize)
	if u.limit > 0 && u.limit < uint64(batchTargetRows) {
		batchTargetRows = int(u.limit)
	}
	for n < batchTargetRows {
		if u.offset >= len(u.keys) {
			if !u.multiRoundEnabled {
				break
			}
			// Drain the whole current SQL round before stopping. A round is sorted
			// independently, so truncating it as soon as the cumulative count reaches
			// the budget could discard a row that belongs in the global top-K.
			if u.limit > 0 && u.emittedCandidates >= u.limit {
				if u.cursor != nil {
					u.cursor.Exhausted = true
				}
				break
			}
			if u.cursor != nil && u.cursor.Exhausted {
				break
			}
			prevRound, prevNextOffset, prevBucketCount, prevExhausted := u.cursorProgress()
			if err := u.fetchNextRound(tf, proc); err != nil {
				return vm.CancelResult, err
			}
			if u.offset >= len(u.keys) {
				if u.cursor != nil && u.cursor.Exhausted {
					break
				}
				if u.cursor != nil && u.cursorSameProgress(prevRound, prevNextOffset, prevBucketCount, prevExhausted) {
					return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "ivf_search cursor did not advance after empty round")
				}
				continue
			}
		}

		// Slots resolved once per layout (see vector_search_layout.go): the planner projects
		// only the columns the query reads, so pkid, score or an INCLUDE column may be
		// absent, which is what a -1 slot means.
		if u.slots.pk >= 0 {
			vector.AppendAny(u.batch.Vecs[u.slots.pk], u.keys[u.offset], false, proc.Mp())
		}
		if u.slots.score >= 0 {
			vector.AppendFixed(u.batch.Vecs[u.slots.score], u.distances[u.offset], false, proc.Mp())
		}
		for ci, col := range u.includeColumns {
			pos := u.slots.include[ci]
			if pos < 0 {
				continue
			}
			isNull := false
			if u.includeNulls != nil {
				if nulls, ok := u.includeNulls[col]; ok && u.offset < len(nulls) {
					isNull = nulls[u.offset]
				}
			}
			vector.AppendAny(u.batch.Vecs[pos], u.includeData[col][u.offset], isNull, proc.Mp())
		}
		u.offset++
		u.emittedCandidates++
		n++
	}

	u.batch.SetRowCount(n)

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	// write the batch
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *ivfSearchState) cursorProgress() (round, nextOffset, bucketCount uint, exhausted bool) {
	if u.cursor == nil {
		return 0, 0, 0, false
	}
	return u.cursor.Round, u.cursor.NextBucketOffset, u.cursor.CurrentBucketCount, u.cursor.Exhausted
}

func (u *ivfSearchState) cursorSameProgress(round, nextOffset, bucketCount uint, exhausted bool) bool {
	if u.cursor == nil {
		return false
	}
	return u.cursor.Round == round &&
		u.cursor.NextBucketOffset == nextOffset &&
		u.cursor.CurrentBucketCount == bucketCount &&
		u.cursor.Exhausted == exhausted
}

func (u *ivfSearchState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
	// Clear runtime-filter bytes to release memory
	u.runtimeFilterData = nil
	u.keys = nil
	u.distances = nil
	u.includeData = nil
	u.includeNulls = nil
	u.cursor = nil
}

// waitRuntimeFilterDataForTableFunction blocks until it receives a membership runtime
// filter that matches tf.RuntimeFilterSpecs (if any). It is used when ivf_search
// acts as probe side in a join and the build side produces a runtime filter.
// We keep the raw serialized unique-join-key payload here and let IVF search
// decide whether to build an exact-pk predicate or an entries membership filter.
func waitRuntimeFilterDataForTableFunction(tf *TableFunction, proc *process.Process) ([]byte, error) {
	if len(tf.RuntimeFilterSpecs) == 0 {
		return nil, nil
	}
	spec := tf.RuntimeFilterSpecs[0]
	if !spec.UseMembershipFilter {
		return nil, nil
	}

	msgReceiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, proc.Ctx)
	if err != nil || ctxDone {
		return nil, err
	}

	for i := range msgs {
		m, ok := msgs[i].(message.RuntimeFilterMessage)
		if !ok {
			continue
		}
		if m.Typ != message.RuntimeFilter_UNIQUEJOINKEYS {
			continue
		}

		return m.Data, nil
	}

	return nil, nil
}

func ivfSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &ivfSearchState{}

	var indexReaderLimit *plan.Expr
	if arg.IndexReaderParam != nil {
		indexReaderLimit = arg.IndexReaderParam.GetLimit()
	}
	st.limit, err = evalLimitExpression(proc, indexReaderLimit, 0)
	if err != nil {
		return nil, err
	}
	if arg.Limit != nil {
		var tableFuncLimit uint64
		tableFuncLimit, err = evalLimitExpression(proc, arg.Limit, 1)
		if err != nil {
			return nil, err
		}
		st.limit = max(st.limit, tableFuncLimit)
	}
	st.emptyResult = (indexReaderLimit != nil || arg.Limit != nil) && st.limit == 0

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	if err != nil {
		return nil, err
	}

	st.indexReaderParam = arg.IndexReaderParam

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *ivfSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	if u.emptyResult {
		if u.batch == nil {
			u.batch = tf.createResultBatch()
		} else {
			u.batch.CleanOnlyData()
		}
		u.offset = 0
		u.keys = nil
		u.distances = nil
		u.includeData = nil
		u.includeNulls = nil
		u.cursor = nil
		u.multiRoundEnabled = false
		u.emittedCandidates = 0
		u.nthRow = nthRow
		u.inited = true
		return nil
	}

	if !u.inited {
		if runtimeFilterData, err := waitRuntimeFilterDataForTableFunction(tf, proc); err != nil {
			return err
		} else {
			u.runtimeFilterData = runtimeFilterData
		}

		if len(tf.Params) > 0 {
			err = sonic.Unmarshal([]byte(tf.Params), &u.param)
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

		metrictype, ok := metric.OpTypeToIvfMetric[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "invalid optype")
		}
		u.idxcfg.OpType = u.param.OpType
		u.idxcfg.Ivfflat.Metric = uint16(metrictype)

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
		err := sonic.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		if len(tf.ctr.argVecs) >= 3 {
			filterVec := tf.ctr.argVecs[2]
			if filterVec.GetType().Oid != types.T_varchar {
				return moerr.NewInvalidInput(proc.Ctx, "Third argument (pushdown filter) must be a string")
			}
			if !filterVec.IsConst() {
				return moerr.NewInternalError(proc.Ctx, "Pushdown filter must be a String constant")
			}
			u.pushdownFilterSQL = filterVec.UnsafeGetStringAt(0)
		}
		if len(tf.ctr.argVecs) >= 4 {
			roundLimitVec := tf.ctr.argVecs[3]
			if roundLimitVec.IsConst() && roundLimitVec.GetType().Oid == types.T_uint64 {
				u.baseSearchRoundLimit = uint(vector.GetFixedAtNoTypeCheck[uint64](roundLimitVec, 0))
			}
		}
		if len(tf.ctr.argVecs) >= 5 {
			stepVec := tf.ctr.argVecs[4]
			if stepVec.IsConst() && stepVec.GetType().Oid == types.T_uint64 {
				u.baseBucketExpandStep = uint(vector.GetFixedAtNoTypeCheck[uint64](stepVec, 0))
			}
		}
		u.multiRoundEnabled = u.baseSearchRoundLimit > 0 || u.baseBucketExpandStep > 0

		// f32vec
		faVec := tf.ctr.argVecs[1]
		if !catalogplugin.SupportsVectorType(ivfflatCatalogHooks, faVec.GetType().Oid) {
			return moerr.NewInvalidInput(proc.Ctx, "Second argument (vector must be a vecf32 or vecf64 type")
		}

		if int32(faVec.GetType().Oid) != u.tblcfg.KeyPartType {
			return moerr.NewInvalidInput(proc.Ctx, "Second argument (vector type not match with source part type")
		}

		dimension := faVec.GetType().Width

		// dimension
		u.idxcfg.Ivfflat.Dimensions = uint(dimension)
		u.idxcfg.Type = vectorindex.IVFFLAT

		// get version
		version, err := getVersion(sqlexec.NewSqlProcess(proc), u.tblcfg)
		if err != nil {
			return err
		}
		u.idxcfg.Ivfflat.Version = version                 // version from meta table
		u.idxcfg.Ivfflat.VectorType = u.tblcfg.KeyPartType // entry/input type
		// Centroid type is decoupled: f32 for narrow entries (must match the f32
		// centroid hidden table from schema.go), else same as the entry type.
		switch types.T(u.tblcfg.KeyPartType) {
		case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
			u.idxcfg.Ivfflat.CentroidType = int32(types.T_array_float32)
		default:
			u.idxcfg.Ivfflat.CentroidType = u.tblcfg.KeyPartType
		}
		// QUANTIZATION: entries are stored as the quantization (down-cast) type,
		// independent of the base column. The centroids are forced to f32 (decoupled
		// — accurate assignment, fast f32 search) for ANY base type, including f64;
		// the query is decoded to f32 for the centroid search and to the entry type
		// for the re-rank. VectorType = the entry/quantization type.
		if u.param.Quantization != "" {
			if qt, ok := quantizer.ToVectorType(u.param.Quantization); ok {
				u.idxcfg.Ivfflat.VectorType = int32(qt)
				u.idxcfg.Ivfflat.CentroidType = int32(types.T_array_float32)
			}
		}

		u.batch = tf.createResultBatch()
		u.includeColumns = requestedIvfIncludeColumns(tf.Attrs)
		// Resolve the output slots once for this layout; the emit loop below indexes them.
		u.slots = resolveVectorSearchSlots(u.batch.Attrs, u.includeColumns,
			catalog.SystemSI_IVFFLAT_IncludeColPrefix)
		if u.limit == 0 && (!u.multiRoundEnabled || len(u.includeColumns) == 0) {
			u.limit = 1
		}
		// When a residual filter will drop candidates after this search (post-filter
		// JOIN), grow the candidate budget so k rows still survive. For a prepared
		// LIMIT ? this is the only place k is known; a literal LIMIT was already
		// over-fetched at plan time and leaves the flag off. Done once here (guarded
		// by u.inited) so the search budget and the emit cap both see k'. See
		// pkg/vectorindex/overfetch (#26878).
		if u.tblcfg.PostFilterOverFetch && u.limit > 0 {
			u.limit = overfetch.FilteredPostModeLimit(u.limit)
		}
		u.inited = true
	}

	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.includeData = make(map[string][]any, len(u.includeColumns))
	u.includeNulls = make(map[string][]bool, len(u.includeColumns))
	for _, col := range u.includeColumns {
		u.includeData[col] = nil
		u.includeNulls[col] = nil
	}
	u.searchRoundLimit = u.baseSearchRoundLimit
	if u.searchRoundLimit == 0 {
		u.searchRoundLimit = uint(u.limit)
		if u.searchRoundLimit == 0 {
			u.searchRoundLimit = 1
		}
	}
	u.bucketExpandStep = u.baseBucketExpandStep
	if u.bucketExpandStep == 0 {
		u.bucketExpandStep = uint(u.tblcfg.Nprobe)
		if u.bucketExpandStep == 0 {
			u.bucketExpandStep = 1
		}
	}
	if u.multiRoundEnabled {
		u.cursor = &vectorindex.IvfSearchCursor{}
	} else {
		u.cursor = nil
	}
	u.emittedCandidates = 0
	u.nthRow = nthRow

	u.batch.CleanOnlyData()

	return u.fetchNextRound(tf, proc)
}

func requestedIvfIncludeColumns(attrs []string) []string {
	// Scan every attribute: the INCLUDE columns do not necessarily start at index 2,
	// because the planner can prune pkid or score ahead of them.
	cols := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		if strings.HasPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix) {
			cols = append(cols, strings.TrimPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix))
		}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

const defaultIvfIncludeCentroidBatchCap = uint(4096)

func ivfIncludeCentroidBatchCap() uint {
	return defaultIvfIncludeCentroidBatchCap
}

func nextIvfIncludeBucketCount(current, remaining, maxBatch uint) uint {
	if remaining == 0 {
		return 0
	}
	if current == 0 {
		current = 1
	}

	nextCount := current * 2
	if nextCount < current {
		nextCount = remaining
	}

	effectiveMax := maxBatch
	if effectiveMax > 0 && effectiveMax < current {
		effectiveMax = current
	}
	if effectiveMax > 0 && nextCount > effectiveMax {
		nextCount = effectiveMax
	}
	if nextCount > remaining {
		nextCount = remaining
	}
	return nextCount
}

func (u *ivfSearchState) advanceCursor() {
	if !u.multiRoundEnabled || u.cursor == nil || u.cursor.Round == 0 || u.cursor.Exhausted {
		return
	}

	nextOffset := u.cursor.NextBucketOffset + u.cursor.CurrentBucketCount
	total := uint(len(u.cursor.RankedCentroidIDs))
	if nextOffset >= total {
		u.cursor.NextBucketOffset = total
		u.cursor.CurrentBucketCount = 0
		u.cursor.Exhausted = true
		return
	}

	remaining := total - nextOffset
	nextCount := nextIvfIncludeBucketCount(u.cursor.CurrentBucketCount, remaining, ivfIncludeCentroidBatchCap())
	u.cursor.NextBucketOffset = nextOffset
	u.cursor.CurrentBucketCount = nextCount
	u.cursor.Exhausted = false
}

func (u *ivfSearchState) fetchNextRound(tf *TableFunction, proc *process.Process) error {
	if u.cursor != nil && u.cursor.Exhausted {
		u.keys = nil
		u.distances = nil
		for _, col := range u.includeColumns {
			u.includeData[col] = nil
			u.includeNulls[col] = nil
		}
		u.offset = 0
		return nil
	}

	u.advanceCursor()

	faVec := tf.ctr.argVecs[1]

	// Dispatch on the CENTROID type, not the base type. Only a plain f64 index
	// (f64 base, no quantization) keeps f64 centroids; every other case — f32 base,
	// narrow base, or any base under QUANTIZATION — searches f32 centroids, so the
	// query is decoded to float32 regardless of its column type.
	if u.idxcfg.Ivfflat.CentroidType == int32(types.T_array_float64) {
		return runIvfSearchVector[float64](tf, u, proc, faVec, u.nthRow)
	}
	return runIvfSearchVectorToF32(tf, u, proc, faVec, u.nthRow)
}

func runIvfSearchVector[T types.RealNumbers](tf *TableFunction, u *ivfSearchState, proc *process.Process, faVec *vector.Vector, nthRow int) (err error) {
	if faVec.IsNull(uint64(nthRow)) {
		if u.cursor != nil {
			u.cursor.Exhausted = true
		}
		return nil
	}
	return runIvfSearchQuery(tf, u, proc, types.BytesToArray[T](faVec.GetBytesAt(nthRow)))
}

// runIvfSearchVectorToF32 decodes the query (of any vector column type: f32, f64,
// or narrow bf16/f16/int8) to float32 and runs the float32 centroid search. The
// SQL re-rank then encodes the query in the entry/quantization type.
func runIvfSearchVectorToF32(tf *TableFunction, u *ivfSearchState, proc *process.Process, faVec *vector.Vector, nthRow int) error {
	if faVec.IsNull(uint64(nthRow)) {
		if u.cursor != nil {
			u.cursor.Exhausted = true
		}
		return nil
	}
	b := faVec.GetBytesAt(nthRow)
	var fa []float32
	switch faVec.GetType().Oid {
	case types.T_array_float32:
		fa = types.BytesToArray[float32](b)
	case types.T_array_float64:
		f64 := types.BytesToArray[float64](b)
		fa = make([]float32, len(f64))
		for i, x := range f64 {
			fa[i] = float32(x)
		}
	case types.T_array_bf16:
		fa = types.BF16ToFloat32Slice(types.BytesToArray[types.BF16](b))
	case types.T_array_float16:
		fa = types.Float16ToFloat32Slice(types.BytesToArray[types.Float16](b))
	case types.T_array_int8:
		fa = types.Int8ToFloat32Slice(types.BytesToArray[int8](b))
	case types.T_array_uint8:
		fa = types.Uint8ToFloat32Slice(types.BytesToArray[uint8](b))
	default:
		return moerr.NewInternalError(proc.Ctx, "unsupported ivfflat vector type")
	}
	return runIvfSearchQuery(tf, u, proc, fa)
}

func runIvfSearchQuery[T types.RealNumbers](tf *TableFunction, u *ivfSearchState, proc *process.Process, fa []T) (err error) {
	if uint(len(fa)) != u.idxcfg.Ivfflat.Dimensions {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("vector ops between different dimensions (%d, %d) is not permitted.", u.idxcfg.Ivfflat.Dimensions, len(fa)))
	}

	veccache.Cache.Once()

	algo, err := newIvfAlgo(u.idxcfg, u.tblcfg)
	if err != nil {
		return err
	}
	key := ivfSearchCacheKey(u.tblcfg.IndexTable, u.idxcfg.Ivfflat.Version, u.indexReaderParam)
	rtLimit := uint(u.limit)
	if rtLimit == 0 {
		rtLimit = 1
	}
	useIncludeRuntime := u.multiRoundEnabled || len(u.includeColumns) > 0 || u.pushdownFilterSQL != ""

	var includeResult *vectorindex.IvfIncludeResult
	var requestedIncludeColumns []string
	var pushdownFilterSQL string
	var searchRoundLimit uint
	var bucketExpandStep uint
	var searchCursor *vectorindex.IvfSearchCursor
	if useIncludeRuntime {
		includeResult = &vectorindex.IvfIncludeResult{}
		requestedIncludeColumns = u.includeColumns
		pushdownFilterSQL = u.pushdownFilterSQL
		searchRoundLimit = u.searchRoundLimit
		bucketExpandStep = u.bucketExpandStep
		searchCursor = u.cursor
	}
	rt := vectorindex.RuntimeConfig{
		Limit:                   rtLimit,
		Probe:                   uint(u.tblcfg.Nprobe),
		OrigFuncName:            u.tblcfg.OrigFuncName,
		BackgroundQueries:       make([]*plan.Query, 1),
		RuntimeFilterData:       u.runtimeFilterData,
		RequestedIncludeColumns: requestedIncludeColumns,
		PushdownFilterSQL:       pushdownFilterSQL,
		IncludeResult:           includeResult,
		SearchRoundLimit:        searchRoundLimit,
		BucketExpandStep:        bucketExpandStep,
		SearchCursor:            searchCursor,
	}
	sqlProc := sqlexec.NewSqlProcess(proc)
	sqlProc.IndexReaderParam = u.indexReaderParam
	sqlProc.RuntimeFilterSpecs = tf.RuntimeFilterSpecs

	keys, distances, err := veccache.Cache.Search(sqlProc, key, algo, fa, rt)
	if err != nil {
		return err
	}

	opStats := tf.OpAnalyzer.GetOpStats()
	if shouldRecordIvfSearchBackgroundQueries(proc, u.indexReaderParam) {
		opStats.BackgroundQueries = append(opStats.BackgroundQueries, rt.BackgroundQueries...)
	}

	keySlice, ok := keys.([]any)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "keys is not []any")
	}
	for _, col := range u.includeColumns {
		if len(includeResult.Data[col]) != len(keySlice) {
			return moerr.NewInternalErrorf(
				proc.Ctx,
				"ivf_search: include data length mismatch for column %s: keys=%d, data=%d",
				col,
				len(keySlice),
				len(includeResult.Data[col]),
			)
		}
		if includeResult.Nulls != nil && len(includeResult.Nulls[col]) != len(keySlice) {
			return moerr.NewInternalErrorf(
				proc.Ctx,
				"ivf_search: include nulls length mismatch for column %s: keys=%d, nulls=%d",
				col,
				len(keySlice),
				len(includeResult.Nulls[col]),
			)
		}
	}

	u.keys = u.keys[:0]
	u.distances = u.distances[:0]
	for _, col := range u.includeColumns {
		u.includeData[col] = u.includeData[col][:0]
		u.includeNulls[col] = u.includeNulls[col][:0]
	}
	u.offset = 0

	// Search rounds cover disjoint centroid slices, and current-version entries
	// assign each PK to one centroid. Retaining PKs across rounds would therefore
	// add table-cardinality memory without deduplicating healthy index data.
	for i, keyAny := range keySlice {
		u.keys = append(u.keys, keyAny)
		u.distances = append(u.distances, distances[i])
		for _, col := range u.includeColumns {
			u.includeData[col] = append(u.includeData[col], includeResult.Data[col][i])
			isNull := false
			if includeResult.Nulls != nil {
				isNull = includeResult.Nulls[col][i]
			}
			u.includeNulls[col] = append(u.includeNulls[col], isNull)
		}
	}

	return nil
}

func isRemoteRunContext(proc *process.Process) bool {
	if proc == nil || proc.Ctx == nil {
		return false
	}
	v, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool)
	return v
}

func shouldRecordIvfSearchBackgroundQueries(proc *process.Process, param *plan.IndexReaderParam) bool {
	// A Multi-CN query has one partition executing on its coordinator. Record
	// that local plan as the representative entries scan, regardless of its
	// partition index. Remote physical plans use JSON for operator statistics;
	// plan.Expr contains protobuf oneofs that cannot be decoded through that
	// JSON path, so remote partitions must not carry background query plans.
	return !isRemoteRunContext(proc)
}

func ivfSearchCacheKey(indexTable string, version int64, param *plan.IndexReaderParam) string {
	key := fmt.Sprintf("%s:%d", indexTable, version)
	if param.GetPartitionCnCnt() > 1 {
		key = fmt.Sprintf("%s:%d/%d", key, param.GetPartitionCnIdx(), param.GetPartitionCnCnt())
	}
	return key
}
