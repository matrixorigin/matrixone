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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
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
	keys           []any
	distances      []float64
	includeColumns []string
	// includeData stays keyed by column name for round-merge lookups and test
	// assertions; output order still comes from includeColumns, not map iteration.
	includeData          map[string][]any
	pushdownFilterSQL    string
	seenPK               map[string]struct{}
	cursor               *vectorindex.IvfSearchCursor
	multiRoundEnabled    bool
	baseSearchRoundLimit uint
	baseBucketExpandStep uint
	searchRoundLimit     uint
	bucketExpandStep     uint
	nthRow               int
	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch

	// Raw runtime-filter payload from the hash build side (optional).
	// IVF code will convert it into an exact-pk filter or entries bloom filter.
	bloomFilter      []byte
	indexReaderParam *plan.IndexReaderParam
}

// stub function
var (
	newIvfAlgo = newIvfAlgoFn
	getVersion = ivfflat.GetVersion
)

const (
	maxConsecutiveEmptyIvfRounds = 32
)

func newIvfAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (veccache.VectorIndexSearchIf, error) {
	switch idxcfg.Ivfflat.VectorType {
	case int32(types.T_array_float32):
		return ivfflat.NewIvfflatSearch[float32](idxcfg, tblcfg), nil
	case int32(types.T_array_float64):
		return ivfflat.NewIvfflatSearch[float64](idxcfg, tblcfg), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("newIvfAlgoFn: invalid vector type")
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
	u.seenPK = nil
	u.cursor = nil
	// Note: bloomFilter is kept across resets as it's only set once during initialization
	// It will be cleared in free() method
}

func (u *ivfSearchState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	n := 0
	consecutiveEmptyRounds := 0
	batchTargetRows := int(colexec.DefaultBatchSize)
	if u.limit > 0 && u.limit < uint64(batchTargetRows) {
		batchTargetRows = int(u.limit)
	}
	for n < batchTargetRows {
		if u.offset >= len(u.keys) {
			if !u.multiRoundEnabled {
				break
			}
			if u.cursor != nil && u.cursor.Exhausted {
				break
			}
			if err := u.fetchNextRound(tf, proc); err != nil {
				return vm.CancelResult, err
			}
			if u.offset >= len(u.keys) {
				if u.cursor != nil && u.cursor.Exhausted {
					break
				}
				consecutiveEmptyRounds++
				if consecutiveEmptyRounds >= maxConsecutiveEmptyIvfRounds {
					if u.cursor != nil {
						u.cursor.Exhausted = true
					}
					break
				}
				continue
			}
		}

		consecutiveEmptyRounds = 0
		vector.AppendAny(u.batch.Vecs[0], u.keys[u.offset], false, proc.Mp())
		vector.AppendFixed(u.batch.Vecs[1], u.distances[u.offset], false, proc.Mp())
		for i, col := range u.includeColumns {
			vector.AppendAny(u.batch.Vecs[2+i], u.includeData[col][u.offset], false, proc.Mp())
		}
		u.offset++
		n++
	}

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
	// Clear bloomFilter bytes to release memory
	u.bloomFilter = nil
	u.keys = nil
	u.distances = nil
	u.includeData = nil
	u.cursor = nil
	u.seenPK = nil
}

// waitBloomFilterForTableFunction blocks until it receives a bloomfilter runtime
// filter that matches tf.RuntimeFilterSpecs (if any). It is used when ivf_search
// acts as probe side in a join and the build side produces a runtime filter.
// We keep the raw serialized unique-join-key payload here and let IVF search
// decide whether to build an exact-pk predicate or a real entries bloom filter.
func waitBloomFilterForTableFunction(tf *TableFunction, proc *process.Process) ([]byte, error) {
	if len(tf.RuntimeFilterSpecs) == 0 {
		return nil, nil
	}
	spec := tf.RuntimeFilterSpecs[0]
	if !spec.UseBloomFilter {
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
		if m.Typ != message.RuntimeFilter_BLOOMFILTER {
			continue
		}

		return m.Data, nil
	}

	return nil, nil
}

func ivfSearchPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &ivfSearchState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	if arg.Limit != nil {
		if litExpr, ok := arg.Limit.Expr.(*plan.Expr_Lit); ok {
			if val, ok := litExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				st.limit = max(val.U64Val, uint64(1))
			}
		}
	}
	if arg.IndexReaderParam != nil &&
		arg.IndexReaderParam.GetLimit() != nil &&
		arg.IndexReaderParam.GetLimit().GetLit() != nil {
		// Planner may record a larger candidate budget than the user-visible LIMIT
		// so IVF search can fetch enough rows before residual filtering / OFFSET.
		// The outer sort/limit still enforces the final query semantics.
		st.limit = max(st.limit, max(arg.IndexReaderParam.GetLimit().GetLit().GetU64Val(), uint64(1)))
	}
	st.indexReaderParam = arg.IndexReaderParam

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *ivfSearchState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {
		if bf, err := waitBloomFilterForTableFunction(tf, proc); err != nil {
			return err
		} else {
			u.bloomFilter = bf
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
		if faVec.GetType().Oid != types.T_array_float32 && faVec.GetType().Oid != types.T_array_float64 {
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
		u.idxcfg.Ivfflat.VectorType = u.tblcfg.KeyPartType // array float32 or array float64

		u.batch = tf.createResultBatch()
		u.includeColumns = requestedIvfIncludeColumns(tf.Attrs)
		if u.limit == 0 && (!u.multiRoundEnabled || len(u.includeColumns) == 0) {
			u.limit = 1
		}
		u.inited = true
	}

	u.offset = 0
	u.keys = nil
	u.distances = nil
	u.includeData = make(map[string][]any, len(u.includeColumns))
	for _, col := range u.includeColumns {
		u.includeData[col] = nil
	}
	u.seenPK = make(map[string]struct{})
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
	u.nthRow = nthRow

	u.batch.CleanOnlyData()

	return u.fetchNextRound(tf, proc)
}

func requestedIvfIncludeColumns(attrs []string) []string {
	if len(attrs) <= 2 {
		return nil
	}

	cols := make([]string, 0, len(attrs)-2)
	for _, attr := range attrs[2:] {
		if strings.HasPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix) {
			cols = append(cols, strings.TrimPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix))
		}
	}
	return cols
}

func ivfSearchSeenKey(v any) string {
	return fmt.Sprintf("%T:%v", v, v)
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

	nextCount := u.bucketExpandStep
	remaining := total - nextOffset
	if nextCount > remaining {
		nextCount = remaining
	}
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
		}
		u.offset = 0
		return nil
	}

	u.advanceCursor()

	faVec := tf.ctr.argVecs[1]
	switch faVec.GetType().Oid {
	case types.T_array_float32:
		return runIvfSearchVector[float32](tf, u, proc, faVec, u.nthRow)
	case types.T_array_float64:
		return runIvfSearchVector[float64](tf, u, proc, faVec, u.nthRow)
	default:
		return moerr.NewInternalError(proc.Ctx, "vector is not array_float32 or array_float64")
	}
}

func runIvfSearchVector[T types.RealNumbers](tf *TableFunction, u *ivfSearchState, proc *process.Process, faVec *vector.Vector, nthRow int) (err error) {
	if faVec.IsNull(uint64(nthRow)) {
		if u.cursor != nil {
			u.cursor.Exhausted = true
		}
		return nil
	}

	fa := types.BytesToArray[T](faVec.GetBytesAt(nthRow))
	if uint(len(fa)) != u.idxcfg.Ivfflat.Dimensions {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("vector ops between different dimensions (%d, %d) is not permitted.", u.idxcfg.Ivfflat.Dimensions, len(fa)))
	}

	veccache.Cache.Once()

	algo, err := newIvfAlgo(u.idxcfg, u.tblcfg)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%d", u.tblcfg.IndexTable, u.idxcfg.Ivfflat.Version)
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
		BloomFilter:             u.bloomFilter,
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
	opStats.BackgroundQueries = append(opStats.BackgroundQueries, rt.BackgroundQueries...)

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
	}

	u.keys = u.keys[:0]
	u.distances = u.distances[:0]
	for _, col := range u.includeColumns {
		u.includeData[col] = u.includeData[col][:0]
	}
	u.offset = 0

	for i, keyAny := range keySlice {
		seenKey := ivfSearchSeenKey(keyAny)
		if _, ok := u.seenPK[seenKey]; ok {
			continue
		}
		u.seenPK[seenKey] = struct{}{}
		u.keys = append(u.keys, keyAny)
		u.distances = append(u.distances, distances[i])
		for _, col := range u.includeColumns {
			u.includeData[col] = append(u.includeData[col], includeResult.Data[col][i])
		}
	}

	return nil
}
