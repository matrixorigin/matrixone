// Copyright 2026 Matrix Origin
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

package ivfflat

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// planReader is the IVF-FLAT implementation of an optimizer-visible vector
// index scan. It owns one execution generation and its direct relation scanner.
type planReader struct {
	proc    *process.Process
	spec    *plan.VectorIndexScan
	req     searchplugin.Request
	scanner *relationScanner
	closed  bool

	initialized  bool
	keys         []any
	distances    []float64
	includeData  map[string][]any
	includeNulls map[string][]bool
	offset       int
}

var _ engine.Reader = (*planReader)(nil)

func NewPlanReader(proc *process.Process, spec *plan.VectorIndexScan, req searchplugin.Request) (engine.Reader, error) {
	if proc == nil || proc.GetTxnOperator() == nil || proc.GetSessionInfo() == nil || proc.GetSessionInfo().StorageEngine == nil {
		return nil, moerr.NewInvalidStateNoCtx("ivfflat vector scan requires a process, transaction, and storage engine")
	}
	if spec == nil || spec.Index == nil || spec.SourceTable == nil {
		return nil, moerr.NewInvalidInputNoCtx("ivfflat vector scan is missing source or index metadata")
	}
	if req.CandidateBudget < req.ResultLimit {
		return nil, moerr.NewInvalidInputNoCtx("ivfflat candidate budget is smaller than the result limit")
	}
	r := &planReader{proc: proc, spec: spec, req: req}
	r.scanner = &relationScanner{
		proc:           proc,
		partitionCount: req.Identity.PartitionCount,
		partitionIndex: req.Identity.PartitionIndex,
		ownsInMemory:   ownsInMemoryPartition(req.Identity.PartitionCount, req.Identity.PartitionIndex),
		txnOffset:      req.Identity.TxnOffset,
		snapshot:       cloneIvfSnapshot(req.Identity.Snapshot),
	}
	if req.Identity.PhysicalAccountID != nil {
		accountID := *req.Identity.PhysicalAccountID
		r.scanner.accountID = &accountID
	}
	return r, nil
}

func ownsInMemoryPartition(partitionCount, partitionIndex int32) bool {
	return partitionCount <= 1 || partitionIndex == 0
}

func cloneIvfSnapshot(snapshot *plan.Snapshot) *plan.Snapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	if snapshot.TS != nil {
		ts := *snapshot.TS
		clone.TS = &ts
	}
	if snapshot.Tenant != nil {
		tenant := *snapshot.Tenant
		clone.Tenant = &tenant
	}
	if snapshot.ExtraInfo != nil {
		extra := *snapshot.ExtraInfo
		clone.ExtraInfo = &extra
	}
	return &clone
}

func (r *planReader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	r.keys = nil
	r.distances = nil
	r.includeData = nil
	r.includeNulls = nil
	r.scanner = nil
	return nil
}

func (r *planReader) Read(ctx context.Context, attrs []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.closed {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return false, err
		}
		r.initialized = true
	}
	if r.offset >= len(r.keys) {
		return true, nil
	}

	out.CleanOnlyData()
	end := min(len(r.keys), r.offset+8192)
	for row := r.offset; row < end; row++ {
		for col, attr := range attrs {
			switch {
			case attr == "pkid":
				if err := vector.AppendAny(out.Vecs[col], r.keys[row], false, mp); err != nil {
					return false, err
				}
			case attr == "score":
				if err := vector.AppendFixed(out.Vecs[col], r.distances[row], false, mp); err != nil {
					return false, err
				}
			case strings.HasPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix):
				name := strings.TrimPrefix(attr, catalog.SystemSI_IVFFLAT_IncludeColPrefix)
				values, ok := r.includeData[name]
				if !ok || row >= len(values) {
					return false, moerr.NewInternalErrorNoCtxf("ivfflat include output %q is not aligned", name)
				}
				isNull := row < len(r.includeNulls[name]) && r.includeNulls[name][row]
				if err := vector.AppendAny(out.Vecs[col], values[row], isNull, mp); err != nil {
					return false, err
				}
			default:
				return false, moerr.NewInternalErrorNoCtxf("unknown ivfflat vector scan output %q", attr)
			}
		}
	}
	out.SetRowCount(end - r.offset)
	r.offset = end
	return false, nil
}

func (*planReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (*planReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*planReader) SetIndexParam(*plan.IndexReaderParam) {}
func (*planReader) SetFilterZM(objectio.ZoneMap)         {}

func (r *planReader) initialize() error {
	if r.req.CandidateBudget == 0 {
		return nil
	}
	if r.req.HasMembershipFilter && len(r.req.MembershipFilter) == 0 {
		// UNIQUEJOINKEYS with no payload is an exact empty build side. RF PASS
		// is represented by HasMembershipFilter=false and must still search.
		return nil
	}
	param := vectorindex.IvfParam{}
	if err := sonic.Unmarshal([]byte(r.spec.Index.IndexAlgoParams), &param); err != nil {
		return err
	}
	lists, err := strconv.Atoi(param.Lists)
	if err != nil || lists <= 0 {
		return moerr.NewInvalidInputNoCtxf("invalid IVF lists value %q", param.Lists)
	}
	metricType, ok := metric.OpTypeToIvfMetric[param.OpType]
	if !ok {
		return moerr.NewInvalidInputNoCtxf("invalid IVF op_type %q", param.OpType)
	}
	if len(r.spec.Index.Parts) == 0 || r.spec.SourceTableDef == nil || r.spec.SourceTableDef.Pkey == nil {
		return moerr.NewInvalidInputNoCtx("incomplete IVF source/index metadata")
	}

	metaTable := r.hiddenTable(catalog.SystemSI_IVFFLAT_TblType_Metadata)
	centroidTable := r.hiddenTable(catalog.SystemSI_IVFFLAT_TblType_Centroids)
	entriesTable := r.hiddenTable(catalog.SystemSI_IVFFLAT_TblType_Entries)
	if metaTable == "" || centroidTable == "" || entriesTable == "" {
		return moerr.NewInvalidInputNoCtx("IVF vector scan is missing hidden-table references")
	}
	partName := r.spec.Index.Parts[0]
	partPos, ok := r.spec.SourceTableDef.Name2ColIndex[partName]
	if !ok {
		return moerr.NewInvalidInputNoCtxf("IVF source vector column %q not found", partName)
	}
	pkName := r.spec.SourceTableDef.Pkey.PkeyColName
	pkPos, ok := r.spec.SourceTableDef.Name2ColIndex[pkName]
	if !ok {
		return moerr.NewInvalidInputNoCtxf("IVF source primary key %q not found", pkName)
	}
	includeColumns := append([]string(nil), r.spec.Index.IncludedColumns...)
	if len(includeColumns) == 0 {
		includeColumns = append(includeColumns, r.spec.IncludedColumns...)
	}
	includeTypes := make([]int32, 0, len(includeColumns))
	for _, name := range includeColumns {
		pos, found := r.spec.SourceTableDef.Name2ColIndex[name]
		if !found || pos < 0 || int(pos) >= len(r.spec.SourceTableDef.Cols) {
			return moerr.NewInvalidInputNoCtxf("IVF included column %q not found in source table", name)
		}
		includeTypes = append(includeTypes, r.spec.SourceTableDef.Cols[pos].Typ.Id)
	}

	tblcfg := vectorindex.IndexTableConfig{
		DbName:             r.spec.SourceTable.SchemaName,
		SrcTable:           r.spec.SourceTableDef.Name,
		MetadataTable:      metaTable,
		IndexTable:         centroidTable,
		EntriesTable:       entriesTable,
		ThreadsSearch:      r.spec.ThreadsSearch,
		Nprobe:             uint(max(uint32(1), r.spec.InitialProbeCount)),
		PKeyType:           r.spec.SourceTableDef.Cols[pkPos].Typ.Id,
		PKey:               pkName,
		KeyPart:            partName,
		KeyPartType:        r.spec.SourceTableDef.Cols[partPos].Typ.Id,
		OrigFuncName:       r.spec.DistanceFunction,
		IncludeColumns:     includeColumns,
		IncludeColumnTypes: includeTypes,
	}
	sqlproc := sqlexec.NewSqlProcess(r.proc)
	sqlproc.RelationScanner = r.scanner
	sqlproc.IvfRuntimeFilterData = append([]byte(nil), r.req.MembershipFilter...)
	sqlproc.IvfHasMembershipFilter = r.req.HasMembershipFilter
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{
		Limit:        ivfUint64Expr(r.req.CandidateBudget),
		OrderBy:      []*plan.OrderBySpec{{Flag: r.spec.Direction}},
		OrigFuncName: r.spec.DistanceFunction,
		DistRange:    r.req.DistanceRange,
	}
	version, err := GetVersion(sqlproc, tblcfg)
	if err != nil {
		return err
	}
	idxcfg := vectorindex.IndexConfig{
		Type:   vectorindex.IVFFLAT,
		OpType: param.OpType,
	}
	idxcfg.Ivfflat.Lists = uint(lists)
	idxcfg.Ivfflat.Metric = uint16(metricType)
	idxcfg.Ivfflat.Version = version
	expectedDimensions := r.spec.SourceTableDef.Cols[partPos].Typ.Width
	idxcfg.Ivfflat.Dimensions = uint(expectedDimensions)
	idxcfg.Ivfflat.VectorType = tblcfg.KeyPartType
	idxcfg.Ivfflat.CentroidType = tblcfg.KeyPartType
	switch types.T(tblcfg.KeyPartType) {
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		idxcfg.Ivfflat.CentroidType = int32(types.T_array_float32)
	}
	if param.Quantization != "" {
		if qt, found := quantizer.ToVectorType(param.Quantization); found {
			idxcfg.Ivfflat.VectorType = int32(qt)
			idxcfg.Ivfflat.CentroidType = int32(types.T_array_float32)
		}
	}

	if idxcfg.Ivfflat.CentroidType == int32(types.T_array_float64) {
		query, decodeErr := r.queryFloat64()
		if decodeErr != nil {
			return decodeErr
		}
		if dimensionErr := validateIvfQueryDimensions(expectedDimensions, len(query)); dimensionErr != nil {
			return dimensionErr
		}
		if expectedDimensions <= 0 {
			idxcfg.Ivfflat.Dimensions = uint(len(query))
		}
		return searchPlanReader(r, sqlproc, idxcfg, tblcfg, query)
	}
	query, err := r.queryFloat32()
	if err != nil {
		return err
	}
	if err = validateIvfQueryDimensions(expectedDimensions, len(query)); err != nil {
		return err
	}
	if expectedDimensions <= 0 {
		idxcfg.Ivfflat.Dimensions = uint(len(query))
	}
	return searchPlanReader(r, sqlproc, idxcfg, tblcfg, query)
}

func validateIvfQueryDimensions(expected int32, actual int) error {
	if expected > 0 && int(expected) != actual {
		return moerr.NewArrayInvalidOpNoCtx(int(expected), actual)
	}
	return nil
}

func (r *planReader) hiddenTable(role string) string {
	for _, table := range r.spec.HiddenTables {
		if table != nil && table.Role == role && table.Object != nil {
			return table.Object.ObjName
		}
	}
	return ""
}

func (r *planReader) queryFloat64() ([]float64, error) {
	if types.T(r.req.QueryType.Id) != types.T_array_float64 {
		return nil, moerr.NewInvalidInputNoCtx("f64 IVF centroids require a VECF64 query")
	}
	return types.BytesToArray[float64](r.req.QueryVector), nil
}

func (r *planReader) queryFloat32() ([]float32, error) {
	switch types.T(r.req.QueryType.Id) {
	case types.T_array_float32:
		return types.BytesToArray[float32](r.req.QueryVector), nil
	case types.T_array_float64:
		values := types.BytesToArray[float64](r.req.QueryVector)
		out := make([]float32, len(values))
		for i, value := range values {
			out[i] = float32(value)
		}
		return out, nil
	case types.T_array_bf16:
		return types.BF16ToFloat32Slice(types.BytesToArray[types.BF16](r.req.QueryVector)), nil
	case types.T_array_float16:
		return types.Float16ToFloat32Slice(types.BytesToArray[types.Float16](r.req.QueryVector)), nil
	case types.T_array_int8:
		return types.Int8ToFloat32Slice(types.BytesToArray[int8](r.req.QueryVector)), nil
	case types.T_array_uint8:
		return types.Uint8ToFloat32Slice(types.BytesToArray[uint8](r.req.QueryVector)), nil
	default:
		return nil, moerr.NewInvalidInputNoCtxf("unsupported IVF query type %s", types.T(r.req.QueryType.Id))
	}
}

func searchPlanReader[T types.RealNumbers](
	r *planReader,
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
) error {
	cache.Cache.Once()
	algo := NewIvfflatSearch[T](idxcfg, tblcfg)
	key := fmt.Sprintf("%s:%d", tblcfg.IndexTable, idxcfg.Ivfflat.Version)
	if source := r.spec.SourceTable; source != nil && source.PubInfo != nil {
		key = fmt.Sprintf("tenant=%d:%s", source.PubInfo.TenantId, key)
	}
	if r.req.Identity.PartitionCount > 1 {
		key = fmt.Sprintf("%s:%d/%d", key, r.req.Identity.PartitionIndex, r.req.Identity.PartitionCount)
	}

	multiRound := r.req.HasFirstRound || r.spec.BucketExpandStep > 0
	var cursor *vectorindex.IvfSearchCursor
	if multiRound {
		cursor = &vectorindex.IvfSearchCursor{}
	}
	limit := uint(r.req.CandidateBudget)
	if uint64(limit) != r.req.CandidateBudget {
		return moerr.NewInvalidInputNoCtx("IVF candidate limit exceeds platform uint")
	}
	if limit == 0 {
		return nil
	}
	firstRoundLimit := uint(0)
	if r.req.HasFirstRound {
		if r.req.FirstRoundLimit > uint64(^uint(0)>>1) {
			return moerr.NewInvalidInputNoCtx("IVF first-round limit is not a platform uint")
		}
		firstRoundLimit = uint(r.req.FirstRoundLimit)
	}
	r.includeData = make(map[string][]any, len(r.spec.IncludedColumns))
	r.includeNulls = make(map[string][]bool, len(r.spec.IncludedColumns))
	for _, name := range r.spec.IncludedColumns {
		r.includeData[name] = nil
		r.includeNulls[name] = nil
	}

	for {
		includeResult := &vectorindex.IvfIncludeResult{}
		rt := vectorindex.RuntimeConfig{
			Limit:                   limit,
			Probe:                   uint(max(uint32(1), r.spec.InitialProbeCount)),
			OrigFuncName:            r.spec.DistanceFunction,
			RuntimeFilterData:       r.req.MembershipFilter,
			RequestedIncludeColumns: r.spec.IncludedColumns,
			PushdownFilters:         r.req.PreFilters,
			IncludeResult:           includeResult,
			SearchRoundLimit:        firstRoundLimit,
			BucketExpandStep:        uint(r.spec.BucketExpandStep),
			SearchCursor:            cursor,
		}
		keys, distances, err := cache.Cache.Search(sqlproc, key, algo, query, rt)
		if err != nil {
			return err
		}
		keySlice, ok := keys.([]any)
		if !ok {
			return moerr.NewInternalErrorNoCtx("ivfflat keys are not []any")
		}
		r.keys = append(r.keys, keySlice...)
		r.distances = append(r.distances, distances...)
		for _, name := range r.spec.IncludedColumns {
			r.includeData[name] = append(r.includeData[name], includeResult.Data[name]...)
			r.includeNulls[name] = append(r.includeNulls[name], includeResult.Nulls[name]...)
		}

		if !multiRound || cursor == nil || cursor.Exhausted || uint64(len(r.keys)) >= uint64(limit) {
			break
		}
		advancePlanCursor(cursor)
	}
	r.sortAndLimit(uint64(limit))
	return nil
}

func advancePlanCursor(cursor *vectorindex.IvfSearchCursor) {
	if cursor == nil || cursor.Round == 0 || cursor.Exhausted {
		return
	}
	next := cursor.NextBucketOffset + cursor.CurrentBucketCount
	total := uint(len(cursor.RankedCentroidIDs))
	if next >= total {
		cursor.NextBucketOffset = total
		cursor.CurrentBucketCount = 0
		cursor.Exhausted = true
		return
	}
	remaining := total - next
	count := cursor.CurrentBucketCount * 2
	if count < cursor.CurrentBucketCount {
		count = remaining
	}
	if count > 4096 {
		count = 4096
	}
	if count > remaining {
		count = remaining
	}
	cursor.NextBucketOffset = next
	cursor.CurrentBucketCount = count
}

func (r *planReader) sortAndLimit(candidateLimit uint64) {
	order := make([]int, len(r.keys))
	for i := range order {
		order[i] = i
	}
	desc := r.spec.Direction&plan.OrderBySpec_DESC != 0
	sort.SliceStable(order, func(i, j int) bool {
		left, right := r.distances[order[i]], r.distances[order[j]]
		if left == right {
			return fmt.Sprint(r.keys[order[i]]) < fmt.Sprint(r.keys[order[j]])
		}
		if desc {
			return left > right
		}
		return left < right
	})
	limit := len(order)
	if candidateLimit < uint64(limit) {
		limit = int(candidateLimit)
	}
	keys := make([]any, 0, limit)
	distances := make([]float64, 0, limit)
	includeData := make(map[string][]any, len(r.includeData))
	includeNulls := make(map[string][]bool, len(r.includeNulls))
	for name := range r.includeData {
		includeData[name] = make([]any, 0, limit)
		includeNulls[name] = make([]bool, 0, limit)
	}
	for _, idx := range order[:limit] {
		keys = append(keys, r.keys[idx])
		distances = append(distances, r.distances[idx])
		for name, values := range r.includeData {
			includeData[name] = append(includeData[name], values[idx])
			includeNulls[name] = append(includeNulls[name], r.includeNulls[name][idx])
		}
	}
	r.keys, r.distances = keys, distances
	r.includeData, r.includeNulls = includeData, includeNulls
}

// relationScanner executes typed hidden-table reads in the caller's current
// transaction. It is the direct-engine replacement for sqlexec.RunSql.
type relationScanner struct {
	proc           *process.Process
	accountID      *uint32
	snapshot       *plan.Snapshot
	partitionCount int32
	partitionIndex int32
	ownsInMemory   bool
	txnOffset      int
}

var _ sqlexec.RelationScanExecutor = (*relationScanner)(nil)

func (s *relationScanner) ScanRelation(req sqlexec.RelationScanRequest) (res executor.Result, err error) {
	res = executor.NewResult(s.proc.Mp())
	ctx := s.proc.Ctx
	if s.accountID != nil {
		ctx = defines.AttachAccountId(ctx, *s.accountID)
	}
	txn := s.proc.GetTxnOperator()
	if s.snapshot != nil && s.snapshot.TS != nil &&
		(s.snapshot.TS.LogicalTime != 0 || s.snapshot.TS.PhysicalTime != 0) &&
		s.snapshot.TS.Less(txn.Txn().SnapshotTS) {
		clone := s.proc.GetCloneTxnOperator()
		if clone == nil {
			clone = txn.CloneSnapshotOp(*s.snapshot.TS)
			s.proc.SetCloneTxnOperator(clone)
		}
		txn = clone
	}
	db, err := s.proc.GetSessionInfo().StorageEngine.Database(ctx, req.Schema, txn)
	if err != nil {
		return res, err
	}
	rel, err := db.Relation(ctx, req.Table, s.proc)
	if err != nil {
		return res, err
	}
	rel = engine.NewRelationHandle(rel)
	tableDef := rel.GetTableDef(ctx)
	if tableDef == nil {
		return res, moerr.NewInvalidStateNoCtxf("ivfflat hidden relation %s.%s has no table definition", req.Schema, req.Table)
	}

	partitionCount := req.PartitionCount
	if partitionCount <= 0 {
		partitionCount = s.partitionCount
	}
	if partitionCount <= 0 {
		partitionCount = 1
	}
	partitionIndex := req.PartitionIndex
	if req.PartitionCount <= 0 {
		partitionIndex = s.partitionIndex
	}
	txnOffset := s.txnOffset

	rsp := &engine.RangesShuffleParam{
		Node:              &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: tableDef},
		CNCNT:             partitionCount,
		CNIDX:             partitionIndex,
		IsLocalCN:         s.ownsInMemory,
		ShuffleByObjectID: partitionCount > 1,
	}
	policy := relationScanPolicy(partitionCount, s.ownsInMemory)
	relData, err := rel.Ranges(ctx, engine.RangesParam{
		BlockFilters: req.BlockFilters,
		TxnOffset:    txnOffset,
		Policy:       policy,
		Rsp:          rsp,
	})
	if err != nil {
		return res, err
	}
	readers, err := rel.BuildReaders(
		ctx,
		s.proc,
		req.Filter,
		relData,
		1,
		txnOffset,
		req.IndexParam != nil && !req.PostFilterTopOnly,
		engine.Policy_CheckAll,
		req.FilterHint,
	)
	if err != nil {
		return res, err
	}
	var filterExecutor colexec.ExpressionExecutor
	if req.Filter != nil {
		filterExecutor, err = colexec.NewExpressionExecutor(s.proc, req.Filter)
		if err != nil {
			for _, reader := range readers {
				_ = reader.Close()
			}
			return res, err
		}
		defer filterExecutor.Free()
	}
	defer func() {
		for _, reader := range readers {
			if closeErr := reader.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
		if err != nil {
			res.Close()
			res.Batches = nil
		}
	}()

	for _, reader := range readers {
		if !req.PostFilterTopOnly {
			reader.SetIndexParam(req.IndexParam)
		}
		for {
			bat, makeErr := makeRelationScanBatch(tableDef, req.Columns)
			if makeErr != nil {
				return res, makeErr
			}
			end, readErr := reader.Read(ctx, req.Columns, req.Filter, s.proc.Mp(), bat)
			if readErr != nil {
				bat.Clean(s.proc.Mp())
				return res, readErr
			}
			if end {
				bat.Clean(s.proc.Mp())
				break
			}
			if filterExecutor != nil && !bat.IsEmpty() {
				if readErr = filterRelationBatch(s.proc, filterExecutor, bat); readErr != nil {
					bat.Clean(s.proc.Mp())
					return res, readErr
				}
			}
			if req.BatchTransform != nil && !bat.IsEmpty() {
				if readErr = req.BatchTransform(bat); readErr != nil {
					bat.Clean(s.proc.Mp())
					return res, readErr
				}
			}
			if bat.IsEmpty() {
				bat.Clean(s.proc.Mp())
				continue
			}
			res.Batches = append(res.Batches, bat)
			if vectorTopLimit, ok := relationVectorTopLimit(req.IndexParam, req.PostFilterTopOnly); ok && resultRowCount(res.Batches) > 2*vectorTopLimit {
				if err = compactRelationTop(&res, vectorTopLimit, req.IndexParam.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0); err != nil {
					return res, err
				}
			}
		}
	}
	if vectorTopLimit, ok := relationVectorTopLimit(req.IndexParam, req.PostFilterTopOnly); ok {
		if err = compactRelationTop(&res, vectorTopLimit, req.IndexParam.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0); err != nil {
			return res, err
		}
	}
	return res, nil
}

// relationScanPolicy mirrors the distributed table-scan ownership contract:
// partition zero owns committed in-memory/appendable rows, while every other
// partition reads only persisted objects assigned by object ID. The scheduler
// pins the coordinator-local CN at ordinal zero for VECTOR_INDEX_SCAN queries.
// Replicated metadata/centroid requests set partitionCount=1 and therefore
// read all visible data on every executing CN.
func relationScanPolicy(partitionCount int32, ownsInMemory bool) engine.DataCollectPolicy {
	if partitionCount > 1 && !ownsInMemory {
		return engine.Policy_CollectCommittedPersistedData
	}
	return engine.Policy_CollectAllData
}

func filterRelationBatch(proc *process.Process, executor colexec.ExpressionExecutor, bat *batch.Batch) error {
	result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return err
	}
	values := vector.GenerateFunctionFixedTypeParameter[bool](result)
	sels := make([]int64, 0, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		value, isNull := values.GetValue(uint64(row))
		if !isNull && value {
			sels = append(sels, int64(row))
		}
	}
	if len(sels) == bat.RowCount() {
		return nil
	}
	if len(sels) == 0 {
		bat.CleanOnlyData()
		return nil
	}
	bat.Shrink(sels, false)
	return nil
}

func relationVectorTopLimit(param *plan.IndexReaderParam, postFilterTopOnly bool) (int, bool) {
	if param == nil || len(param.OrderBy) == 0 || param.OrderBy[0] == nil {
		return 0, false
	}
	if !postFilterTopOnly && param.OrderBy[0].Expr.GetF() == nil {
		return 0, false
	}
	lit := param.GetLimit().GetLit()
	if lit == nil || lit.Isnull {
		return 0, false
	}
	value, ok := lit.Value.(*plan.Literal_U64Val)
	if !ok || value.U64Val == 0 || value.U64Val > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(value.U64Val), true
}

func resultRowCount(batches []*batch.Batch) int {
	total := 0
	for _, bat := range batches {
		total += bat.RowCount()
	}
	return total
}

type relationTopRow struct {
	batch int
	row   int
	dist  float64
}

func compactRelationTop(res *executor.Result, limit int, desc bool) error {
	if res == nil || limit <= 0 || len(res.Batches) == 0 {
		return nil
	}
	rows := make([]relationTopRow, 0, resultRowCount(res.Batches))
	for batchIdx, bat := range res.Batches {
		if len(bat.Vecs) == 0 {
			continue
		}
		distVec := bat.Vecs[len(bat.Vecs)-1]
		for row := 0; row < bat.RowCount(); row++ {
			if distVec.IsNull(uint64(row)) {
				continue
			}
			rows = append(rows, relationTopRow{
				batch: batchIdx,
				row:   row,
				dist:  vector.GetFixedAtNoTypeCheck[float64](distVec, row),
			})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if desc {
			return rows[i].dist > rows[j].dist
		}
		return rows[i].dist < rows[j].dist
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	first := res.Batches[0]
	out := batch.NewWithSize(len(first.Vecs))
	out.Attrs = append(out.Attrs, first.Attrs...)
	for i, src := range first.Vecs {
		out.Vecs[i] = vector.NewVec(*src.GetType())
	}
	for _, selected := range rows {
		src := res.Batches[selected.batch]
		for col, vec := range src.Vecs {
			// Vector top pushdown deliberately clears the entry vector after
			// consuming it. Preserve that empty slot; scanEntries removes it.
			if vec.Length() == 0 && src.RowCount() != 0 {
				continue
			}
			if err := out.Vecs[col].UnionOne(vec, int64(selected.row), res.Mp); err != nil {
				out.Clean(res.Mp)
				return err
			}
		}
	}
	out.SetRowCount(len(rows))
	for _, bat := range res.Batches {
		bat.Clean(res.Mp)
	}
	res.Batches = []*batch.Batch{out}
	return nil
}

func makeRelationScanBatch(tableDef *plan.TableDef, columns []string) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(columns))
	bat.Attrs = append(bat.Attrs, columns...)
	for i, name := range columns {
		pos, ok := tableDef.Name2ColIndex[name]
		if !ok {
			pos, ok = tableDef.Name2ColIndex[strings.ToLower(name)]
		}
		if !ok || int(pos) >= len(tableDef.Cols) {
			bat.Clean(nil)
			return nil, moerr.NewInternalErrorNoCtxf("ivfflat hidden column %q not found in %s", name, tableDef.Name)
		}
		colType := tableDef.Cols[pos].Typ
		bat.Vecs[i] = vector.NewVec(types.New(types.T(colType.Id), colType.Width, colType.Scale))
	}
	return bat, nil
}
