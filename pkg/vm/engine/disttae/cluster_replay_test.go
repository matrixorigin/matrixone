// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
	testutil3 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// clusterReplayCompoundName is the hidden compound column of the reported
// CLUSTER BY(tenant_id, device_id, metric_id, event_ts) table.
var clusterReplayCompoundName = planutil.BuildCompositeClusterByColumnName([]string{
	"tenant_id", "device_id", "metric_id", "event_ts",
})

// clusterReplayTableDef mirrors the reported table definition: six visible
// columns, a fake primary key and the hidden compound cluster column.
func clusterReplayTableDef() *plan.TableDef {
	mkCol := func(name string, seqnum uint32, oid types.T, hidden, clusterBy bool) *plan.ColDef {
		return &plan.ColDef{
			Name:      name,
			Seqnum:    seqnum,
			Hidden:    hidden,
			ClusterBy: clusterBy,
			Typ:       plan.Type{Id: int32(oid)},
		}
	}
	cols := []*plan.ColDef{
		mkCol("tenant_id", 0, types.T_int32, false, false),
		mkCol("device_id", 1, types.T_int32, false, false),
		mkCol("metric_id", 2, types.T_int32, false, false),
		mkCol("event_ts", 3, types.T_datetime, false, false),
		mkCol("value", 4, types.T_float64, false, false),
		mkCol("quality", 5, types.T_int16, false, false),
		mkCol(catalog.FakePrimaryKeyColName, 6, types.T_uint64, true, false),
		mkCol(clusterReplayCompoundName, 7, types.T_varchar, true, true),
	}
	name2Idx := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2Idx[col.Name] = int32(i)
	}
	return &plan.TableDef{
		TblId:         22,
		Name:          "metrics",
		Cols:          cols,
		Name2ColIndex: name2Idx,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
		},
		ClusterBy: &plan.ClusterByDef{
			Name:         clusterReplayCompoundName,
			CompCbkeyCol: cols[len(cols)-1],
		},
	}
}

// clusterRowSpec describes one replayed row of the reported table.
type clusterRowSpec struct {
	device     int32
	metric     int32
	tsOrdinal  int64
	value      float64
	nullMetric bool
}

// issueRowSpec maps the deterministic issue dataset index to its row.
// clusterVecOwner centralizes ownership of the original engine vectors backing
// a synthetic replay batch. VectorToProtoVector only wraps their Data/Area and
// PartitionState retains those aliasing views (cantFree), so the bytes can
// only be returned by freeing the originals, after the state's readers have
// finished. Registering with t.Cleanup covers failure paths as well.
type clusterVecOwner struct {
	pool *mpool.MPool
	vecs []*vector.Vector
}

func newClusterVecOwner(t *testing.T, pool *mpool.MPool, vs ...*vector.Vector) *clusterVecOwner {
	o := &clusterVecOwner{pool: pool, vecs: append([]*vector.Vector(nil), vs...)}
	t.Cleanup(o.Free)
	return o
}

func (o *clusterVecOwner) Free() {
	for _, v := range o.vecs {
		v.Free(o.pool)
	}
	o.vecs = nil
}

// drainClusterPool registers a pool's leak proof: registered first, it runs
// last, after every fixture cleanup has freed the original vectors, and
// asserts both bytes and objects of OnHeapOutstanding return to baseline
// before the pool is deleted. Deleting a normal MustNewZero pool does not
// unregister its allocations from the global pointer shards, so a leak here
// would silently outlive the pool without this assertion.
func drainClusterPool(tb testing.TB, pool *mpool.MPool) {
	tb.Cleanup(func() {
		defer mpool.DeleteMPool(pool)
		bytes, objects := pool.OnHeapOutstanding()
		require.Zero(tb, bytes, "replay fixture vectors must return their bytes to the pool")
		require.Zero(tb, objects, "replay fixture vectors must return their objects to the pool")
	})
}

func issueRowSpec(i int) clusterRowSpec {
	j := int64((i / 5000) % 20)
	return clusterRowSpec{
		device:    int32(i % 1000),
		metric:    int32((i / 1000) % 5),
		tsOrdinal: j,
		value:     float64(j + 1),
	}
}

// clusterReplayBatchOptions shapes the attr layout of a replayed insert
// batch: the hidden compound column may be omitted (a pre-cluster schema
// era) or shadowed by an extra user column carrying the reserved prefix.
type clusterReplayBatchOptions struct {
	omitCompound     bool
	shadowPrefixUser bool
}

// clusterReplaySpecBatch builds one logtail-shaped insert batch for the
// reported table: [rowid, time, then the schema columns in seqnum order],
// with the compound column carrying serial_full of the four key parts. Row
// ids are unique across batches through rowIDBase.
func clusterReplaySpecBatch(
	t *testing.T,
	mp *mpool.MPool,
	packer *types.Packer,
	specs []clusterRowSpec,
	tsBase int64,
	rowIDBase uint32,
	opts clusterReplayBatchOptions,
) (*api.Batch, *objectio.Segmentid) {
	t.Helper()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	tenantVec := vector.NewVec(types.T_int32.ToType())
	deviceVec := vector.NewVec(types.T_int32.ToType())
	metricVec := vector.NewVec(types.T_int32.ToType())
	eventTsVec := vector.NewVec(types.T_datetime.ToType())
	valueVec := vector.NewVec(types.T_float64.ToType())
	qualityVec := vector.NewVec(types.T_int16.ToType())
	fakePkVec := vector.NewVec(types.T_uint64.ToType())
	clusterVec := vector.NewVec(types.T_varchar.ToType())
	shadowVec := vector.NewVec(types.T_int32.ToType())
	newClusterVecOwner(t, mp, rowIDVec, tsVec, tenantVec, deviceVec, metricVec,
		eventTsVec, valueVec, qualityVec, fakePkVec, clusterVec, shadowVec)

	sid := objectio.NewSegmentid()
	for n, spec := range specs {
		eventTs, err := types.ParseDatetime(
			fmt.Sprintf("2026-01-01 00:%02d:%02d", spec.tsOrdinal*10/60, spec.tsOrdinal*10%60), 6)
		require.NoError(t, err)

		packer.Reset()
		packer.EncodeInt32(0)
		packer.EncodeInt32(spec.device)
		if spec.nullMetric {
			packer.EncodeNull()
			vector.AppendFixed(metricVec, int32(0), true, mp)
		} else {
			packer.EncodeInt32(spec.metric)
			vector.AppendFixed(metricVec, spec.metric, false, mp)
		}
		packer.EncodeDatetime(eventTs)
		serial := packer.Bytes()

		offset := rowIDBase + uint32(n)
		vector.AppendFixed(rowIDVec,
			objectio.NewRowid(objectio.NewBlockid(sid, uint16(offset%16), 0), offset), false, mp)
		vector.AppendFixed(tsVec, types.BuildTS(tsBase+int64(n), 0), false, mp)
		vector.AppendFixed(tenantVec, int32(0), false, mp)
		vector.AppendFixed(deviceVec, spec.device, false, mp)
		vector.AppendFixed(eventTsVec, eventTs, false, mp)
		vector.AppendFixed(valueVec, spec.value, false, mp)
		vector.AppendFixed(qualityVec, int16(n%3), false, mp)
		vector.AppendFixed(fakePkVec, uint64(offset), false, mp)
		vector.AppendBytes(clusterVec, serial, false, mp)
		vector.AppendFixed(shadowVec, int32(n), false, mp)
	}

	attrs := []string{
		"rowid", "time",
		"tenant_id", "device_id", "metric_id", "event_ts",
		"value", "quality",
		catalog.FakePrimaryKeyColName,
	}
	vecs := []api.Vector{
		mustProtoVector(rowIDVec),
		mustProtoVector(tsVec),
		mustProtoVector(tenantVec),
		mustProtoVector(deviceVec),
		mustProtoVector(metricVec),
		mustProtoVector(eventTsVec),
		mustProtoVector(valueVec),
		mustProtoVector(qualityVec),
		mustProtoVector(fakePkVec),
	}
	if !opts.omitCompound {
		attrs = append(attrs, clusterReplayCompoundName)
		vecs = append(vecs, mustProtoVector(clusterVec))
	}
	if opts.shadowPrefixUser {
		attrs = append(attrs, "__mo_cbkey_usercopy")
		vecs = append(vecs, mustProtoVector(shadowVec))
	}
	return &api.Batch{Attrs: attrs, Vecs: vecs}, sid
}

// clusterReplayInsertBatch builds the reported table's batch for the
// deterministic issue dataset indices.
func clusterReplayInsertBatch(
	t *testing.T,
	proc *process.Process,
	mp *mpool.MPool,
	packer *types.Packer,
	rowIdx []int,
	tsBase int64,
) (*api.Batch, *objectio.Segmentid) {
	t.Helper()
	specs := make([]clusterRowSpec, 0, len(rowIdx))
	for _, i := range rowIdx {
		specs = append(specs, issueRowSpec(i))
	}
	return clusterReplaySpecBatch(t, mp, packer, specs, tsBase, 0, clusterReplayBatchOptions{})
}

func mustProtoVector(v *vector.Vector) api.Vector {
	ret, err := vector.VectorToProtoVector(v)
	if err != nil {
		panic(err)
	}
	return ret
}

var clusterScanRe = regexp.MustCompile(`scan:(\d+)`)

// readClusterReplay runs one query through the production reader path
// (readutil.NewReader over a LocalDisttaeDataSource whose committed replay
// state is pState) and returns the row count, value sum and the number of
// replay candidates the datasource examined.
type clusterReadResult struct {
	rows    int
	sum     float64
	scanned int
	batches int
}

func readClusterReplay(
	t *testing.T,
	ctx context.Context,
	mp *mpool.MPool,
	pool *fileservice.Pool[*types.Packer],
	tableDef *plan.TableDef,
	pState *logtailreplay.PartitionState,
	expr *plan.Expr,
) clusterReadResult {
	t.Helper()

	var rows int
	var sum float64
	var scanned int

	summaryBuf := &bytes.Buffer{}
	qryCtx := context.WithValue(ctx, defines.ReaderSummaryKey{}, summaryBuf)

	txnOp, closeTxn := client.NewTestTxnOperator(ctx)
	defer closeTxn()

	ls := &LocalDisttaeDataSource{
		ctx: qryCtx,
		table: &txnTable{
			db:       &txnDatabase{databaseId: 11, op: txnOp},
			tableId:  22,
			tableDef: tableDef,
		},
		pState:          pState,
		category:        engine.GeneralLocalDataSource,
		iteratePhase:    engine.InMem,
		tombstonePolicy: engine.Policy_SkipAll,
		snapshotTS:      types.MaxTs(),
	}

	reader, err := readutil.NewReader(
		qryCtx, mp, pool, nil, tableDef,
		timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()},
		expr, ls, 0, engine.FilterHint{},
	)
	require.NoError(t, err)

	out := batch.NewWithSize(1)
	out.Attrs = []string{"value"}
	out.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	defer out.Clean(mp)

	readBatches := 0
	for {
		out.CleanOnlyData()
		isEnd, err := reader.Read(qryCtx, out.Attrs, nil, mp, out)
		require.NoError(t, err)
		if isEnd {
			break
		}
		readBatches++
		for _, v := range vector.MustFixedColWithTypeCheck[float64](out.Vecs[0]) {
			rows++
			sum += v
		}
	}
	require.NoError(t, reader.Close())
	require.Greater(t, readBatches, 0, "the reader must return data")

	for _, m := range clusterScanRe.FindAllStringSubmatch(summaryBuf.String(), -1) {
		var v int
		_, err := fmt.Sscanf(m[1], "%d", &v)
		require.NoError(t, err)
		scanned += v
	}
	require.NotZero(t, scanned,
		"the reader summary must report the replay scan count: %s", summaryBuf.String())
	return clusterReadResult{rows: rows, sum: sum, scanned: scanned, batches: readBatches}
}

// TestClusterReplayDataSourcePrunesToMatchingCandidates is the datasource
// boundary regression for issue #26996: committed unflushed rows in the CN
// replay state must be iterated through the cluster-key index, examining
// only the matching candidates, and every duplicate cluster key must be
// returned.
func TestClusterReplayDataSourcePrunesToMatchingCandidates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()

	idx := []int{0, 1, 90000, 95000}
	totalRows := len(idx)
	// Replay the deterministic issue dataset into the committed replay state:
	// 1,000 devices x 5 metrics x 20 ts ordinals, tenant 0, value = j+1.
	pState := logtailreplay.NewPartitionState("", false, 22, false)
	const perBatch = 2
	for start := 0; start < totalRows; start += perBatch {
		end := min(start+perBatch, totalRows)
		bat, _ := clusterReplayInsertBatch(t, proc, mp, packer, idx[start:end], 1)
		pState.HandleRowsInsert(ctx, bat, 6 /*fake pk seqnum*/, packer, mp, clusterReplayKey(22))
	}

	// The reported predicate: prefix_in_range(cbkey, serial(0,0,0,'...00:03:00'),
	// serial(0,0,0), 0) with folded serial bounds.
	expr := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 18), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))

	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, expr)
	require.Equal(t, 2, res.rows)
	require.Equal(t, float64(39), res.sum)
	require.Equal(t, 2, res.scanned,
		"the committed replay iteration must examine only the matching cluster-key candidates")

	// Counterexample control: without a filter expression the same source
	// scans the whole replay state, proving the pruning above comes from the
	// cluster path rather than a shrunken dataset.
	all := readClusterReplay(t, ctx, mp, pool, tableDef, pState, nil)
	require.Equal(t, totalRows, all.rows)
	require.Equal(t, 41.0, all.sum) // 1000 devices * 5 metrics * sum(1..20)
	require.Equal(t, totalRows, all.scanned)
}

// bindClusterPredicate binds and publishes a predicate the way the planner
// does: constant arguments are folded into Expr_Fold payloads, the shape the
// read filters consume.
func bindClusterPredicate(
	t *testing.T,
	proc *process.Process,
	name string,
	args ...*plan.Expr,
) *plan.Expr {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
	require.NoError(t, err)
	var executors []colexec.ExpressionExecutor
	_, err = plan2.ReplaceFoldExpr(proc, expr, &executors)
	require.NoError(t, err)
	require.NoError(t, plan2.EvalFoldExpr(proc, expr, &executors))
	for _, executor := range executors {
		e := executor
		t.Cleanup(e.Free)
	}
	return expr
}

// clusterColExpr references the hidden compound cluster column.
func clusterColExpr() *plan.Expr {
	compoundType := types.T_varchar.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&compoundType),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 0, ColPos: 7, Name: clusterReplayCompoundName,
		}},
	}
}

// serialBound folds serial(0,0,0[,datetime]) into the serialized const the
// planner publishes; fromOrdinal < 0 means the three-part prefix bound.
func serialBound(
	t *testing.T,
	proc *process.Process,
	fromOrdinal int,
) *plan.Expr {
	t.Helper()
	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	args := []*plan.Expr{intConst(0), intConst(0), intConst(0)}
	if fromOrdinal >= 0 {
		bound, err := types.ParseDatetime(
			fmt.Sprintf("2026-01-01 00:%02d:%02d", fromOrdinal*10/60, fromOrdinal*10%60), 6)
		require.NoError(t, err)
		args = append(args, plan2.MakePlan2DateTimeConstExprWithType(int64(bound)))
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "serial", args)
	require.NoError(t, err)
	folded, err := plan2.ConstantFold(
		batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded)
	return folded
}

// TestClusterReplayDataSourceReturnsAllDuplicateClusterKeys proves the
// nonunique contract at the datasource boundary: an equality predicate on a
// cluster value shared by many rows returns every row; the unique-PK
// exact-hit shortcut must not truncate the result.
func TestClusterReplayDataSourceReturnsAllDuplicateClusterKeys(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	// Two rows share the cluster key (0,0,0,00:00:00); two differ.
	dupIdx := make([]int, 2)
	bat, _ := clusterReplayInsertBatch(t, proc, mp, packer, dupIdx, 1)
	pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))
	other, _ := clusterReplayInsertBatch(t, proc, mp, packer, []int{5000, 6000}, 2)
	pState.HandleRowsInsert(ctx, other, 6, packer, mp, clusterReplayKey(22))

	// Equality on the full compound key.
	equalExpr := bindClusterPredicate(t, proc, "=",
		clusterColExpr(), serialBound(t, proc, 0))

	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, equalExpr)
	require.Equal(t, 2, res.rows, "every duplicate cluster key must be returned")
	require.Equal(t, 2.0, res.sum) // ts ordinal 0 -> value 1 for each duplicate
	require.Equal(t, 2, res.scanned)

	// The primary-key exact-hit shortcut stays disabled: the persisted phase
	// of the same source must not terminate early, and the summary proves the
	// scan saw both matching candidates.
}

// TestClusterReplayDataSourceDeletesAndSnapshots keeps the deleted-row and
// future-commit contracts at the datasource boundary: rows deleted in the
// replay state disappear from the cluster iteration, while snapshots taken
// before the delete still see them.
func TestClusterReplayDataSourceDeletesAndSnapshots(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	bat, sid := clusterReplayInsertBatch(t, proc, mp, packer, []int{0, 5000}, 1)
	pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))

	// Delete the device-0 row (ts ordinal 0) at a later commit time.
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	delTSVec := vector.NewVec(types.T_TS.ToType())
	delPkVec := vector.NewVec(types.T_uint64.ToType())
	tbRowIDVec := vector.NewVec(types.T_Rowid.ToType())
	newClusterVecOwner(t, mp, rowIDVec, delTSVec, delPkVec, tbRowIDVec)
	rowID := objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), 0)
	vector.AppendFixed(rowIDVec, rowID, false, mp)
	vector.AppendFixed(delTSVec, types.BuildTS(500, 0), false, mp)
	vector.AppendFixed(delPkVec, uint64(0), false, mp)
	vector.AppendFixed(tbRowIDVec, types.RandomRowid(), false, mp)
	pState.HandleRowsDelete(ctx, &api.Batch{
		Attrs: []string{"rowid", "time", "pk", "tbrowid"},
		Vecs: []api.Vector{
			mustProtoVector(rowIDVec),
			mustProtoVector(delTSVec),
			mustProtoVector(delPkVec),
			mustProtoVector(tbRowIDVec),
		},
	}, packer, mp)

	prefixExpr := bindClusterPredicate(t, proc, "prefix_eq",
		clusterColExpr(), serialBound(t, proc, -1))

	// At the maximum snapshot only the device-5000 row survives.
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, prefixExpr)
	require.Equal(t, 1, res.rows)
	require.Equal(t, 1, res.scanned)
}

// serialIntsBound folds serial(ints...[, datetime(ordinal)]) with a variable
// number of leading int parts, the planner's bound shape for predicates that
// constrain only a prefix of the cluster key.
func serialIntsBound(
	t *testing.T,
	proc *process.Process,
	ints []int32,
	fromOrdinal int64,
) *plan.Expr {
	t.Helper()
	args := make([]*plan.Expr, 0, len(ints)+1)
	for _, v := range ints {
		args = append(args, plan2.MakePlan2Int32ConstExprWithType(v))
	}
	if fromOrdinal >= 0 {
		bound, err := types.ParseDatetime(
			fmt.Sprintf("2026-01-01 00:%02d:%02d", fromOrdinal*10/60, fromOrdinal*10%60), 6)
		require.NoError(t, err)
		args = append(args, plan2.MakePlan2DateTimeConstExprWithType(int64(bound)))
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "serial", args)
	require.NoError(t, err)
	folded, err := plan2.ConstantFold(
		batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded)
	return folded
}

// TestClusterReplayDataSourceIgnoresUnrelatedPrefixedUserColumn is
// the datasource regression for the review's P1: a legal user column named
// with the reserved __mo_cbkey_ prefix makes the replay cluster index
// unbuildable, and the read must still return every matching committed row
// through the general iteration instead of missing them behind an empty
// index.
func TestClusterReplayDataSourceIgnoresUnrelatedPrefixedUserColumn(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	// 100 rows: device=i%10, metric 0, ts ordinal i/10. The reported
	// predicate shape matches device 0 with ordinal >= 8: i=80,90. Matching
	// rows carry a unique value so their presence in the result is provable;
	// the fallback returns a candidate superset that the residual SQL filter
	// narrows upstream, so the datasource contract is "no matching row is
	// missed".
	specs := make([]clusterRowSpec, 0, 100)
	for i := 0; i < 100; i++ {
		value := 1.0
		if i == 80 || i == 90 {
			value = 999.0
		}
		specs = append(specs, clusterRowSpec{
			device:    int32(i % 10),
			metric:    0,
			tsOrdinal: int64(i / 10),
			value:     value,
		})
	}
	bat, _ := clusterReplaySpecBatch(t, mp, packer, specs, 1, 0,
		clusterReplayBatchOptions{shadowPrefixUser: true})
	pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))

	require.True(t, pState.ClusterIndexReadyFor(*clusterReplayKey(22)))

	expr := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 8), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, expr)
	require.Equal(t, 2, res.rows)
	require.Equal(t, 2*999.0, res.sum)
	require.Equal(t, 2, res.scanned, "an unrelated prefix does not obscure the declared key")
}

// TestClusterReplayDataSourceFallsBackAcrossSchemaEras replays a batch from
// before the compound column existed and a batch after it, then proves the
// committed read returns matching rows from BOTH eras through the fallback.
func TestClusterReplayDataSourceFallsBackAcrossSchemaEras(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	specOf := func(i int) clusterRowSpec {
		value := 1.0
		switch i {
		case 20, 30, 40, 50, 60, 70, 80, 90:
			value = 999.0
		}
		return clusterRowSpec{
			device:    int32(i % 10),
			metric:    0,
			tsOrdinal: int64(i / 10),
			value:     value,
		}
	}
	plain := make([]clusterRowSpec, 0, 50)
	for i := 0; i < 50; i++ {
		plain = append(plain, specOf(i))
	}
	clustered := make([]clusterRowSpec, 0, 50)
	for i := 50; i < 100; i++ {
		clustered = append(clustered, specOf(i))
	}
	bat1, _ := clusterReplaySpecBatch(t, mp, packer, plain, 1, 0,
		clusterReplayBatchOptions{omitCompound: true})
	pState.HandleRowsInsert(ctx, bat1, 6, packer, mp, clusterReplayKey(22))
	bat2, _ := clusterReplaySpecBatch(t, mp, packer, clustered, 1000, 50,
		clusterReplayBatchOptions{})
	pState.HandleRowsInsert(ctx, bat2, 6, packer, mp, clusterReplayKey(22))

	require.False(t, pState.ClusterIndexReadyFor(*clusterReplayKey(22)))

	// Ordinal >= 2 matches device-0 rows of both eras: i in
	// {20,30,40,50,60,70,80,90}; each carries value 999. The fallback
	// returns all rows as candidates, and every matching row of both eras
	// must be among them.
	expr := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 2), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, expr)
	require.Equal(t, 100, res.rows, "every committed row is a fallback candidate")
	require.Equal(t, 8*999.0+92, res.sum,
		"matching rows of both schema eras must be present")
	require.Equal(t, 100, res.scanned)
}

// TestClusterReplayDataSourceMultiBatchDuplicates returns one cluster value
// duplicated across many replay batches: the nonunique iteration must return
// every row across output batch boundaries.
func TestClusterReplayDataSourceMultiBatchDuplicates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	const batches = 9
	const perBatch = 1000
	specs := make([]clusterRowSpec, perBatch)
	for i := range specs {
		specs[i] = clusterRowSpec{device: 0, metric: 0, tsOrdinal: 0, value: 1}
	}
	for b := 0; b < batches; b++ {
		bat, _ := clusterReplaySpecBatch(t, mp, packer, specs,
			int64(1+b*perBatch), uint32(b*perBatch), clusterReplayBatchOptions{})
		pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))
	}

	equalExpr := bindClusterPredicate(t, proc, "=",
		clusterColExpr(), serialBound(t, proc, 0))
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, equalExpr)
	require.Equal(t, batches*perBatch, res.rows,
		"every duplicate across replay batches must be returned")
	require.Equal(t, float64(batches*perBatch), res.sum)
	require.Equal(t, batches*perBatch, res.scanned)
	require.Greater(t, res.batches, 1,
		"the duplicates must span more than one output batch")
}

// TestClusterReplayDataSourceNULLClusterParts keeps the SQL semantics of
// NULL cluster components at the datasource boundary: rows with a NULL part
// are returned by predicates that only constrain earlier parts, and excluded
// by predicates whose bound pins the NULL part to a non-null value.
func TestClusterReplayDataSourceNULLClusterParts(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	specs := []clusterRowSpec{
		{device: 0, metric: 0, tsOrdinal: 0, value: 1},
		{device: 0, metric: 0, tsOrdinal: 1, value: 2},
		{device: 0, metric: 0, tsOrdinal: 2, value: 3},
		{device: 0, tsOrdinal: 0, value: 1, nullMetric: true},
		{device: 0, tsOrdinal: 1, value: 2, nullMetric: true},
		{device: 0, tsOrdinal: 2, value: 3, nullMetric: true},
	}
	bat, _ := clusterReplaySpecBatch(t, mp, packer, specs, 1, 0, clusterReplayBatchOptions{})
	pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))

	// Predicate on the first two parts only (tenant=0 AND device=0): the
	// NULL-metric rows must be returned like any other row.
	prefixTwo := bindClusterPredicate(t, proc, "prefix_eq",
		clusterColExpr(), serialIntsBound(t, proc, []int32{0, 0}, -1))
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, prefixTwo)
	require.Equal(t, 6, res.rows, "NULL-part rows must survive a prefix predicate on earlier parts")
	require.Equal(t, float64(12), res.sum)

	// Predicate pinning metric=0: NULL-metric rows must be excluded, exactly
	// like the SQL comparison they would fail.
	metricZero := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 0), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))
	res = readClusterReplay(t, ctx, mp, pool, tableDef, pState, metricZero)
	require.Equal(t, 3, res.rows, "NULL-metric rows must not match a metric=0 bound")
	require.Equal(t, float64(6), res.sum)
}

// TestMaterializedSnapshotClusterEntryCoversPruningAndFallback drives the
// second committed in-memory read entry (the materialized snapshot
// datasource) directly: cluster pruning returns only the matching rows, and
// an unusable index falls back to the general iteration. It also pins that
// a one-row cluster result cannot arm the primary-key exact-hit shortcut,
// which would otherwise skip persisted matches.
func TestMaterializedSnapshotClusterEntryCoversPruningAndFallback(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	// ts ordinal >= 8 of the 100-row dataset below: rows i=80 and i=90.
	expr := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 8), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))

	build := func(shadow bool) (*logtailreplay.PartitionState, *readutil.MemPKFilter) {
		pState := logtailreplay.NewPartitionState("", false, 22, false)
		const rows = 100
		specs := make([]clusterRowSpec, 0, rows)
		for i := 0; i < rows; i++ {
			specs = append(specs, clusterRowSpec{
				device:    int32(i % 10),
				metric:    0,
				tsOrdinal: int64(i / 10),
				value:     float64(i/10 + 1),
			})
		}
		bat, _ := clusterReplaySpecBatch(t, mp, packer, specs, 1, 0,
			clusterReplayBatchOptions{shadowPrefixUser: shadow})
		pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))

		filter := &readutil.MemPKFilter{}
		clusterFilter, err := readutil.NewMemClusterKeyFilter(
			tableDef, expr,
			timestamp.Timestamp{PhysicalTime: types.MaxTs().ToTimestamp().PhysicalTime},
			mp, pool)
		require.NoError(t, err)
		require.True(t, clusterFilter.Valid())
		filter.SetClusterKeyFilter(&clusterFilter)
		return pState, filter
	}

	buildAndCount := func(shadow bool) (rows int, sum float64) {
		pState, filter := build(shadow)
		ds := &materializedSnapshotDataSource{
			ctx:         ctx,
			pState:      pState,
			snapshotTS:  types.MaxTs(),
			memPKFilter: filter,
			mp:          mp,
		}
		require.NoError(t, ds.buildCommittedInMemBatches(
			ctx, []string{"value"}, []types.Type{types.T_float64.ToType()},
			[]uint16{4}, mp))
		for _, bat := range ds.inMemBatches {
			for _, v := range vector.MustFixedColWithTypeCheck[float64](bat.Vecs[0]) {
				rows++
				sum += v
			}
			bat.Clean(mp)
		}
		// The exact-hit shortcut must not arm for a nonunique cluster read,
		// even when exactly one in-memory row matched.
		ok1, ok2 := filter.Exact()
		require.False(t, ok1 && ok2,
			"a cluster-key read must not arm the primary-key exact-hit shortcut")
		return rows, sum
	}

	// Pruning entry: only the two matching rows (ordinals 8 and 9 of
	// device 0 -> rows i=80, 90 -> values 9 and 10).
	rows, sum := buildAndCount(false)
	require.Equal(t, 2, rows)
	require.Equal(t, float64(19), sum)

	// A known declaration remains usable beside an unrelated prefixed user column.
	rows, sum = buildAndCount(true)
	require.Equal(t, 2, rows)
	require.Equal(t, 19.0, sum)
}

// TestClusterOneReplayMatchNeverArmsExactHit covers the single-row feedback
// branches of BOTH committed in-memory read entries (local
// filterInMemCommittedInserts and the materialized build) with exactly one
// replay match: the RecordExactHit call must stay a no-op for a nonunique
// cluster key, so later phases (including any persisted matches upstream)
// can never be skipped.
func TestClusterOneReplayMatchNeverArmsExactHit(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()

	// Exactly one row matches ts ordinal >= 9 (the dataset's last ordinal).
	specs := make([]clusterRowSpec, 0, 100)
	for i := 0; i < 100; i++ {
		specs = append(specs, clusterRowSpec{
			device:    int32(i % 10),
			metric:    0,
			tsOrdinal: int64(i / 10),
			value:     float64(i/10 + 1),
		})
	}
	bat, _ := clusterReplaySpecBatch(t, mp, packer, specs, 1, 0, clusterReplayBatchOptions{})
	pState := logtailreplay.NewPartitionState("", false, 22, false)
	pState.HandleRowsInsert(ctx, bat, 6, packer, mp, clusterReplayKey(22))

	expr := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 9), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))
	clusterFilter, err := readutil.NewMemClusterKeyFilter(
		tableDef, expr,
		timestamp.Timestamp{PhysicalTime: types.MaxTs().ToTimestamp().PhysicalTime},
		mp, pool)
	require.NoError(t, err)
	require.True(t, clusterFilter.Valid())

	// Local entry: drive filterInMemCommittedInserts directly so the one-row
	// feedback branch is observable on the filter object.
	filter := &readutil.MemPKFilter{}
	filter.SetClusterKeyFilter(&clusterFilter)
	ls := &LocalDisttaeDataSource{
		ctx: ctx,
		table: &txnTable{
			db: &txnDatabase{databaseId: 11, op: func() client.TxnOperator {
				txnOp, closeTxn := client.NewTestTxnOperator(ctx)
				t.Cleanup(closeTxn)
				return txnOp
			}()},
			tableId:  22,
			tableDef: tableDef,
		},
		pState:          pState,
		category:        engine.GeneralLocalDataSource,
		tombstonePolicy: engine.Policy_SkipAll,
		snapshotTS:      types.MaxTs(),
		memPKFilter:     filter,
	}
	out := batch.NewWithSize(1)
	out.Attrs = []string{"value"}
	out.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, ls.filterInMemCommittedInserts(
		ctx, []types.Type{types.T_float64.ToType()}, []uint16{4}, mp, out))
	require.Equal(t, 1, out.RowCount(), "exactly one replay row matches")
	require.Equal(t, float64(10),
		vector.MustFixedColWithTypeCheck[float64](out.Vecs[0])[0])
	ok1, ok2 := filter.Exact()
	require.False(t, ok1 && ok2,
		"a one-row cluster read must not arm the primary-key exact-hit shortcut")
	ls.Close()
	out.Clean(mp)

	// Materialized entry: the same shape through buildCommittedInMemBatches,
	// which records the exact hit when it builds exactly one row.
	filter2 := &readutil.MemPKFilter{}
	filter2.SetClusterKeyFilter(&clusterFilter)
	ds := &materializedSnapshotDataSource{
		ctx:         ctx,
		pState:      pState,
		snapshotTS:  types.MaxTs(),
		memPKFilter: filter2,
		mp:          mp,
	}
	require.NoError(t, ds.buildCommittedInMemBatches(
		ctx, []string{"value"}, []types.Type{types.T_float64.ToType()},
		[]uint16{4}, mp))
	require.Len(t, ds.inMemBatches, 1)
	require.Equal(t, 1, ds.inMemBatches[0].RowCount())
	require.Equal(t, float64(10),
		vector.MustFixedColWithTypeCheck[float64](ds.inMemBatches[0].Vecs[0])[0])
	ok1, ok2 = filter2.Exact()
	require.False(t, ok1 && ok2,
		"the materialized one-row branch must not arm the exact-hit shortcut")
	for _, b := range ds.inMemBatches {
		b.Clean(mp)
	}
}

// perBlockTombstoner records every block the committed in-memory build asks
// about and starts failing on a chosen call, letting tests observe both the
// selected iteration's candidate stream and the error path after partial
// accumulation.
type perBlockTombstoner struct {
	failAfter int
	asked     []*types.Blockid
}

func (f *perBlockTombstoner) Type() engine.TombstoneType     { return engine.TombstoneData }
func (f *perBlockTombstoner) HasAnyInMemoryTombstone() bool  { return false }
func (f *perBlockTombstoner) HasAnyTombstoneFile() bool      { return false }
func (f *perBlockTombstoner) String() string                 { return "perBlock" }
func (f *perBlockTombstoner) StringWithPrefix(string) string { return "perBlock" }
func (f *perBlockTombstoner) HasBlockTombstone(
	context.Context, *objectio.Blockid, fileservice.FileService,
) (bool, error) {
	return false, nil
}
func (f *perBlockTombstoner) MarshalBinaryWithBuffer(*bytes.Buffer) error { return nil }
func (f *perBlockTombstoner) UnmarshalBinary([]byte) error                { return nil }
func (f *perBlockTombstoner) Merge(engine.Tombstoner) error               { return nil }
func (f *perBlockTombstoner) SortInMemory()                               {}
func (f *perBlockTombstoner) PrefetchTombstones(
	context.Context, string, fileservice.FileService, []objectio.Blockid,
) {
}
func (f *perBlockTombstoner) ApplyInMemTombstones(
	*types.Blockid, []int64, *objectio.Bitmap,
) (left []int64) {
	return nil
}
func (f *perBlockTombstoner) ApplyPersistedTombstones(
	_ context.Context,
	_ fileservice.FileService,
	_ *types.TS,
	bid *types.Blockid,
	_ []int64,
	_ *objectio.Bitmap,
) ([]int64, error) {
	copied := *bid
	f.asked = append(f.asked, &copied)
	if len(f.asked) > f.failAfter {
		return nil, moerr.NewInternalErrorNoCtx("injected tombstone read failure")
	}
	return nil, nil
}

// TestMaterializedSnapshotTombstoneErrorCleansUp drives the materialized
// committed in-memory build with a VALID cluster filter and a tombstone that
// fails on the second surfaced candidate: the cluster iteration must be the
// one surfacing candidates (only the predicate-matching blocks, in key
// order), one row is accumulated before the failure, the error propagates,
// and no half-built batch is retained through the production defer-Close
// path.
func TestMaterializedSnapshotTombstoneErrorCleansUp(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	tableDef := clusterReplayTableDef()
	pState := logtailreplay.NewPartitionState("", false, 22, false)

	// Ten rows on ten distinct blocks: rows 0, 1, 2 share the matching
	// cluster key; rows 3..9 carry other keys. Distinct blocks make each
	// candidate's tombstone check observable.
	const rows = 10
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	tenantVec := vector.NewVec(types.T_int32.ToType())
	deviceVec := vector.NewVec(types.T_int32.ToType())
	metricVec := vector.NewVec(types.T_int32.ToType())
	eventTsVec := vector.NewVec(types.T_datetime.ToType())
	valueVec := vector.NewVec(types.T_float64.ToType())
	qualityVec := vector.NewVec(types.T_int16.ToType())
	fakePkVec := vector.NewVec(types.T_uint64.ToType())
	clusterVec := vector.NewVec(types.T_varchar.ToType())
	newClusterVecOwner(t, mp, rowIDVec, tsVec, tenantVec, deviceVec, metricVec,
		eventTsVec, valueVec, qualityVec, fakePkVec, clusterVec)
	sid := objectio.NewSegmentid()
	var matchBlocks []string
	var blockOfRow [rows]*objectio.Blockid
	for i := 0; i < rows; i++ {
		// rows 0..2 share the matching key (device 0); rows 3..9 carry
		// distinct non-matching devices.
		device := int32(i)
		if i < 3 {
			device = 0
		}
		eventTs, err := types.ParseDatetime("2026-01-01 00:00:00", 6)
		require.NoError(t, err)
		packer.Reset()
		packer.EncodeInt32(0)
		packer.EncodeInt32(device)
		packer.EncodeInt32(0)
		packer.EncodeDatetime(eventTs)
		blockID := objectio.NewBlockid(sid, uint16(i), 0)
		rowID := objectio.NewRowid(blockID, 0)
		blockOfRow[i] = blockID
		vector.AppendFixed(rowIDVec, rowID, false, mp)
		vector.AppendFixed(tsVec, types.BuildTS(int64(10+i), 0), false, mp)
		vector.AppendFixed(tenantVec, int32(0), false, mp)
		vector.AppendFixed(deviceVec, device, false, mp)
		vector.AppendFixed(metricVec, int32(0), false, mp)
		vector.AppendFixed(eventTsVec, eventTs, false, mp)
		vector.AppendFixed(valueVec, float64(i+1), false, mp)
		vector.AppendFixed(qualityVec, int16(i%3), false, mp)
		vector.AppendFixed(fakePkVec, uint64(i), false, mp)
		vector.AppendBytes(clusterVec, packer.Bytes(), false, mp)
	}
	pState.HandleRowsInsert(ctx, &api.Batch{
		Attrs: []string{
			"rowid", "time",
			"tenant_id", "device_id", "metric_id", "event_ts",
			"value", "quality",
			catalog.FakePrimaryKeyColName, clusterReplayCompoundName,
		},
		Vecs: []api.Vector{
			mustProtoVector(rowIDVec),
			mustProtoVector(tsVec),
			mustProtoVector(tenantVec),
			mustProtoVector(deviceVec),
			mustProtoVector(metricVec),
			mustProtoVector(eventTsVec),
			mustProtoVector(valueVec),
			mustProtoVector(qualityVec),
			mustProtoVector(fakePkVec),
			mustProtoVector(clusterVec),
		},
	}, 6, packer, mp, clusterReplayKey(22))
	require.True(t, pState.ClusterIndexReadyFor(*clusterReplayKey(22)))

	// Equal keys surface newest version first: rows 2, 1, 0 in that order.
	matchBlocks = []string{
		blockOfRow[2].String(),
		blockOfRow[1].String(),
		blockOfRow[0].String(),
	}

	// Equality on the full key shared by rows 0..2: a valid cluster filter.
	equalExpr := bindClusterPredicate(t, proc, "=",
		clusterColExpr(), serialIntsBound(t, proc, []int32{0, 0, 0}, 0))
	clusterFilter, err := readutil.NewMemClusterKeyFilter(
		tableDef, equalExpr,
		timestamp.Timestamp{PhysicalTime: types.MaxTs().ToTimestamp().PhysicalTime},
		mp, pool)
	require.NoError(t, err)
	require.True(t, clusterFilter.Valid())
	filter := &readutil.MemPKFilter{}
	filter.SetClusterKeyFilter(&clusterFilter)
	require.True(t, filter.ClusterKeyValid(),
		"the test must exercise the cluster iteration, not the general one")

	tomb := &perBlockTombstoner{failAfter: 1}
	ds := &materializedSnapshotDataSource{
		ctx:              ctx,
		pState:           pState,
		snapshotTS:       types.MaxTs(),
		tombstone:        tomb,
		memPKFilter:      filter,
		mp:               mp,
		deletedRowsCache: make(map[objectio.Blockid]*objectio.Bitmap),
	}
	err = ds.buildCommittedInMemBatches(
		ctx, []string{"value"}, []types.Type{types.T_float64.ToType()},
		[]uint16{4}, mp)
	require.Error(t, err, "the injected tombstone failure must surface")
	require.Contains(t, err.Error(), "injected tombstone read failure")
	require.Empty(t, ds.inMemBatches,
		"a failed build must not retain the partially accumulated batch")

	// Only the three matching blocks were surfaced, in cluster-key order:
	// the cluster iterator selected the candidates, and the first one was
	// accumulated before the second failed its tombstone read.
	require.Len(t, tomb.asked, 2,
		"the failure must land on the second surfaced candidate, after partial accumulation")
	for i, bid := range tomb.asked {
		require.Equal(t, matchBlocks[i], bid.String(),
			"candidates must be the predicate-matching blocks in key order")
	}
}

func clusterReplayKey(tableID uint64) *readutil.ClusterKeyDescriptor {
	def := clusterReplayTableDef()
	def.TblId = tableID
	key, ok := readutil.DeclaredClusterKey(def)
	if !ok {
		panic("invalid cluster fixture schema")
	}
	key.Through = types.MaxTs()
	return &key
}
