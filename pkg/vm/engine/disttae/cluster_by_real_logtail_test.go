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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	testutil3 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	taeCatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taeCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	taeContainers "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	taeDBTestutil "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	taeLogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	taeTestutils "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	taeConfig "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

// TestClusterByRealLogtailIntegration is the real-TAE integration regression
// for issue #26996. It goes beyond the handcrafted-batch unit suites by
// driving a genuine TN (TAE) composite CLUSTER BY table through the
// production logtail producer (logtail.HandleSyncLogTailReq), the
// production CN consumer (PartitionState.HandleLogtailEntry) and the
// production reader path (readutil.NewReader over LocalDisttaeDataSource),
// including real persisted objects and real flush transitions:
//
//   - phase A: 100,000 deterministic committed-but-unflushed rows arrive via
//     real TAE-produced logtail; the reported predicate returns count=2 and
//     sum=39 while the committed replay iteration examines only the two
//     matching candidates (scan=2). A nil-filter control proves the full
//     100,000 rows are still present in replay state.
//   - buffer ownership: every applied entry is decoded through a
//     marshal/unmarshal round-trip first, exactly as the transport-decoded
//     messages the production CN consumes, so the retained replay state owns
//     its bytes independently of the producer's pooled response buffers. A
//     churn phase then recycles those pools through a second table's
//     appends, flush and repeated tail pulls, and the golden read repeats
//     unchanged, proving retained replay data survives producer-buffer reuse.
//   - phase B: mo_ctl('dn','flush') semantics (db.FlushTable) move the data
//     into real persisted objects through the real sorted cluster flush; the
//     follow-up tail transfers every replay row out of the replay state, the
//     cluster index carries no stale entries, and the production block
//     filter (readutil.TryFastFilterBlocksWithZone) prunes the real objects
//     from 13 blocks to exactly the one block holding the (0,0,0)-prefixed
//     keys, matching the issue's one-block control experiment.
//   - phase C: after the flush, a fresh committed-but-unflushed batch lands
//     in replay state only; one datasource pass composes the pruned real
//     persisted matches with the new replay matches, with the replay layer
//     examining only the three new candidates.
//
// Fixture hygiene is asserted too: every constructed TAE append batch is
// closed after its append (with a construction-time cleanup backstop), and
// the pooled fixture allocations are verified to return the TAE allocator
// to its pre-fixture baseline once everything is torn down.
//
// The reader boundary returns pruned blocks' rows and pruned replay
// candidates; the upper SQL layer applies the residual predicate in
// production, and the harness mirrors that residual evaluation explicitly.
func TestClusterByRealLogtailIntegration(t *testing.T) {
	defer taeTestutils.AfterTest(t)()
	taeTestutils.EnsureNoLeak(t)
	ctx := context.Background()

	tableDef := clusterReplayTableDef()

	// ------------------------------------------------------------------
	// Boot a real TAE engine: real catalog, real logtail manager and a
	// real local file service holding real persisted objects.
	// ------------------------------------------------------------------
	opts := taeConfig.WithLongScanAndCKPOpts(nil)
	tae := taeDBTestutil.NewTestEngine(ctx, "TestClusterByRealLogtailIntegration", t, opts)
	defer func() { require.NoError(t, tae.Close()) }()
	tae.BindTenantID(0)

	schema := newClusterTaeSchema(t)
	tae.BindSchema(schema)

	// ------------------------------------------------------------------
	// Create the table and ingest the reported deterministic dataset
	// (tenant 0, device 0..999, metric 0..4, 20 ts ordinals on the 10s
	// grid from 2026-01-01 00:00:00, value = ordinal+1) in the reported
	// shuffled order. Nothing is flushed: every row is
	// committed-but-unflushed, exactly the reported pre-flush scenario.
	// ------------------------------------------------------------------
	allocatorBaseline := taeCommon.DefaultAllocator.CurrNB()
	// Registered first so it runs last, after the engine-close defer and
	// every batch cleanup: the pooled fixture allocations must return the
	// allocator to its pre-fixture baseline.
	t.Cleanup(func() {
		end := taeCommon.DefaultAllocator.CurrNB()
		t.Logf("TAE DefaultAllocator bytes: baseline=%d end=%d", allocatorBaseline, end)
		require.LessOrEqual(t, end, allocatorBaseline+(16<<20),
			"every fixture batch must be closed back to the pool: a leak of the 10x10000-row batches alone would be tens of MB")
	})

	const totalRows = 100000
	appends := clusterByIssueTaeBatches(t, totalRows)
	tae.CreateRelAndAppend(appends[0], true)
	appends[0].Close()
	for _, bat := range appends[1:] {
		tae.DoAppend(bat)
		bat.Close()
	}

	dbID, tblID := clusterTaeIDs(ctx, t, tae)
	primarySeqnum := int(schema.GetPrimaryKey().SeqNum)

	// Shared query-side plumbing, as in the unit golden regression.
	mp := mpool.MustNewZero()
	drainClusterPool(t, mp)
	proc := testutil3.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	packer := types.NewPacker()
	defer packer.Close()
	tailPacker := types.NewPacker()
	defer tailPacker.Close()
	pool := fileservice.NewPool(4,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() })

	// ------------------------------------------------------------------
	// Phase A: pull the real logtail from the real TN and consume it with
	// the production CN replay consumer.
	// ------------------------------------------------------------------
	pState := logtailreplay.NewPartitionState("", false, tblID, false)
	pState.UpdateDuration(types.TS{}, types.MaxTs())
	cursor := applyRealTaeTail(t, ctx, tae, pState, tailPacker, mp,
		dbID, tblID, primarySeqnum, types.BuildTS(0, 0))

	require.Equal(t, totalRows, pState.ApproxInMemRows(),
		"all committed-but-unflushed rows must arrive as real replay rows")
	require.True(t, pState.ClusterIndexReadyFor(clusterReplayCompoundName),
		"the real TAE logtail must have built the cluster-key index")
	require.Zero(t, pState.ApproxDataObjectsNum(),
		"the pre-flush replay state must not carry persisted objects")

	// The reported predicate, in the exact planner-published shape:
	// prefix_in_range(cbkey, serial(0,0,0,'2026-01-01 00:03:00'),
	// serial(0,0,0), 0).
	rangesPredicate := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 18), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))
	readerPredicate := bindClusterPredicate(t, proc, "prefix_in_range",
		clusterColExpr(),
		serialBound(t, proc, 18), serialBound(t, proc, -1),
		plan2.MakePlan2Uint8ConstExprWithType(0))

	// Golden: count=2, sum=39 with only the matching candidates visited.
	res := readClusterReplay(t, ctx, mp, pool, tableDef, pState, readerPredicate)
	require.Equal(t, 2, res.rows)
	require.Equal(t, float64(39), res.sum)
	require.Equal(t, 2, res.scanned,
		"the committed replay iteration over the real TAE logtail must examine only the matching cluster-key candidates")

	// Control: with no predicate the same real replay state surfaces every
	// row, proving the pruning above did not come from a shrunken dataset.
	all := readClusterReplay(t, ctx, mp, pool, tableDef, pState, nil)
	require.Equal(t, totalRows, all.rows)
	require.Equal(t, 1050000.0, all.sum)
	require.Equal(t, totalRows, all.scanned)

	// ------------------------------------------------------------------
	// Buffer-ownership churn: the first tail's closeCB already returned the
	// producer vectors to common.LogtailAllocator while the replay state
	// still references the inserts it consumed. Every applied entry was
	// decoded into independently owned bytes, so recycling those pool
	// buffers through a second table's appends, flush and repeated tail
	// pulls must not disturb the retained replay rows. Re-run the golden.
	// ------------------------------------------------------------------
	churnSchema := taeCatalog.NewEmptySchema("metrics_churn")
	require.NoError(t, churnSchema.AppendCol("v", types.T_int32.ToType()))
	require.NoError(t, churnSchema.AppendFakePKCol())
	require.NoError(t, churnSchema.Finalize(false))
	tae.BindSchema(churnSchema)
	churnFirst := churnTaeBatch(t, 20000)
	tae.CreateRelAndAppend(churnFirst, false)
	churnFirst.Close()
	_, churnTblID := clusterTaeIDs(ctx, t, tae)
	churnSecond := churnTaeBatch(t, 20000)
	tae.DoAppend(churnSecond)
	churnSecond.Close()
	require.NoError(t, tae.FlushTable(ctx, 0, dbID, churnTblID, tae.TxnMgr.Now()))
	for i := 0; i < 3; i++ {
		churnLogtailPool(t, ctx, tae, dbID, churnTblID)
	}
	// Flush completion is not the point here: the pool machinery has been
	// actively recycled, and the re-read must stay exact.
	tae.BindSchema(schema)

	resChurn := readClusterReplay(t, ctx, mp, pool, tableDef, pState, readerPredicate)
	require.Equal(t, 2, resChurn.rows,
		"the retained replay state must survive producer-buffer reuse untouched")
	require.Equal(t, float64(39), resChurn.sum,
		"the retained replay state must survive producer-buffer reuse untouched")
	require.Equal(t, 2, resChurn.scanned,
		"the cluster iteration must stay bounded after producer-buffer reuse")

	// ------------------------------------------------------------------
	// Phase B: flush through the real TN flush path (the engine-level
	// equivalent of mo_ctl('dn','flush')) and pull follow-up tails until
	// every replay row has been transferred into persisted objects.
	// ------------------------------------------------------------------
	require.NoError(t, tae.FlushTable(ctx, 0, dbID, tblID, tae.TxnMgr.Now()))
	require.Eventually(t, func() bool {
		cursor = applyRealTaeTail(t, ctx, tae, pState, tailPacker, mp,
			dbID, tblID, primarySeqnum, cursor)
		return pState.ApproxInMemRows() == 0 && pState.ApproxDataObjectsNum() > 0
	}, 2*time.Minute, 2*time.Second,
		"the real flush tail must transfer every replay row into persisted objects")

	require.Zero(t, pState.ApproxInMemRows(),
		"the post-flush replay state must hold no row versions: the flush tail must remove them")
	require.Greater(t, pState.ApproxDataObjectsNum(), 0)

	// The predicate is answered from real persisted objects whose fan-out
	// is pruned by the production block filter over real object zonemaps.
	filteredRanges := buildRealTaeRanges(t, ctx, tae, tableDef, pState,
		[]*plan.Expr{rangesPredicate})
	allRanges := buildRealTaeRanges(t, ctx, tae, tableDef, pState, nil)
	require.NotNil(t, filteredRanges)
	filteredBlocks := realTaeBlockCount(t, filteredRanges)
	fullBlocks := realTaeBlockCount(t, allRanges)
	require.Greater(t, fullBlocks, filteredBlocks,
		"the cluster predicate must prune real persisted blocks, not only replay rows")
	require.GreaterOrEqual(t, fullBlocks, totalRows/8192,
		"the flushed dataset must span at least its unpruned 8192-row block count")
	require.Equal(t, 1, filteredBlocks,
		"the sorted real cluster objects keep every (0,0,0)-prefixed key inside exactly one block, matching the issue's one-block control")

	// Post-flush golden through the full fan-out: the committed replay layer
	// carries no candidates (the flush tail removed them all; stale cluster
	// index entries would surface here), and the real persisted blocks
	// reproduce the reported answer after the residual predicate that the
	// upper SQL layer applies in production.
	flushAll := readClusterTaeRange(t, ctx, mp, pool,
		tae.Runtime.Fs, tableDef, pState, readerPredicate, allRanges)
	require.Equal(t, 0, sumRealTaeScans(flushAll.scans),
		"the committed replay layer must carry no candidates after the flush: stale cluster index entries would break this")
	flushAllCount, flushAllSum := reportedResidual(t, flushAll.raws)
	require.Equal(t, 2, flushAllCount)
	require.Equal(t, 39.0, flushAllSum)

	// Post-flush pruned fan-out: the production block filter keeps only the
	// real blocks whose sorted cluster-key zonemaps overlap the interval.
	flushFiltered := readClusterTaeRange(t, ctx, mp, pool,
		tae.Runtime.Fs, tableDef, pState, readerPredicate, filteredRanges)
	flushFilteredCount, flushFilteredSum := reportedResidual(t, flushFiltered.raws)
	require.Equal(t, 2, flushFilteredCount, "the pruned block list must preserve the reported result")
	require.Equal(t, 39.0, flushFilteredSum)
	require.Equal(t, 8192*filteredBlocks, flushFiltered.rows,
		"the pruned read must surface exactly the matching blocks' rows")

	// Real persisted round trip: the full unfiltered read of the real
	// objects reproduces the ingested digest.
	digest := readClusterTaeRange(t, ctx, mp, pool,
		tae.Runtime.Fs, tableDef, pState, nil, allRanges)
	require.Equal(t, totalRows, digest.rows)
	require.Equal(t, 1050000.0, digest.valueSum)

	// ------------------------------------------------------------------
	// Phase C: a fresh committed-but-unflushed batch lands in replay state
	// only; one datasource pass composes the persisted matches with the
	// new replay matches.
	// ------------------------------------------------------------------
	bat := clusterBySpecTaeBatch(t, packer, []clusterByTaeRow{
		{device: 0, metric: 0, tsOrd: 20, value: 40.5, quality: 1},
		{device: 0, metric: 0, tsOrd: 20, value: 41.5, quality: 1},
		{device: 0, metric: 0, tsOrd: 21, value: 42.5, quality: 2},
	}, totalRows+1)
	tae.DoAppend(bat)
	bat.Close()
	cursor = applyRealTaeTail(t, ctx, tae, pState, tailPacker, mp,
		dbID, tblID, primarySeqnum, cursor)

	require.Equal(t, 3, pState.ApproxInMemRows(),
		"only the post-flush batch may live in the replay state")

	mixedRes := readClusterTaeRange(t, ctx, mp, pool,
		tae.Runtime.Fs, tableDef, pState, readerPredicate, allRanges)
	mixedCount, mixedSum := reportedResidual(t, mixedRes.raws)
	require.Equal(t, 5, mixedCount,
		"the mixed read must return the two persisted and the three replay matches")
	require.Equal(t, 163.5, mixedSum)
	require.Equal(t, 3, sumRealTaeScans(mixedRes.scans),
		"the replay iteration must examine only the three matching post-flush candidates; stale flushed keys would inflate this count")

	// Bounded mixed read: the pruned blocks plus the replay candidates
	// still compose the complete reported answer.
	mixedFiltered := readClusterTaeRange(t, ctx, mp, pool,
		tae.Runtime.Fs, tableDef, pState, readerPredicate, filteredRanges)
	mixedFilteredCount, mixedFilteredSum := reportedResidual(t, mixedFiltered.raws)
	require.Equal(t, 5, mixedFilteredCount,
		"the bounded mixed read must preserve the composed answer")
	require.Equal(t, 163.5, mixedFilteredSum)
	require.Equal(t, 8192*filteredBlocks+3, mixedFiltered.rows,
		"the bounded mixed read surfaces the matching blocks plus exactly the three replay rows")
}

// clusterByTaeRow describes one row appended through the real TAE write
// path. Tenant is always 0 and timestamps sit on the reported 10s grid.
type clusterByTaeRow struct {
	device  int32
	metric  int32
	tsOrd   int64
	value   float64
	quality int16
}

// clusterTaeIssueRow maps the deterministic issue dataset index to a row.
func clusterTaeIssueRow(i int) clusterByTaeRow {
	j := int64((i / 5000) % 20)
	return clusterByTaeRow{
		device:  int32(i % 1000),
		metric:  int32((i / 1000) % 5),
		tsOrd:   j,
		value:   float64(j + 1),
		quality: int16(i % 3),
	}
}

// clusterByIssueTaeBatches materializes the reported 100,000-row dataset
// into append batches (10 x 10,000 rows) in the reported shuffled order.
func clusterByIssueTaeBatches(t *testing.T, total int) []*taeContainers.Batch {
	t.Helper()

	order := make([]int, total)
	for i := range order {
		order[i] = i
	}
	shuffleClusterOrder(order)

	packer := types.NewPacker()
	defer packer.Close()
	batches := make([]*taeContainers.Batch, 0, (total+9999)/10000)
	for start := 0; start < total; start += 10000 {
		end := start + 10000
		if end > total {
			end = total
		}
		rows := make([]clusterByTaeRow, 0, end-start)
		for _, i := range order[start:end] {
			rows = append(rows, clusterTaeIssueRow(i))
		}
		batches = append(batches,
			clusterBySpecTaeBatch(t, packer, rows, uint64(start)+1))
	}
	return batches
}

// newClusterTaeSchema builds the TAE mirror of the reported table: six
// visible columns, the auto-populated fake primary key and the hidden
// compound CLUSTER BY column as the single non-primary sort key, in the
// same column order the CN table definition carries (visible columns 0..5,
// fake primary key at 6, compound cluster column at 7).
func newClusterTaeSchema(t *testing.T) *taeCatalog.Schema {
	t.Helper()

	schema := taeCatalog.NewEmptySchema("metrics")
	visible := []struct {
		name string
		typ  types.T
	}{
		{"tenant_id", types.T_int32},
		{"device_id", types.T_int32},
		{"metric_id", types.T_int32},
		{"event_ts", types.T_datetime},
		{"value", types.T_float64},
		{"quality", types.T_int16},
	}
	for _, col := range visible {
		require.NoError(t, schema.AppendCol(col.name, col.typ.ToType()))
	}
	require.NoError(t, schema.AppendFakePKCol())
	cbType := types.T_varchar.ToType()
	cbType.Width = 65535
	compoundAttr := engine.Attribute{
		Name:      clusterReplayCompoundName,
		Type:      cbType,
		IsHidden:  true,
		ClusterBy: true,
	}
	require.NoError(t, schema.AppendSortColWithAttribute(compoundAttr, 0, false))
	require.NoError(t, schema.Finalize(false))
	return schema
}

// clusterBySpecTaeBatch builds one real TAE append batch in the schema's
// column layout: the six visible columns, the hidden fake primary key
// (unique values, as the CN write path publishes) and the hidden compound
// cluster column filled with the production serial-encoded cluster key
// bytes, i.e. the same materialized bytes FillCompositeClusterByBatch
// writes on the CN write path and the read-side interval encodes.
func clusterBySpecTaeBatch(
	t *testing.T,
	packer *types.Packer,
	rows []clusterByTaeRow,
	fakePkBase uint64,
) *taeContainers.Batch {
	t.Helper()

	cbType := types.T_varchar.ToType()
	cbType.Width = 65535
	bat := taeContainers.NewBatch()
	newVec := func(typ types.Type) taeContainers.Vector {
		return taeContainers.MakeVector(typ, taeCommon.DefaultAllocator)
	}
	tenantVec := newVec(types.T_int32.ToType())
	deviceVec := newVec(types.T_int32.ToType())
	metricVec := newVec(types.T_int32.ToType())
	eventVec := newVec(types.T_datetime.ToType())
	valueVec := newVec(types.T_float64.ToType())
	qualityVec := newVec(types.T_int16.ToType())
	fakePkVec := newVec(types.T_uint64.ToType())
	clusterVec := newVec(cbType)

	for i, r := range rows {
		eventTs, err := types.ParseDatetime(
			fmt.Sprintf("2026-01-01 00:%02d:%02d", (r.tsOrd*10)/60, (r.tsOrd*10)%60), 6)
		require.NoError(t, err)

		packer.Reset()
		packer.EncodeInt32(0)
		packer.EncodeInt32(r.device)
		packer.EncodeInt32(r.metric)
		packer.EncodeDatetime(eventTs)

		tenantVec.Append(int32(0), false)
		deviceVec.Append(r.device, false)
		metricVec.Append(r.metric, false)
		eventVec.Append(eventTs, false)
		valueVec.Append(r.value, false)
		qualityVec.Append(r.quality, false)
		fakePkVec.Append(fakePkBase+uint64(i), false)
		clusterVec.Append(packer.Bytes(), false)
	}

	// Schema-position order: visible 0..5, fake primary key 6, compound 7.
	bat.AddVector("tenant_id", tenantVec)
	bat.AddVector("device_id", deviceVec)
	bat.AddVector("metric_id", metricVec)
	bat.AddVector("event_ts", eventVec)
	bat.AddVector("value", valueVec)
	bat.AddVector("quality", qualityVec)
	bat.AddVector(taeFakePkColName(), fakePkVec)
	bat.AddVector(clusterReplayCompoundName, clusterVec)
	// Close on failure paths too; the vector wrappers' Close is idempotent
	// (compare-and-swap guard), so the post-append close in the test body
	// can run first and this cleanup is a backstop.
	t.Cleanup(bat.Close)
	return bat
}

// taeFakePkColName pins the hidden fake primary key column name the TAE
// schema and the CN table definition share.
func taeFakePkColName() string {
	return catalog.FakePrimaryKeyColName
}

// shuffleClusterOrder applies the reported dataset's fixed shuffle so rows
// arrive out of cluster-key order, as in the reported ingestion.
func shuffleClusterOrder(order []int) {
	rand.New(rand.NewSource(20260101)).Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
}

// churnTaeBatch builds one small single-column batch for a second table
// whose writes and flush recycle TAE's pooled buffers while the first
// table's replay state is held in memory.
func churnTaeBatch(t *testing.T, rows int) *taeContainers.Batch {
	t.Helper()

	bat := taeContainers.NewBatch()
	vec := taeContainers.MakeVector(types.T_int32.ToType(), taeCommon.DefaultAllocator)
	for i := 0; i < rows; i++ {
		vec.Append(int32(i), false)
	}
	bat.AddVector("v", vec)
	t.Cleanup(bat.Close)
	return bat
}

// clusterTaeIDs returns the real database and table ids of the bound
// relation.
func clusterTaeIDs(
	ctx context.Context,
	t *testing.T,
	tae *taeDBTestutil.TestEngine,
) (dbID, tblID uint64) {
	t.Helper()

	txn, rel := tae.GetRelation()
	dbHandle, err := rel.GetDB()
	require.NoError(t, err)
	dbID, tblID = dbHandle.GetID(), rel.ID()
	require.NoError(t, txn.Commit(ctx))
	return dbID, tblID
}

// clusterTaeCkpClient satisfies logtail.CheckpointClient the way the wire
// handler tests do here: no checkpoint collections and no table pushes
// mid-response.
type clusterTaeCkpClient struct{}

func (clusterTaeCkpClient) CollectCheckpointsInRange(
	ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error) {
	return "", types.TS{}, nil
}

func (clusterTaeCkpClient) FlushTable(
	ctx context.Context, accoutID uint32, dbID, tableID uint64, ts types.TS) error {
	return nil
}

// realTaeTstp converts a TAE timestamp into the wire pointer form.
func realTaeTstp(ts types.TS) *timestamp.Timestamp {
	x := ts.ToTimestamp()
	return &x
}

// applyRealTaeTail pulls the real TAE logtail for [have, now] through the
// production TN producer (logtail.HandleSyncLogTailReq) and applies every
// entry into the CN replay state through the production consumer
// (PartitionState.HandleLogtailEntry), mirroring consumeEntry. It returns
// the applied watermark so incremental tails can follow.
//
// Every entry is decoded through a marshal/unmarshal round-trip before it
// is applied: on the real wire the CN consumes transport-decoded messages,
// so protobuf unmarshal gives each message independently owned Data/Area
// bytes, and that ownership is what protects the retained replay state
// (HandleRowsInsert keeps entry.Batch alive for every replayed row) from
// the producer side. The in-process pull bypasses the transport, so the
// raw producer vectors alias common.LogtailAllocator pool buffers that
// TableLogtailRespBuilder.Close (the returned closeCB) recycles once this
// function returns; decoding first gives the replay state exactly the bytes
// the production CN would hold.
func applyRealTaeTail(
	t *testing.T,
	ctx context.Context,
	tae *taeDBTestutil.TestEngine,
	pState *logtailreplay.PartitionState,
	packer *types.Packer,
	mp *mpool.MPool,
	dbID, tblID uint64,
	primarySeqnum int,
	have types.TS,
) types.TS {
	t.Helper()

	now := tae.TxnMgr.Now()
	resp, closeCB, err := taeLogtail.HandleSyncLogTailReq(ctx,
		clusterTaeCkpClient{},
		tae.LogtailMgr,
		tae.Catalog,
		api.SyncLogTailReq{
			CnHave: realTaeTstp(have),
			CnWant: realTaeTstp(now),
			Table:  &api.TableID{DbId: dbID, TbId: tblID},
		}, true)
	require.NoError(t, err)
	if closeCB != nil {
		defer closeCB()
	}
	for _, entry := range resp.Commands {
		payload, err := entry.Marshal()
		require.NoError(t, err)
		var owned api.Entry
		require.NoError(t, owned.Unmarshal(payload))
		pState.HandleLogtailEntry(ctx, tae.Runtime.Fs, &owned,
			primarySeqnum, packer, mp)
	}
	pState.UpdateAppliedTo(now)
	return now
}

// churnLogtailPool pulls a real tail without applying it and immediately
// releases the producer vectors, recycling common.LogtailAllocator buffers
// that earlier responses used. It lets the integration prove retained
// replay data survives producer-buffer reuse.
func churnLogtailPool(
	t *testing.T,
	ctx context.Context,
	tae *taeDBTestutil.TestEngine,
	dbID, tblID uint64,
) {
	t.Helper()

	resp, closeCB, err := taeLogtail.HandleSyncLogTailReq(ctx,
		clusterTaeCkpClient{},
		tae.LogtailMgr,
		tae.Catalog,
		api.SyncLogTailReq{
			CnHave: realTaeTstp(types.BuildTS(0, 0)),
			CnWant: realTaeTstp(tae.TxnMgr.Now()),
			Table:  &api.TableID{DbId: dbID, TbId: tblID},
		}, true)
	require.NoError(t, err)
	_ = resp
	if closeCB != nil {
		closeCB()
	}
}

// buildRealTaeRanges drives the production fast block filter over the real
// partition state and real objects, producing the block list the pushed
// ranges would carry.
func buildRealTaeRanges(
	t *testing.T,
	ctx context.Context,
	tae *taeDBTestutil.TestEngine,
	tableDef *plan.TableDef,
	pState *logtailreplay.PartitionState,
	blockFilters []*plan.Expr,
) objectio.BlockInfoSlice {
	t.Helper()

	snapshotTS := pState.GetEnd().ToTimestamp()
	outBlocks := objectio.PreAllocBlockInfoSlice(8)
	outBlocks.AppendBlockInfo(&objectio.EmptyBlockInfo)
	rangesParam := engine.RangesParam{
		BlockFilters:   blockFilters,
		PreAllocBlocks: 8,
		Policy:         engine.Policy_CollectAllData,
	}
	done, err := readutil.TryFastFilterBlocksWithZone(
		ctx,
		snapshotTS,
		tableDef,
		rangesParam,
		pState,
		nil,
		nil,
		&outBlocks,
		func(context.Context) bool { return false },
		tae.Runtime.Fs,
		time.Local,
	)
	require.NoError(t, err)
	require.True(t, done,
		"the production fast block filter must accept the real partition state and the cluster predicate")
	return outBlocks
}

// realTaeBlockCount counts real blocks in a range list, ignoring the
// leading in-memory marker block.
func realTaeBlockCount(t *testing.T, ranges objectio.BlockInfoSlice) int {
	t.Helper()
	count := 0
	for i := 0; i < ranges.Len(); i++ {
		if !ranges.Get(i).IsMemBlk() {
			count++
		}
	}
	return count
}

// realClusterReadResult carries per-read facts from the datasource pass:
// the raw rows surfaced (the reader boundary hands out pruned candidates,
// the upper SQL layer applies the residual filtering, which the harness
// mirrors), plus the ordered replay-side scan counters the reader writes
// into its summary.
type realClusterReadResult struct {
	raws     []realClusterRowTuple
	rows     int
	valueSum float64
	batches  int
	scans    []int
}

// realClusterRowTuple captures one surfaced row for residual evaluation.
type realClusterRowTuple struct {
	tenant  int32
	device  int32
	metric  int32
	eventTs types.Datetime
	value   float64
}

// reportedResidual mirrors the reported predicate's residual evaluation:
// tenant_id = 0 AND device_id = 0 AND metric_id = 0 AND
// event_ts >= '2026-01-01 00:03:00'.
func reportedResidual(t *testing.T, raws []realClusterRowTuple) (count int, sum float64) {
	t.Helper()
	before, err := types.ParseDatetime("2026-01-01 00:03:00", 6)
	require.NoError(t, err)
	for _, r := range raws {
		if r.tenant == 0 && r.device == 0 && r.metric == 0 && int64(r.eventTs) >= int64(before) {
			count++
			sum += r.value
		}
	}
	return count, sum
}

func sumRealTaeScans(scans []int) int {
	total := 0
	for _, s := range scans {
		total += s
	}
	return total
}

// readClusterTaeRange runs one query through the production reader path
// over a LocalDisttaeDataSource backed by the real partition state and the
// real block ranges, so the committed replay layer and the persisted object
// layer are both exercised in one pass.
func readClusterTaeRange(
	t *testing.T,
	ctx context.Context,
	mp *mpool.MPool,
	pool *fileservice.Pool[*types.Packer],
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	pState *logtailreplay.PartitionState,
	expr *plan.Expr,
	ranges objectio.BlockInfoSlice,
) realClusterReadResult {
	t.Helper()

	summaryBuf := &bytes.Buffer{}
	qryCtx := context.WithValue(ctx, defines.ReaderSummaryKey{}, summaryBuf)

	txnOp, closeTxn := client.NewTestTxnOperator(qryCtx)
	defer closeTxn()

	tbl := &txnTable{
		db:       &txnDatabase{databaseId: 11, op: txnOp},
		tableId:  22,
		tableDef: tableDef,
	}

	ls := &LocalDisttaeDataSource{
		ctx:             qryCtx,
		table:           tbl,
		pState:          pState,
		category:        engine.GeneralLocalDataSource,
		iteratePhase:    engine.InMem,
		tombstonePolicy: engine.Policy_SkipAll,
		snapshotTS:      types.MaxTs(),
		fs:              fs,
	}
	if ranges != nil && ranges.Len() > 0 && ranges.Get(0).IsMemBlk() {
		ranges = ranges.Slice(1, ranges.Len())
	}
	ls.rangeSlice = ranges
	ls.rc.prefetchDisabled = true
	defer ls.Close()

	reader, err := readutil.NewReader(
		qryCtx, mp, pool, fs, tableDef,
		timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()},
		expr, ls, 0, engine.FilterHint{})
	require.NoError(t, err)

	attrs := []string{"tenant_id", "device_id", "metric_id", "event_ts", "value"}
	out := batch.NewWithSize(len(attrs))
	out.Attrs = attrs
	for i := range attrs {
		switch attrs[i] {
		case "tenant_id", "device_id", "metric_id":
			out.Vecs[i] = vector.NewVec(types.T_int32.ToType())
		case "event_ts":
			out.Vecs[i] = vector.NewVec(types.T_datetime.ToType())
		default:
			out.Vecs[i] = vector.NewVec(types.T_float64.ToType())
		}
	}
	defer out.Clean(mp)

	res := realClusterReadResult{}
	for {
		out.CleanOnlyData()
		isEnd, err := reader.Read(qryCtx, out.Attrs, nil, mp, out)
		require.NoError(t, err)
		if isEnd {
			break
		}
		res.batches++
		tenants := vector.MustFixedColWithTypeCheck[int32](out.Vecs[0])
		devices := vector.MustFixedColWithTypeCheck[int32](out.Vecs[1])
		metrics := vector.MustFixedColWithTypeCheck[int32](out.Vecs[2])
		eventTs := vector.MustFixedColWithTypeCheck[types.Datetime](out.Vecs[3])
		values := vector.MustFixedColWithTypeCheck[float64](out.Vecs[4])
		for i := range values {
			res.raws = append(res.raws, realClusterRowTuple{
				tenant:  tenants[i],
				device:  devices[i],
				metric:  metrics[i],
				eventTs: eventTs[i],
				value:   values[i],
			})
			res.rows++
			res.valueSum += values[i]
		}
	}
	require.NoError(t, reader.Close())

	for _, m := range clusterScanRe.FindAllStringSubmatch(summaryBuf.String(), -1) {
		var v int
		_, err := fmt.Sscanf(m[1], "%d", &v)
		require.NoError(t, err)
		res.scans = append(res.scans, v)
	}
	return res
}
