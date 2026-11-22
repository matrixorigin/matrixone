// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type temporalNativeFixture struct {
	ctx              context.Context
	read             client.CatalogReadStamp
	base, hidden     *txnTableDelegate
	full, hiddenFull *plan.TableDef
	stats            *pb.StatsInfo
	key              pb.StatsInfoKey
	gs               *GlobalStats
}

func newTemporalNativeFixture(t testing.TB) *temporalNativeFixture {
	t.Helper()
	ctx := defines.AttachAccountId(t.Context(), 7)
	op, closeClient := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeClient)
	op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
	e := &Engine{}
	txn := &Transaction{engine: e, op: op}
	txn.readOnly.Store(true)
	op.AddWorkspace(txn)
	stamp, ok := op.(interface {
		TryCatalogReadStamp() (client.CatalogReadStamp, bool)
	}).TryCatalogReadStamp()
	require.True(t, ok)
	visibility, stable := txn.CatalogVisibility()
	require.True(t, stable)
	read := (stamp).BindRead(op, visibility)
	integer := plan.Type{Id: int32(types.T_uint64)}
	full := &plan.TableDef{TblId: 98, Version: 2, Name: "base", Cols: []*plan.ColDef{{Name: "id", Typ: integer}, {Name: "d", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_datetime)}}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}, Indexes: []*plan.IndexDef{{IndexName: "idx", IndexTableName: "__mo_index_secondary_hidden", TableExist: true, Parts: []string{"d", catalog.CreateAlias("id")}}}}
	hiddenFull := &plan.TableDef{TblId: 99, Version: 2, Name: "__mo_index_secondary_hidden", FeatureFlag: features.IndexTable, Cols: []*plan.ColDef{{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}, Primary: true}, {Name: catalog.IndexTablePrimaryColName, Seqnum: 1, Typ: integer}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName}}
	db := &txnDatabase{accountId: 7, databaseId: 10, databaseName: "db", op: op}
	base := &txnTableDelegate{origin: &txnTable{accountId: 7, tableId: 98, version: 2, tableName: "base", tableDef: full, relKind: catalog.SystemOrdinaryRel, eng: e, db: db}}
	hidden := &txnTableDelegate{origin: &txnTable{accountId: 7, tableId: 99, version: 2, tableName: hiddenFull.Name, tableDef: hiddenFull, relKind: catalog.SystemIndexRel, extraInfo: &api.SchemaExtra{ParentTableID: 98}, eng: e, db: db}}
	owner, cancel := context.WithCancel(t.Context())
	gs := &GlobalStats{ctx: owner, cancel: cancel, engine: e}
	gs.mu.cond = sync.NewCond(&gs.mu)
	e.globalStats = gs
	t.Cleanup(gs.Close)
	stats := &pb.StatsInfo{TableCnt: 1000, BlockNumber: 10, ShuffleRangeMap: map[string]*pb.ShuffleRange{"d": {Overlap: .25}}, MinValMap: map[string]float64{"d": 100}, MaxValMap: map[string]float64{"d": 200}, NdvMap: map[string]float64{"d": 100}, NullCntMap: map[string]uint64{"d": 0}, DataTypeMap: map[string]uint64{"d": uint64(types.T_datetime)}}
	key := pb.StatsInfoKey{AccId: 7, DatabaseID: 10, TableID: 98, DbName: "db", TableName: "base"}
	gs.cache.statsInfoMap = map[pb.StatsInfoKey]*pb.StatsInfo{key: stats}
	gs.cache.tableDefVersions = map[pb.StatsInfoKey]uint32{key: 2}
	return &temporalNativeFixture{ctx: ctx, read: read, base: base, hidden: hidden, full: full, hiddenFull: hiddenFull, stats: stats, key: key, gs: gs}
}
func (f *temporalNativeFixture) pair() engine.IndexPairCheck {
	return engine.IndexPairCheck{BaseRef: &plan.ObjectRef{Obj: 98, SchemaName: "db", ObjName: "base"}, HiddenRef: &plan.ObjectRef{Obj: 99, SchemaName: "db", ObjName: f.hiddenFull.Name}, BaseFull: f.full, BaseScan: f.full, HiddenFull: f.hiddenFull, Index: f.full.Indexes[0], Column: "d", Hidden: f.hidden, HiddenRead: f.read}
}

func TestIndexCostStatsAvailableDuringCoordination(t *testing.T) {
	f := newTemporalNativeFixture(t)
	f.gs.mu.Lock()
	defer f.gs.mu.Unlock()

	value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.True(t, ok, "coordination alone must not hide published statistics")
	require.Equal(t, 100., value.Min)
	require.Equal(t, 200., value.Max)
}

func TestIndexCostStatsRecoverAfterWriter(t *testing.T) {
	f := newTemporalNativeFixture(t)
	var ok bool
	var err error
	func() {
		f.gs.cache.Lock()
		defer f.gs.cache.Unlock()
		_, ok, err = f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	}()
	require.NoError(t, err)
	require.False(t, ok, "an active writer must not block the observation")

	value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 100., value.Min)
}

func TestIndexCostStatsFollowPublicationLifetime(t *testing.T) {
	f := newTemporalNativeFixture(t)
	f.gs.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	oldGeneration := f.gs.currentOrCreateUpdateRecord(f.key)
	f.gs.RemoveTid(f.key.TableID)
	_, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.False(t, ok)

	newStats := *f.stats
	newGeneration := f.gs.currentOrCreateUpdateRecord(f.key)
	require.NotSame(t, oldGeneration, newGeneration)
	published, err := f.gs.publishStatsForGeneration(f.ctx, f.key, newGeneration, &newStats)
	require.NoError(t, err)
	require.True(t, published)
	_, ok, err = f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.False(t, ok, "equal values from a replacement do not validate an old pointer")
	value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, &newStats, "d")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 100., value.Min)
}

func BenchmarkIndexCostStatsRead(b *testing.B) {
	for _, withCacheLookup := range []bool{false, true} {
		name := "observation"
		if withCacheLookup {
			name = "cache_and_observation"
		}
		b.Run(name, func(b *testing.B) {
			f := newTemporalNativeFixture(b)
			var observed atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var successes uint64
				for pb.Next() {
					if withCacheLookup {
						f.gs.Get(f.ctx, f.key, false)
					}
					_, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
					if err != nil {
						b.Error(err)
						return
					}
					if ok {
						successes++
					}
				}
				observed.Add(successes)
			})
			b.StopTimer()
			b.ReportMetric(float64(observed.Load())/float64(b.N), "observed/op")
		})
	}
}

func TestTemporalCBOStatsCopiesOrdinaryValuesWithoutTransport(t *testing.T) {
	f := newTemporalNativeFixture(t)
	require.Nil(t, f.gs.engine.pClient.subscriber, "metadata needs no transport/ledger/worker")
	value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1000., value.Rows)
	require.Equal(t, 100., value.Min)
	require.EqualValues(t, 10, value.Blocks)
	for _, scenario := range []string{"unbound", "missing nulls", "wrong pointer", "wrong version", "reader active", "writer active", "cancelled", "closed"} {
		t.Run(scenario, func(t *testing.T) {
			f := newTemporalNativeFixture(t)
			observed := f.stats
			ctx := f.ctx
			switch scenario {
			case "unbound":
				delete(f.gs.cache.tableDefVersions, f.key)
			case "missing nulls":
				delete(f.stats.NullCntMap, "d")
			case "wrong pointer":
				observed = &pb.StatsInfo{}
			case "wrong version":
				f.gs.cache.tableDefVersions[f.key]++
			case "reader active":
				f.gs.cache.RLock()
				defer f.gs.cache.RUnlock()
			case "writer active":
				f.gs.cache.Lock()
				defer f.gs.cache.Unlock()
			case "cancelled":
				var cancel context.CancelCauseFunc
				ctx, cancel = context.WithCancelCause(ctx)
				cancel(context.DeadlineExceeded)
			case "closed":
				f.gs.Close()
			}
			_, ok, err := f.base.ReadIndexCostStats(ctx, f.read, observed, "d")
			if scenario == "cancelled" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, scenario == "unbound" || scenario == "reader active", ok)
		})
	}
}
func TestTemporalCBOStatsSnapshotConcurrentPatch(t *testing.T) {
	f := newTemporalNativeFixture(t)
	f.stats.MinValMap["d"], f.stats.MaxValMap["d"] = 2, 4
	start := make(chan struct{})
	done := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	defer worker.Wait()
	go func() {
		defer worker.Done()
		<-start
		for i := 1; i <= 128; i++ {
			v := float64(i%8 + 1)
			if err := f.gs.PatchStats(f.key, &PatchArgs{MinValMap: map[string]float64{"d": v}, MaxValMap: map[string]float64{"d": 2 * v}}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	close(start)
	for range 128 {
		value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
		require.NoError(t, err)
		if ok {
			require.Equal(t, 2*value.Min, value.Max)
		}
	}
	require.NoError(t, <-done)
	value, ok, err := f.base.ReadIndexCostStats(f.ctx, f.read, f.stats, "d")
	require.NoError(t, err)
	require.True(t, ok, "the observation must recover after patching stops")
	require.Equal(t, 2*value.Min, value.Max)
}
func TestTemporalCBONativePairUsesActualRelationsAndLayouts(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong parent", "wrong kind", "wrong account", "wrong base ref", "wrong hidden version", "swapped varchar columns", "read mutation", "snapshot ABA", "budget exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			f := newTemporalNativeFixture(t)
			check := f.pair()
			budget := engine.MaxIndexPlanningWork
			switch scenario {
			case "wrong parent":
				f.hidden.origin.extraInfo.ParentTableID++
			case "wrong kind":
				f.hidden.origin.relKind = catalog.SystemOrdinaryRel
			case "wrong account":
				f.hidden.origin.accountId++
			case "wrong base ref":
				check.BaseRef.Obj++
			case "wrong hidden version":
				copy := *f.hiddenFull
				copy.Version++
				check.HiddenFull = &copy
			case "swapped varchar columns":
				f.full.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar)}
				f.hiddenFull.Cols[1].Typ = f.full.Cols[0].Typ
				copy := *f.hiddenFull
				copy.Cols = append([]*plan.ColDef(nil), f.hiddenFull.Cols...)
				copy.Cols[0], copy.Cols[1] = copy.Cols[1], copy.Cols[0]
				check.HiddenFull = &copy
			case "read mutation":
				txn := f.read.Workspace.(*Transaction)
				txn.beginCatalogMutation()
				txn.endCatalogMutation()
			case "snapshot ABA":
				f.read.Op.SetSnapshotTS(f.read.Snapshot.Next())
				f.read.Op.SetSnapshotTS(f.read.Snapshot)
			case "budget exhausted":
				budget = 0
			}
			ok, err := f.base.VerifyIndexPair(f.ctx, f.read, check, &budget)
			require.NoError(t, err)
			require.Equal(t, scenario == "valid", ok)
		})
	}
}

func TestTemporalNativeReadQualification(t *testing.T) {
	for _, scenario := range []string{"readonly", "prior write", "DDL", "snapshot ABA", "visibility changed", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			f := newTemporalNativeFixture(t)
			txn := f.read.Workspace.(*Transaction)
			switch scenario {
			case "prior write":
				txn.readOnly.Store(false)
			case "DDL":
				txn.haveDDL.Store(true)
			case "snapshot ABA":
				f.read.Op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 4})
				f.read.Op.SetSnapshotTS(f.read.Snapshot)
			case "visibility changed":
				txn.beginCatalogMutation()
				txn.endCatalogMutation()
			case "cancelled":
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				f.ctx = ctx
			}
			_, _, valid := f.base.planningRead(f.ctx, f.read)
			require.Equal(t, scenario == "readonly", valid)
		})
	}
}
