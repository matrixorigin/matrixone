// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataBranchTruncateCollectWindow(t *testing.T) {
	ctx := t.Context()
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	ts := func(p int64) types.TS { return types.BuildTS(p, 0) }
	next := func(p int64) types.TS { return types.BuildTS(p, 1) }
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(ts(65).ToTimestamp()).AnyTimes()
	ses.txnHandler = &TxnHandler{txnOp: txnOp}
	birth := []int64{0, 5, 13, 21, 31, 41, 51}
	relations := make([]engine.Relation, len(birth))
	history := &branchCatalogHistory{
		dbLowerBounds: make(map[dbCreatedTimeLowerBoundKey]types.TS),
		tableCTS:      make(map[tableCreationCommitTSKey]types.TS),
	}
	lowerBound := ts(1)
	for id := 1; id < len(relations); id++ {
		rel := mock_frontend.NewMockRelation(ctrl)
		name := fmt.Sprintf("t%d", id)
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(id)).AnyTimes()
		rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(100)).AnyTimes()
		rel.EXPECT().GetTableName().Return(name).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(newTestBranchTableDef(name, "name")).AnyTimes()
		relations[id] = rel
		for _, snapshot := range []types.TS{ts(12), ts(20), ts(30), ts(40), ts(50), ts(65)} {
			history.dbLowerBounds[dbCreatedTimeLowerBoundKey{
				accountID: ses.GetAccountId(), databaseID: 100, database: "db1", snapshot: snapshot,
			}] = lowerBound
			history.tableCTS[tableCreationCommitTSKey{
				accountID: ses.GetAccountId(), tableID: uint64(id), database: "db1", table: name,
				snapshot: snapshot, lowerBound: lowerBound,
			}] = ts(birth[id])
		}
	}
	ctx = context.WithValue(ctx, branchCatalogHistoryKey{}, history)
	originalReopen := reopenDataBranchRelationByID
	originalCollect := databranchutils.CollectChanges
	t.Cleanup(func() {
		reopenDataBranchRelationByID = originalReopen
		databranchutils.CollectChanges = originalCollect
	})
	reopenDataBranchRelationByID = func(
		_ context.Context, _ *Session, _ BackgroundExec, id uint64, snapshot *plan.Snapshot,
	) (engine.Relation, error) {
		if snapshot.TS.PhysicalTime < birth[id] {
			return nil, moerr.NewNoSuchTableNoCtx("db1", fmt.Sprintf("t%d", id))
		}
		return relations[id], nil
	}

	info := branchMetaInfo{
		lcaTableId: 1, tarRebuildChain: []uint64{6, 5, 4, 3, 2}, pathFromLCAToBase: []uint64{1}, pathFromLCAToBaseTS: []types.TS{{}},
		pathFromLCAToBaseLineageOnly: []bool{false},
		pathFromLCAToTar:             []uint64{1, 2, 3, 4, 5, 6},
		pathFromLCAToTarTS:           []types.TS{{}, ts(12), ts(20), ts(30), ts(40), ts(50)},
		pathFromLCAToTarLineageOnly:  []bool{false, false, true, true, true, true},
		pathFromLCAToTarClearsRows:   []bool{false, false, false, true, true, false},
	}
	tables := newTestBranchTableStuff(ctrl)
	tables.lcaCTS, tables.lcaRel = ts(5), relations[1]
	tables.tarRel, tables.baseRel = relations[6], relations[1]
	tarIdent, baseIdent := tables.endpointIdentities(ctx, ses)
	cr, zeroHistory, err := buildSideCollectRange(ctx, ses, nil, tables,
		dedupeBranchIdentities(tarIdent, baseIdent), ts(65), ts(51), info.pathFromLCAToTar,
		info.pathFromLCAToTarTS, info.pathFromLCAToTarLineageOnly, info.pathFromLCAToTarClearsRows, info.pathFromLCAToBaseTS,
		nil, ts(65), false)
	require.NoError(t, err)
	require.False(t, zeroHistory)
	require.Equal(t, []types.TS{{}, {}, ts(31), ts(41), {}}, cr.clearAt)
	require.Equal(t, ts(31), cr.from[2])
	require.Equal(t, ts(41), cr.from[3])
	require.Equal(t, next(51), cr.from[4])

	type segment struct {
		id       uint64
		from, to types.TS
	}
	for _, tc := range []struct {
		name       string
		lo, hi     int64
		endpoint   uint64
		want       []segment
		baselineID uint64
	}{
		{"last_consecutive_clear", 35, 50, 6, []segment{{5, types.MinTs(), ts(50)}}, 4},
		{"clear_at_hi_even_without_dml", 35, 41, 6, []segment{{5, types.MinTs(), ts(41)}}, 4},
		{"clear_at_lo_is_excluded", 41, 65, 6, []segment{{5, next(41), ts(50)}, {6, next(51), ts(65)}}, 0},
		{"both_clears_and_later_copy", 25, 65, 6, []segment{{6, types.MinTs(), ts(65)}}, 3},
		{"historical_endpoint", 0, 0, 3, []segment{{2, next(13), ts(20)}, {3, next(21), ts(30)}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, info := tables, info
			var lo, hi *types.TS
			if tc.lo != 0 {
				from, to := ts(tc.lo), ts(tc.hi)
				lo, hi = &from, &to
			} else {
				tables.tarRel = relations[tc.endpoint]
				at := ts(30).ToTimestamp()
				tables.tarSnap = &plan.Snapshot{TS: &at}
				info.pathFromLCAToTar = info.pathFromLCAToTar[:3]
				info.pathFromLCAToTarTS = info.pathFromLCAToTarTS[:3]
				info.pathFromLCAToTarLineageOnly = info.pathFromLCAToTarLineageOnly[:3]
				info.pathFromLCAToTarClearsRows = nil
			}
			var got []segment
			databranchutils.CollectChanges = func(
				ctx context.Context, rel engine.Relation, from, to types.TS, _ *mpool.MPool,
			) (engine.ChangesHandle, error) {
				if id := rel.GetTableID(ctx); id != 1 {
					got = append(got, segment{id, from, to})
				}
				return nil, nil
			}
			_, _, err := constructChangeHandle(ctx, ses, nil, &tables, &info, nil, nil, lo, hi, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.baselineID != 0, tables.tarVisibleState)
			if tc.baselineID != 0 {
				require.Equal(t, tc.baselineID, tables.tarClearBaseline.rel.GetTableID(ctx))
				require.Equal(t, *lo, tables.tarClearBaseline.at)
			}
		})
	}
}

func TestHashDiffTruncateNetChanges(t *testing.T) {
	type testCase struct {
		name                              string
		sourceV                           string
		both                              bool
		pick                              int64
		conflict                          *tree.ConflictOpt
		want                              []string
		wantErr                           bool
		boundedDelete, async, pickMarkers bool
	}
	runCase := func(tc testCase) {
		t.Run(tc.name, func(t *testing.T) {
			ses := newValidateSession(t)
			ctrl := gomock.NewController(t)
			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()
			ses.txnHandler = &TxnHandler{txnOp: txnOp}
			tables := newTestBranchTableStuff(ctrl)
			tables.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)
			worker, err := ants.NewPool(1)
			require.NoError(t, err)
			defer worker.Release()
			tables.worker = worker
			tables.tarVisibleState, tables.baseVisibleState = true, tc.both
			tables.lcaRel = mock_frontend.NewMockRelation(ctrl)
			lca := tables.lcaRel.(*mock_frontend.MockRelation)
			lca.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
			lca.EXPECT().GetTableDef(gomock.Any()).Return(newTestBranchTableDef("ancestor", "name")).AnyTimes()
			mp := ses.proc.Mp()
			defer tables.retPool.freeAllRetBatches(mp)

			probe := types.BuildTS(10, 0)
			tables.lcaRelSnapshot = probe
			if tc.boundedDelete {
				tables.tarClearBaseline = &dataBranchSnapshotRelation{rel: lca, at: probe}
			}
			originalScan := scanSnapshotRelationWithPreopened
			t.Cleanup(func() { scanSnapshotRelationWithPreopened = originalScan })
			scanSnapshotRelationWithPreopened = func(
				_ context.Context, caller string, _ *Session, id uint64, at types.TS,
				_ engine.Relation, attrs []string, typs []types.Type, _ *plan.Expr,
				parallelism int, visit func(*batch.Batch) error,
			) error {
				require.Equal(t, "data-branch-truncate-baseline", caller)
				require.Equal(t, uint64(77), id)
				require.Equal(t, probe, at)
				require.Equal(t, []string{"id", "name", "hidden"}, attrs)
				require.Equal(t, 1, parallelism)
				rows := batch.NewWithSize(3)
				defer rows.Clean(mp)
				for i, typ := range typs {
					rows.Vecs[i] = vector.NewVec(typ, mp)
				}
				for i, v := range []string{"a", "b"} {
					require.NoError(t, vector.AppendFixed(rows.Vecs[0], int64(i+1), false, mp))
					require.NoError(t, vector.AppendBytes(rows.Vecs[1], []byte(v), false, mp))
					require.NoError(t, vector.AppendBytes(rows.Vecs[2], []byte("hidden"), false, mp))
				}
				rows.SetRowCount(2)
				return visit(rows)
			}
			var pickKeys databranchutils.BranchHashmap
			if tc.pick != 0 {
				pickKeys, err = databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapAllocator(tables.hashmapAllocator))
				require.NoError(t, err)
				defer pickKeys.Close()
				pk := vector.NewVec(types.T_int64.ToType(), mp)
				defer pk.Free(mp)
				require.NoError(t, vector.AppendFixed(pk, tc.pick, false, mp))
				require.NoError(t, pickKeys.PutByVectors([]*vector.Vector{pk}, []int{0}))
			}
			dataAt := commitTSBytes(types.BuildTS(18, 0))
			target := buildHashDiffDataBatch(t, mp, [][]any{
				{int64(1), tc.sourceV, "hidden", dataAt}, {int64(3), "c", "hidden", dataAt},
			})
			tarHandles := []engine.ChangesHandle{&stubEngineChangesHandle{
				responses: []stubEngineChangesHandleResponse{{data: target}},
			}}
			var baseHandles []engine.ChangesHandle
			if tc.both {
				base := buildHashDiffDataBatch(t, mp, [][]any{{int64(1), "y", "hidden", dataAt}})
				baseHandles = []engine.ChangesHandle{&stubEngineChangesHandle{
					responses: []stubEngineChangesHandleResponse{{data: base}},
				}}
			}
			if tc.boundedDelete {
				base := buildHashDiffDataBatch(t, mp, [][]any{{int64(2), "local", "hidden", dataAt}})
				baseHandles = []engine.ChangesHandle{&stubEngineChangesHandle{
					responses: []stubEngineChangesHandleResponse{{data: base}},
				}}
			}
			var got []string
			err = hashDiff(t.Context(), ses, nil, tables, branchMetaInfo{
				lcaTableId: 77, pathFromLCAToTarTS: []types.TS{{}, probe},
			}, compositeOption{
				onlineApply: !tc.async, expandUpdate: tc.conflict != nil,
				conflictOpt: tc.conflict, preservePickConflicts: tc.pickMarkers,
			},
				func(w batchWithKind) (bool, error) {
					if tc.pickMarkers {
						assert.False(t, w.hasReplacement)
					}
					for _, row := range decodeCapturedRows(t, w.batch, tables.def.colTypes) {
						got = append(got, fmt.Sprintf("%s %s %v %v", w.name, w.logicalOperation(), row[0], row[1]))
					}
					tables.retPool.releaseRetBatch(w.batch, false)
					return false, nil
				}, tarHandles, baseHandles, pickKeys)
			if tc.wantErr {
				require.ErrorContains(t, err, "conflict")
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, tc.want, got)
			}
		})
	}
	for _, tc := range []testCase{
		{name: "net", sourceV: "x", want: []string{"target UPDATE 1 x", "target DELETE 2 b", "target INSERT 3 c"}},
		{name: "identical", sourceV: "a", want: []string{"target DELETE 2 b", "target INSERT 3 c"}},
		{name: "both", sourceV: "x", both: true, want: []string{"target UPDATE 1 x", "base UPDATE 1 y", "target INSERT 3 c"}},
		{name: "picked_new_key", sourceV: "x", pick: 3, want: []string{"target INSERT 3 c"}},
		{name: "picked_deleted_key", sourceV: "x", pick: 2, want: []string{"target DELETE 2 b"}},
		{name: "fail", sourceV: "x", both: true, conflict: &tree.ConflictOpt{Opt: tree.CONFLICT_FAIL}, wantErr: true},
		{name: "skip", sourceV: "x", both: true, conflict: &tree.ConflictOpt{Opt: tree.CONFLICT_SKIP}, want: []string{"target INSERT 3 c"}},
		{name: "accept", sourceV: "x", both: true, conflict: &tree.ConflictOpt{Opt: tree.CONFLICT_ACCEPT}, want: []string{"target UPDATE 1 a", "target UPDATE 1 x", "target INSERT 3 c"}},
	} {
		runCase(tc)
	}

	for _, online := range []bool{true, false} {
		for _, policy := range []struct {
			name   string
			opt    int
			want   []string
			marker bool
		}{
			{name: "fail", opt: tree.CONFLICT_FAIL},
			{name: "skip", opt: tree.CONFLICT_SKIP},
			{name: "accept", opt: tree.CONFLICT_ACCEPT, want: []string{"target DELETE 2 b"}},
			{name: "pick_marker", opt: tree.CONFLICT_ACCEPT, want: []string{"base DELETE 2 local"}, marker: true},
		} {
			tc := testCase{sourceV: "x"}
			tc.name = fmt.Sprintf("bounded_delete_online_%t_%s", online, policy.name)
			tc.pick, tc.boundedDelete, tc.async = 2, true, !online
			tc.conflict = &tree.ConflictOpt{Opt: policy.opt}
			tc.want, tc.wantErr, tc.pickMarkers = policy.want, policy.opt == tree.CONFLICT_FAIL, policy.marker
			runCase(tc)
		}
	}
}

func TestDataBranchFakeClearDeletesUseEachLiveKeyOnce(t *testing.T) {
	ses := newValidateSession(t)
	tables := newTestBranchTableStuff(gomock.NewController(t))
	tables.def.colNames = []string{"v", catalog.FakePrimaryKeyColName}
	tables.def.colTypes = []types.Type{types.T_varchar.ToType(), types.T_uint64.ToType()}
	tables.def.visibleIdxes, tables.def.commonVisibleIdxes = []int{0}, []int{0}
	tables.def.pkKind, tables.def.pkColIdx = fakeKind, 1
	mp := ses.proc.Mp()
	defer tables.retPool.freeAllRetBatches(mp)
	newRows := func(value string, keys ...uint64) *batch.Batch {
		bat := tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
		for _, key := range keys {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], key, false, mp))
		}
		bat.SetRowCount(len(keys))
		return bat
	}
	store, err := databranchutils.NewBranchHashmap()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	live := newRows("a", 30, 40, 50)
	defer tables.retPool.releaseRetBatch(live, false)
	require.NoError(t, store.PutByVectors(live.Vecs, []int{0}))
	deletes := newRows("a", 10, 20)
	defer tables.retPool.releaseRetBatch(deletes, false)
	require.NoError(t, matchDataBranchFakeClearDeletes(
		t.Context(), ses, tables, batchWithKind{batch: deletes}, store, []int{0}, true, nil,
	))
	keys := vector.MustFixedColNoTypeCheck[uint64](deletes.Vecs[1])
	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1])
	for _, key := range keys {
		require.Contains(t, []uint64{30, 40, 50}, key)
	}
	require.EqualValues(t, 1, store.ItemCount())
}

func TestDataBranchClearColumnLineage(t *testing.T) {
	def := func(id uint64, first, second string) *plan.TableDef {
		return &plan.TableDef{TblId: id, Cols: []*plan.ColDef{
			{Name: first, ColId: 2, Seqnum: 1},
			{Name: second, ColId: 3, Seqnum: 2},
		}}
	}
	for _, tc := range []struct {
		name string
		defs []*plan.TableDef
	}{
		{"same_generation", []*plan.TableDef{def(1, "v", "w"), def(1, "renamed", "v"), def(4, "renamed", "v")}},
		{"copy_chain", []*plan.TableDef{def(1, "v", "w"), def(2, "renamed", "w"), def(3, "renamed", "v"), def(4, "renamed", "v")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			end := tc.defs[len(tc.defs)-1]
			added := &plan.ColDef{Name: "extra", ColId: 4, Seqnum: 3, Default: &plan.Default{
				Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 0}}}},
			}}
			withExtra := &plan.TableDef{TblId: 5, Cols: append([]*plan.ColDef{}, end.Cols...)}
			withExtra.Cols = append(withExtra.Cols, added)
			defs := append(tc.defs, withExtra)
			lineageOnly := make([]bool, len(defs))
			for i := 1; i < len(defs); i++ {
				lineageOnly[i] = true
			}
			got := resolveDataBranchClearColumns([]string{"renamed", "v", "extra"}, defs, lineageOnly)
			require.Same(t, tc.defs[0].Cols[0], got.present[0])
			require.Same(t, tc.defs[0].Cols[1], got.present[1])
			require.Nil(t, got.present[2])
			require.Same(t, added, got.introduced[2])
		})
	}
}

func TestDataBranchClearUsesIntroductionDefault(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	tables := newTestBranchTableStuff(ctrl)
	mp := ses.proc.Mp()
	defer tables.retPool.freeAllRetBatches(mp)
	ancestor := newTestBranchTableDef("ancestor", "name")
	ancestor.Cols = append(ancestor.Cols[:1:1], ancestor.Cols[2])
	lca := mock_frontend.NewMockRelation(ctrl)
	lca.EXPECT().GetTableDef(gomock.Any()).Return(ancestor).AnyTimes()
	lca.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
	withDefault := func(value string) *plan.TableDef {
		def := newTestBranchTableDef("target", "name")
		def.Cols[1].Default = &plan.Default{Expr: &plan.Expr{
			Typ: def.Cols[1].Typ,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: value},
			}},
		}}
		return def
	}
	columns := resolveDataBranchClearColumns(tables.def.colNames, []*plan.TableDef{
		ancestor, withDefault("original"), withDefault("later"),
	}, []bool{false, true, true})
	originalScan := scanSnapshotRelationWithPreopened
	t.Cleanup(func() { scanSnapshotRelationWithPreopened = originalScan })
	scanSnapshotRelationWithPreopened = func(
		_ context.Context, _ string, _ *Session, _ uint64, _ types.TS, _ engine.Relation,
		attrs []string, typs []types.Type, _ *plan.Expr, _ int, visit func(*batch.Batch) error,
	) error {
		require.Equal(t, []string{"id", "hidden"}, attrs)
		rows := batch.NewWithSize(len(typs))
		defer rows.Clean(mp)
		for i, typ := range typs {
			rows.Vecs[i] = vector.NewVec(typ, mp)
		}
		require.NoError(t, vector.AppendFixed(rows.Vecs[0], int64(1), false, mp))
		require.NoError(t, vector.AppendBytes(rows.Vecs[1], []byte("hidden"), false, mp))
		rows.SetRowCount(1)
		return visit(rows)
	}
	for _, value := range []string{"original", "later"} {
		t.Run(value, func(t *testing.T) {
			h, err := databranchutils.NewBranchHashmap()
			require.NoError(t, err)
			defer func() { require.NoError(t, h.Close()) }()
			rows := buildHashDiffDataBatch(t, mp, [][]any{{int64(1), value, "hidden", ""}})
			defer rows.Clean(mp)
			require.NoError(t, h.PutByVectors(rows.Vecs[1:4], []int{0}))
			updates := 0
			require.NoError(t, reconcileDataBranchClear(
				t.Context(), ses, tables, dataBranchSnapshotRelation{rel: lca, columns: columns},
				nil, "target", diffSideTarget, false, h, nil, nil, func(w batchWithKind) error {
					defer tables.retPool.releaseRetBatch(w.batch, false)
					require.Equal(t, diffUpdate, w.kind)
					require.Equal(t, value, string(w.batch.Vecs[1].GetBytesAt(0)))
					updates += w.batch.RowCount()
					return nil
				},
			))
			if value == "original" {
				require.Zero(t, updates)
			} else {
				require.Equal(t, 1, updates)
			}
		})
	}
}

func TestHashDiffDestinationClearSpillsDeletes(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()
	ses.txnHandler = &TxnHandler{txnOp: txnOp}
	tables := newTestBranchTableStuff(ctrl)
	tables.baseVisibleState = true
	spillRoot := t.TempDir()
	t.Setenv("TMPDIR", spillRoot)
	tables.hashmapAllocator = &branchHashmapAllocator{
		upstream:  malloc.GetDefault(nil),
		throttler: rscthrottler.NewMemThrottler("DataBranchClearTest", 1, rscthrottler.WithConstLimit(1)),
	}
	mp := ses.proc.Mp()
	defer tables.retPool.freeAllRetBatches(mp)
	probe := types.BuildTS(10, 0)
	lca := mock_frontend.NewMockRelation(ctrl)
	lca.EXPECT().GetTableDef(gomock.Any()).Return(newTestBranchTableDef("ancestor", "name")).AnyTimes()
	lca.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
	tables.lcaRel = lca
	tables.lcaRelSnapshot = probe
	originalScan := scanSnapshotRelationWithPreopened
	t.Cleanup(func() { scanSnapshotRelationWithPreopened = originalScan })
	const batchRows, batches = 64, 3
	scanSnapshotRelationWithPreopened = func(
		_ context.Context, _ string, _ *Session, _ uint64, _ types.TS, _ engine.Relation,
		_ []string, typs []types.Type, _ *plan.Expr, _ int, visit func(*batch.Batch) error,
	) error {
		rows := batch.NewWithSize(len(typs))
		defer rows.Clean(mp)
		for i, typ := range typs {
			rows.Vecs[i] = vector.NewVec(typ, mp)
		}
		for n := range batches {
			rows.CleanOnlyData()
			for i := range batchRows {
				require.NoError(t, vector.AppendFixed(rows.Vecs[0], int64(n*batchRows+i), false, mp))
				require.NoError(t, vector.AppendBytes(rows.Vecs[1], []byte("value"), false, mp))
				require.NoError(t, vector.AppendBytes(rows.Vecs[2], []byte("hidden"), false, mp))
			}
			rows.SetRowCount(batchRows)
			if err := visit(rows); err != nil {
				return err
			}
			spills, err := filepath.Glob(filepath.Join(spillRoot, "branch-hashmap-shard-*"))
			require.NoError(t, err)
			require.NotEmpty(t, spills)
		}
		return nil
	}
	seen := make(map[int64]bool)
	require.NoError(t, hashDiff(t.Context(), ses, nil, tables, branchMetaInfo{
		lcaTableId: 77, pathFromLCAToTarTS: []types.TS{{}, probe},
	}, compositeOption{onlineApply: true}, func(w batchWithKind) (bool, error) {
		defer tables.retPool.releaseRetBatch(w.batch, false)
		if w.batch.RowCount() == 0 {
			return false, nil
		}
		require.Equal(t, diffDelete, w.kind)
		require.Equal(t, diffSideBase, w.side)
		for _, key := range vector.MustFixedColNoTypeCheck[int64](w.batch.Vecs[0]) {
			require.False(t, seen[key])
			seen[key] = true
		}
		return false, nil
	}, nil, nil, nil))
	require.Len(t, seen, batches*batchRows)
	for i := range batches * batchRows {
		require.True(t, seen[int64(i)])
	}
	spills, err := filepath.Glob(filepath.Join(spillRoot, "branch-hashmap-shard-*"))
	require.NoError(t, err)
	require.Empty(t, spills)
}
