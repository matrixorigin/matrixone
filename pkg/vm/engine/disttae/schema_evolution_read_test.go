// Copyright 2021-2024 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolutionDisttaeOracle(t *testing.T) {
	ctx := t.Context()
	txn := newTransactionWithActivePKTableForTest(t, "id")
	table := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	table.proc.Store(txn.proc)
	mp := txn.proc.Mp()
	fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	txn.engine.fs = fs
	frozen := &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}
	idType, boolType := types.T_int64.ToType(), types.T_bool.ToType()
	table.tableDef.Cols = []*plan.ColDef{
		{Name: "id", Typ: idType.PlanType(), Primary: true},
		{Name: "flag", Seqnum: 1, Typ: boolType.PlanType(), Default: &plan.Default{
			NullAbility: true, HasOriginDefault: true, OriginDefault: frozen, Expr: plan2.MakePlan2BoolConstExprWithType(true),
		}},
		{Name: objectio.PhysicalAddr_Attr, Seqnum: objectio.SEQNUM_ROWID, Typ: objectio.RowidType.PlanType(), Hidden: true},
	}
	table.tableDef.FeatureFlag = features.SchemaEvolved
	table.tableDef.Name2ColIndex = map[string]int32{"id": 0, "flag": 1, objectio.PhysicalAddr_Attr: 2}
	snapshot := types.BuildTS(6, 0)
	txn.op.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(snapshot.ToTimestamp()).AnyTimes()
	commit := types.BuildTS(2, 0)

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(idType)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, mp))
	input.SetRowCount(1)
	segment := objectio.NewSegmentid()
	writer := ioutil.ConstructWriterWithSegmentID(segment, 1, 0, []uint16{0}, -1, false, false, fs, nil)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	state := logtailreplay.NewPartitionState("", false, table.tableId, false)
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: commit}, false))
	t.Run("object column metadata", func(t *testing.T) {
		original := table.tableDef
		defer func() { table.tableDef = original }()
		table.tableDef = plan2.DeepCopyTableDef(original, true)
		cols := table.tableDef.Cols
		table.tableDef.Cols = []*plan.ColDef{cols[0], cols[1], {
			Name: "nullable", Seqnum: 2, Typ: boolType.PlanType(),
			Default: &plan.Default{NullAbility: true, HasOriginDefault: true, OriginDefault: &plan.Literal{Isnull: true}},
		}, cols[2]}
		txn.op.(*mock_frontend.MockTxnOperator).EXPECT().IsSnapOp().Return(false).AnyTimes()
		txn.tableOps.addCreatedInTxn(table.tableId, 0)
		txn.proc.Base.FileService = fs
		txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
		part := txn.engine.GetOrCreateLatestPart(ctx, uint64(table.accountId), table.db.databaseId, table.tableId)
		ps, done := part.MutateState()
		ps.UpdateDuration(types.TS{}, types.MaxTs())
		require.NoError(t, ps.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: commit}, false))
		done()

		values, typs, err := table.MaxAndMinValues(ctx)
		require.NoError(t, err)
		require.Equal(t, []uint8{uint8(types.T_int64), uint8(types.T_bool), uint8(types.T_bool)}, typs)
		require.Equal(t, [][2]any{{int64(1), int64(1)}, {false, false}, {nil, nil}}, values)
		checkInfo := func(info *plan.MetadataScanInfo) {
			t.Helper()
			require.EqualValues(t, 1, info.RowCnt)
			zm := objectio.ZoneMap(info.ZoneMap)
			if info.ColName == "nullable" {
				require.Equal(t, info.RowCnt, info.NullCnt)
				require.False(t, zm.IsInited())
			} else {
				require.Zero(t, info.NullCnt)
				require.True(t, zm.IsInited())
				require.Equal(t, false, zm.GetMin())
				require.Equal(t, false, zm.GetMax())
			}
		}
		for _, name := range []string{"flag", "nullable"} {
			info, err := table.GetColumMetadataScanInfo(ctx, name, false)
			require.NoError(t, err)
			require.Len(t, info, 1)
			require.Equal(t, name, info[0].ColName)
			checkInfo(info[0])
		}
		info, err := table.GetColumMetadataScanInfo(ctx, AllColumns, false)
		require.NoError(t, err)
		require.Len(t, info, 3)
		require.Equal(t, "flag", info[1].ColName)
		require.Equal(t, "nullable", info[2].ColName)
		checkInfo(info[1])
		checkInfo(info[2])

		nullBatch := batch.NewWithSize(2)
		nullBatch.Vecs[0], nullBatch.Vecs[1] = vector.NewVec(idType), vector.NewVec(boolType)
		defer nullBatch.Clean(mp)
		require.NoError(t, vector.AppendFixed(nullBatch.Vecs[0], int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(nullBatch.Vecs[1], false, true, mp))
		nullBatch.SetRowCount(1)
		// Give the all-NULL object the lower name so the extrema scan sees
		// its untyped object zone map before the frozen default.
		nullWriter := ioutil.ConstructWriterWithSegmentID(segment, 0, 0, []uint16{0, 1}, -1, false, false, fs, nil)
		_, err = nullWriter.WriteBatch(nullBatch)
		require.NoError(t, err)
		_, _, err = nullWriter.Sync(ctx)
		require.NoError(t, err)
		ps, done = part.MutateState()
		require.NoError(t, ps.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: nullWriter.GetObjectStats(), CreateTime: commit}, false))
		done()
		values, _, err = table.MaxAndMinValues(ctx)
		require.NoError(t, err)
		require.Equal(t, [][2]any{{int64(1), int64(2)}, {false, false}, {nil, nil}}, values)
	})
	t.Run("ordered scan", func(t *testing.T) {
		var blocks objectio.BlockInfoSlice
		blk := stats.ConstructBlockInfo(0)
		blocks.AppendBlockInfo(&blk)
		source, err := NewLocalDataSource(ctx, table, 0, state, blocks, nil, true, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
		require.NoError(t, err)
		defer source.Close()
		source.OrderBy = []*plan.OrderBySpec{{Expr: readutil.MakeColExprForTest(1, types.T_bool, "flag")}}
		zms, nulls, err := source.getBlockZMs(ctx)
		require.NoError(t, err)
		require.Len(t, zms, 1)
		require.True(t, zms[0].IsInited())
		require.Equal(t, false, zms[0].GetMin())
		require.Equal(t, false, zms[0].GetMax())
		require.Equal(t, []uint32{0}, nulls)
	})
	t.Run("statistics", func(t *testing.T) {
		info := plan2.NewTableStatsInfo(2)
		_, err := collectTableStats(ctx, &updateStatsRequest{tableDef: table.tableDef, partitionState: state, fs: fs, ts: snapshot, samplingMode: "full"}, info, nil)
		require.NoError(t, err)
		require.Equal(t, float64(1), info.TableRowCount)
		require.Equal(t, int64(0), info.NullCnts[1])
		require.Equal(t, float64(1), info.ColumnNDVs[1])
		require.Equal(t, int64(0), info.ColumnSize[1])
		require.Equal(t, false, info.ColumnZMs[1].GetMin())
		require.Equal(t, false, info.ColumnZMs[1].GetMax())
	})
	t.Run("slow zone map filtering", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			typ    types.T
			frozen *plan.Literal
			test   string
			value  bool
			want   int
		}{
			{"equals frozen false", types.T_bool, frozen, "", false, 1},
			{"excludes true", types.T_bool, frozen, "", true, 0},
			{"excludes is null", types.T_bool, frozen, "isnull", true, 0},
			{"admits is null", types.T_bool, &plan.Literal{Isnull: true}, "isnull", true, 1},
			{"excludes is not null", types.T_bool, &plan.Literal{Isnull: true}, "isnotnull", true, 0},
			{"array with no zone map is not null", types.T_array_float32, &plan.Literal{Value: &plan.Literal_Sval{Sval: "[1,2]"}}, "isnotnull", true, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				def := plan2.DeepCopyTableDef(table.tableDef, true)
				def.Cols[1].Default.OriginDefault = tc.frozen
				def.Cols[1].Typ = tc.typ.ToType().PlanType()
				if tc.typ.IsArray() {
					def.Cols[1].Typ.Width = 2
				}
				bind := func(name string, args ...*plan.Expr) *plan.Expr {
					expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, name, args)
					require.NoError(t, err)
					return expr
				}
				col := readutil.MakeColExprForTest(1, tc.typ, "flag")
				col.Typ = def.Cols[1].Typ
				var condition *plan.Expr
				if tc.test == "" {
					condition = bind("=", col, plan2.MakePlan2BoolConstExprWithType(tc.value))
				} else {
					condition = bind(tc.test, col)
				}
				// The outer comparison has no direct column argument, routing
				// the expression through the scalar/recursive metadata evaluator.
				filter := bind("=", condition, plan2.MakePlan2BoolConstExprWithType(true))
				_, _, _, _, _, fast, _ := readutil.CompileFilterExpr(filter, def, fs)
				require.False(t, fast)
				var blocks objectio.BlockInfoSlice
				require.NoError(t, table.rangesOnePart(ctx, state, def, engine.RangesParam{
					BlockFilters: []*plan.Expr{filter}, Policy: engine.Policy_CollectCommittedPersistedData,
				}, &blocks, txn.proc, nil))
				require.Equal(t, tc.want, blocks.Len())
			})
		}
	})
	t.Run("replayed row", func(t *testing.T) {
		protoVec := func(typ types.Type, val any) api.Vector {
			v := vector.NewVec(typ)
			defer v.Free(mp)
			require.NoError(t, vector.AppendAny(v, val, false, mp))
			proto, err := vector.VectorToProtoVector(v)
			require.NoError(t, err)
			return proto
		}
		packer := types.NewPacker()
		defer packer.Close()
		state.HandleRowsInsert(ctx, &api.Batch{
			Attrs: []string{objectio.PhysicalAddr_Attr, objectio.DefaultCommitTS_Attr, "id"},
			Vecs: []api.Vector{
				protoVec(objectio.RowidType, buildTestMaterializedSnapshotRowID(t, 2)),
				protoVec(types.T_TS.ToType(), commit), protoVec(idType, int64(2)),
			},
		}, 0, packer, mp)
		source, err := NewLocalDataSource(ctx, table, 0, state, nil, nil, false, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
		require.NoError(t, err)
		defer source.Close()
		source.memPKFilter = &readutil.MemPKFilter{}
		output := batch.New([]string{"id", "flag"})
		defer output.Clean(mp)
		output.Vecs[0], output.Vecs[1] = vector.NewVec(idType), vector.NewVec(boolType)
		require.NoError(t, source.filterInMemCommittedInserts(ctx, []types.Type{idType, boolType}, []uint16{0, 1}, mp, output))
		require.Equal(t, 1, output.RowCount())
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0))
		require.False(t, output.Vecs[1].IsNull(0))
		require.False(t, vector.GetFixedAtNoTypeCheck[bool](output.Vecs[1], 0))

		for _, tc := range []struct {
			name   string
			seq    uint16
			frozen *plan.Literal
			want   int
		}{
			{"NULL defaults are skipped", 1, &plan.Literal{Isnull: true}, 0},
			{"matching default keeps the row", 1, frozen, 1},
			{"missing default in filter skips the row", 1, &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}, 0},
			{"matching sparse default keeps the row", 40000, frozen, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				def := plan2.DeepCopyTableDef(table.tableDef, true)
				def.Cols[1].Seqnum = uint32(tc.seq)
				def.Cols[1].Default.OriginDefault = tc.frozen
				filter := bloomfilter.NewCBloomFilterWithSeed(1024, 3, 1)
				defer filter.Free()
				filter.Add([]byte{0})
				require.False(t, filter.Test([]byte{1}))
				reader, err := NewLocalDataSource(ctx, table, 0, state, nil, nil, false, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
				require.NoError(t, err)
				defer reader.Close()
				reader.readSpec = objectio.NewColumnReadSpecFromTableDef(def)
				reader.memPKFilter = &readutil.MemPKFilter{
					HasBF: true, BFSeqNum: int16(tc.seq), FilterHint: engine.FilterHint{BF: filter},
				}
				out := batch.New([]string{"id"})
				defer out.Clean(mp)
				out.Vecs[0] = vector.NewVec(idType)
				require.NoError(t, reader.filterInMemCommittedInserts(ctx, []types.Type{idType}, []uint16{0}, mp, out))
				require.Equal(t, tc.want, out.RowCount())
				if tc.want != 0 {
					require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0))
				}
			})
		}
	})
	t.Run("four rows across replay and storage", func(t *testing.T) {
		current := batch.NewWithSize(2)
		current.Vecs[0], current.Vecs[1] = vector.NewVec(idType), vector.NewVec(boolType)
		defer current.Clean(mp)
		for _, id := range []int64{3, 4} {
			require.NoError(t, vector.AppendFixed(current.Vecs[0], id, false, mp))
			require.NoError(t, vector.AppendFixed(current.Vecs[1], true, id == 4, mp))
		}
		current.SetRowCount(2)
		writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
		_, err := writer.WriteBatch(current)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		var blocks objectio.BlockInfoSlice
		for _, obj := range []objectio.ObjectStats{stats, writer.GetObjectStats()} {
			blk := obj.ConstructBlockInfo(0)
			blocks.AppendBlockInfo(&blk)
		}
		source, err := NewLocalDataSource(ctx, table, 0, state, blocks, nil, false, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
		require.NoError(t, err)
		r, err := readutil.NewReader(ctx, mp, nil, fs, table.tableDef, snapshot.ToTimestamp(), nil, source, 0, engine.FilterHint{})
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		data := batch.New([]string{"id", "flag"})
		data.Vecs[0], data.Vecs[1] = vector.NewVec(idType), vector.NewVec(boolType)
		defer data.Clean(mp)
		rows := make(map[int64]any)
		for range 3 {
			done, err := r.Read(ctx, []string{"id", "flag"}, nil, mp, data)
			require.NoError(t, err)
			require.False(t, done)
			for i := range data.RowCount() {
				id := vector.GetFixedAtNoTypeCheck[int64](data.Vecs[0], i)
				require.NotContains(t, rows, id, "each visible row must be returned once")
				if data.Vecs[1].IsNull(uint64(i)) {
					rows[id] = nil
				} else {
					rows[id] = vector.GetFixedAtNoTypeCheck[bool](data.Vecs[1], i)
				}
			}
		}
		done, err := r.Read(ctx, []string{"id", "flag"}, nil, mp, data)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, map[int64]any{1: false, 2: false, 3: true, 4: nil}, rows)
	})
}
