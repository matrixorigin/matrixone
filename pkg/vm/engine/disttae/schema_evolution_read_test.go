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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	idType, boolType := types.T_int64.ToType(), types.T_bool.ToType()
	table.tableDef.Cols = []*plan.ColDef{
		{Name: "id", Typ: idType.PlanType(), Primary: true},
		{Name: "flag", Seqnum: 1, Typ: boolType.PlanType(), Default: &plan.Default{
			NullAbility: true, Expr: plan2.MakePlan2BoolConstExprWithType(true),
		}},
		{Name: objectio.PhysicalAddr_Attr, Seqnum: objectio.SEQNUM_ROWID, Typ: objectio.RowidType.PlanType(), Hidden: true},
	}
	table.tableDef.FeatureFlag = features.SchemaEvolved
	table.tableDef.Name2ColIndex = map[string]int32{"id": 0, "flag": 1, objectio.PhysicalAddr_Attr: 2}
	snapshot := types.BuildTS(6, 0)
	txn.op.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().Return(snapshot.ToTimestamp()).AnyTimes()
	commit := types.BuildTS(2, 0)

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(idType, mp)
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
			Default: &plan.Default{NullAbility: true},
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
		require.Equal(t, [][2]any{{int64(1), int64(1)}, {nil, nil}, {nil, nil}}, values)
		checkInfo := func(info *plan.MetadataScanInfo) {
			t.Helper()
			require.EqualValues(t, 1, info.RowCnt)
			zm := objectio.ZoneMap(info.ZoneMap)
			require.Equal(t, info.RowCnt, info.NullCnt)
			require.False(t, zm.IsInited())
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

		presentBatch := batch.NewWithSize(2)
		presentBatch.Vecs[0], presentBatch.Vecs[1] = vector.NewVec(idType, mp), vector.NewVec(boolType, mp)
		defer presentBatch.Clean(mp)
		require.NoError(t, vector.AppendFixed(presentBatch.Vecs[0], int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(presentBatch.Vecs[1], true, false, mp))
		presentBatch.SetRowCount(1)
		// Physical values contribute extrema alongside the older all-NULL column.
		presentWriter := ioutil.ConstructWriterWithSegmentID(segment, 0, 0, []uint16{0, 1}, -1, false, false, fs, nil)
		_, err = presentWriter.WriteBatch(presentBatch)
		require.NoError(t, err)
		_, _, err = presentWriter.Sync(ctx)
		require.NoError(t, err)
		ps, done = part.MutateState()
		require.NoError(t, ps.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: presentWriter.GetObjectStats(), CreateTime: commit}, false))
		done()
		values, _, err = table.MaxAndMinValues(ctx)
		require.NoError(t, err)
		require.Equal(t, [][2]any{{int64(1), int64(2)}, {true, true}, {nil, nil}}, values)
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
		require.False(t, zms[0].IsInited())
		require.Equal(t, []uint32{1}, nulls)
	})
	t.Run("statistics", func(t *testing.T) {
		info := plan2.NewTableStatsInfo(2)
		_, err := collectTableStats(ctx, &updateStatsRequest{tableDef: table.tableDef, partitionState: state, fs: fs, ts: snapshot, samplingMode: "full"}, info, nil)
		require.NoError(t, err)
		require.Equal(t, float64(1), info.TableRowCount)
		require.Equal(t, int64(1), info.NullCnts[1])
		require.Zero(t, info.ColumnNDVs[1])
		require.Equal(t, int64(0), info.ColumnSize[1])
		require.False(t, info.ColumnZMs[1].IsInited())
	})
	t.Run("slow zone map filtering", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			typ   types.T
			test  string
			value bool
			want  int
		}{
			{"conservative equality with no bounds", types.T_bool, "", true, 1},
			{"admits is null", types.T_bool, "isnull", true, 1},
			{"excludes is not null", types.T_bool, "isnotnull", true, 0},
			{"admits array is null", types.T_array_float32, "isnull", true, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				def := plan2.DeepCopyTableDef(table.tableDef, true)
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
		rowIDVec := vector.NewVec(objectio.RowidType, mp)
		tsVec := vector.NewVec(types.T_TS.ToType(), mp)
		idVec := vector.NewVec(idType, mp)
		defer rowIDVec.Free(mp)
		defer tsVec.Free(mp)
		defer idVec.Free(mp)
		require.NoError(t, vector.AppendFixed(rowIDVec, buildTestMaterializedSnapshotRowID(t, 2), false, mp))
		require.NoError(t, vector.AppendFixed(tsVec, commit, false, mp))
		require.NoError(t, vector.AppendFixed(idVec, int64(2), false, mp))
		packer := types.NewPacker()
		defer packer.Close()
		state.HandleRowsInsert(ctx, mustMarshalVectors(rowIDVec, tsVec, idVec),
			[]string{objectio.PhysicalAddr_Attr, objectio.DefaultCommitTS_Attr, "id"}, 0, packer, mp)
		source, err := NewLocalDataSource(ctx, table, 0, state, nil, nil, false, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
		require.NoError(t, err)
		defer source.Close()
		source.memPKFilter = &readutil.MemPKFilter{}
		output := batch.NewWithSize(2)
		defer output.Clean(mp)
		output.Vecs[0], output.Vecs[1] = vector.NewVec(idType, mp), vector.NewVec(boolType, mp)
		require.NoError(t, source.filterInMemCommittedInserts(ctx, []types.Type{idType, boolType}, []uint16{0, 1}, mp, output))
		require.Equal(t, 1, output.RowCount())
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0))
		require.True(t, output.Vecs[1].IsNull(0))

		for _, seq := range []uint16{1, 40000} {
			def := plan2.DeepCopyTableDef(table.tableDef, true)
			def.Cols[1].Seqnum = uint32(seq)
			filter := bloomfilter.NewCBloomFilterWithSeed(1024, 3, 1)
			defer filter.Free()
			filter.Add([]byte{0})
			reader, err := NewLocalDataSource(ctx, table, 0, state, nil, nil, false, engine.Policy_SkipAll, engine.GeneralLocalDataSource)
			require.NoError(t, err)
			defer reader.Close()
			reader.readSpec = objectio.NewColumnReadSpecFromTableDef(def)
			reader.memPKFilter = &readutil.MemPKFilter{
				HasBF: true, BFSeqNum: int16(seq), FilterHint: engine.FilterHint{BF: filter},
			}
			out := batch.NewWithSize(1)
			defer out.Clean(mp)
			out.Vecs[0] = vector.NewVec(idType, mp)
			require.NoError(t, reader.filterInMemCommittedInserts(ctx, []types.Type{idType}, []uint16{0}, mp, out))
			require.Zero(t, out.RowCount(), "missing column %d is NULL and cannot match the filter", seq)
		}
	})
	t.Run("four rows across replay and storage", func(t *testing.T) {
		current := batch.NewWithSize(2)
		current.Vecs[0], current.Vecs[1] = vector.NewVec(idType, mp), vector.NewVec(boolType, mp)
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
		data := batch.NewWithSize(2)
		data.Vecs[0], data.Vecs[1] = vector.NewVec(idType, mp), vector.NewVec(boolType, mp)
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
		require.Equal(t, map[int64]any{1: nil, 2: nil, 3: true, 4: nil}, rows)
	})
}

func TestWorkspaceReadsWithGeneratedRowID(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "id")
	defer txn.closeWorkspaceAllocation()
	defer func() {
		for _, entry := range txn.writes {
			entry.bat.Clean(txn.workspaceMP())
		}
	}()
	ctx := defines.AttachAccountId(t.Context(), 1)
	mp := txn.proc.Mp()
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	txn.tnStores = []DNStore{{}}
	txn.writeWorkspaceThreshold = 64 << 20

	table := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	table.proc.Store(txn.proc)
	table.extraInfo = &api.SchemaExtra{}
	table.primaryIdx, table.primarySeqnum = 0, 0
	typ := types.T_int64.ToType()
	table.defs = []engine.TableDef{
		&engine.AttributeDef{Attr: engine.Attribute{Name: "id", Type: typ, Primary: true}},
		&engine.AttributeDef{Attr: engine.Attribute{Name: "value", Type: typ, Seqnum: 1}},
	}
	table.tableDef = nil
	def := table.GetTableDef(ctx)
	require.Len(t, def.Cols, 3)
	require.Equal(t, catalog.Row_ID, def.Cols[2].Name)

	data := batch.NewWithSize(2)
	defer data.Clean(mp)
	for i, value := range []int64{10, 42} {
		data.Vecs[i] = vector.NewVec(typ, mp)
		require.NoError(t, vector.AppendFixed(data.Vecs[i], value, false, mp))
	}
	data.SetRowCount(1)
	require.NoError(t, table.Write(ctx, data, []string{"id", "value"}))
	require.Len(t, txn.writes, 1)

	source, err := NewLocalDataSource(ctx, table, len(txn.writes), nil, nil, nil, false, engine.Policy_SkipAll, engine.ShardingLocalDataSource)
	require.NoError(t, err)
	defer source.Close()
	out := batch.NewWithSize(3)
	defer out.Clean(mp)
	for i, typ := range []types.Type{typ, typ, objectio.RowidType} {
		out.Vecs[i] = vector.NewVec(typ, mp)
	}
	_, state, err := source.Next(ctx, []string{"id", "value", catalog.Row_ID},
		[]types.Type{typ, typ, objectio.RowidType}, []uint16{0, 1, objectio.SEQNUM_ROWID},
		0, &readutil.MemPKFilter{}, mp, out)
	require.NoError(t, err)
	require.Equal(t, engine.InMem, state)
	require.Equal(t, 1, out.RowCount())
	require.Equal(t, []int64{10}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
	require.Equal(t, []int64{42}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[1]))
	require.Equal(t, vector.MustFixedColWithTypeCheck[types.Rowid](txn.writes[0].bat.Vecs[0]),
		vector.MustFixedColWithTypeCheck[types.Rowid](out.Vecs[2]))
}

func TestWorkspaceReadsSparseColumnSequences(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	defer txn.closeWorkspaceAllocation()
	defer func() {
		for _, entry := range txn.writes {
			entry.bat.Clean(txn.workspaceMP())
		}
	}()
	ctx := defines.AttachAccountId(t.Context(), 1)
	mp := txn.proc.Mp()
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	txn.tnStores = []DNStore{{}}
	txn.writeWorkspaceThreshold = 64 << 20
	packer := types.NewPacker()
	defer packer.Close()
	txn.engine.packerPool = fileservice.NewPool(1, func() *types.Packer { return packer }, (*types.Packer).Reset, nil)
	table := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, table)
	table.proc.Store(txn.proc)
	table.extraInfo = &api.SchemaExtra{}
	table.primaryIdx, table.primarySeqnum = 1, 3
	typ := types.T_int64.ToType()
	attrs := []string{catalog.IndexTablePrimaryColName, "pk", "original"}
	table.tableDef = &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: attrs[0], Seqnum: 9, Typ: typ.PlanType()},
			{Name: attrs[1], Seqnum: 3, Typ: typ.PlanType(), Primary: true},
			{Name: attrs[2], Seqnum: 0, Typ: typ.PlanType()},
			plan2.MakeRowIdColDef(),
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "pk"},
		Name2ColIndex: map[string]int32{attrs[0]: 0, attrs[1]: 1, attrs[2]: 2, catalog.Row_ID: 3},
	}
	input := batch.NewWithSize(len(attrs))
	defer input.Clean(mp)
	want := [][]int64{{100, 200, 300}, {11, 22, 33}, {901, 902, 903}}
	for col, vals := range want {
		input.Vecs[col] = vector.NewVec(typ, mp)
		for _, value := range vals {
			require.NoError(t, vector.AppendFixed(input.Vecs[col], value, false, mp))
		}
	}
	input.SetRowCount(3)
	require.NoError(t, table.Write(ctx, input, attrs))
	require.Len(t, txn.writes, 1)
	require.Equal(t, 2, txn.writes[0].pkCheckPos)

	bf := bloomfilter.NewCBloomFilterWithSeed(1024, 3, 1)
	defer bf.Free()
	bf.Add(input.Vecs[0].GetRawBytesAt(1))
	equal := readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: types.EncodeFixed(int64(22))}
	for _, tc := range []struct {
		name string
		cols []string
		seqs []uint16
		base readutil.BasePKFilter
		hint engine.FilterHint
		want [][]int64
	}{
		{name: "all", cols: attrs, seqs: []uint16{9, 3, 0}, want: want},
		{name: "subset", cols: []string{"original", attrs[0]}, seqs: []uint16{0, 9}, want: [][]int64{want[2], want[0]}},
		{name: "primary key", cols: []string{"original"}, seqs: []uint16{0}, base: equal, want: [][]int64{{902}}},
		{name: "bloom only", cols: []string{"pk"}, seqs: []uint16{3}, hint: engine.FilterHint{BF: bf}, want: [][]int64{{22}}},
		{name: "primary key and bloom", cols: []string{"original"}, seqs: []uint16{0}, base: equal, hint: engine.FilterHint{BF: bf}, want: [][]int64{{902}}},
		{name: "invalid sequence", cols: []string{"missing"}, seqs: []uint16{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := NewLocalDataSource(ctx, table, len(txn.writes), nil, nil, nil, false, engine.Policy_SkipAll, engine.ShardingLocalDataSource)
			require.NoError(t, err)
			defer src.Close()
			filter, err := readutil.NewMemPKFilter(table.tableDef, timestamp.Timestamp{}, txn.engine.packerPool, tc.base, tc.hint)
			require.NoError(t, err)
			if tc.hint.BF != nil {
				require.Equal(t, int16(9), filter.BFSeqNum)
			}
			out := batch.NewWithSize(len(tc.cols))
			defer out.Clean(mp)
			typs := make([]types.Type, len(tc.cols))
			for i := range out.Vecs {
				typs[i] = typ
				out.Vecs[i] = vector.NewVec(typ, mp)
			}
			_, state, err := src.Next(ctx, tc.cols, typs, tc.seqs, int32(table.primarySeqnum), &filter, mp, out)
			require.Equal(t, engine.InMem, state)
			if tc.want == nil {
				require.ErrorContains(t, err, "invalid workspace INSERT column position")
				require.Zero(t, out.RowCount())
				return
			}
			require.NoError(t, err)
			require.Equal(t, len(tc.want[0]), out.RowCount())
			for i, vals := range tc.want {
				require.Equal(t, vals, vector.MustFixedColWithTypeCheck[int64](out.Vecs[i]))
			}
		})
	}
}
