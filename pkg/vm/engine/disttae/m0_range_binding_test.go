//go:build mo_m0

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
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"testing"

	"github.com/golang/mock/gomock"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestM0RangesBindMissingBeforePruning(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(3)
	t.Cleanup(func() { bat.Clean(mp) })
	for i := range bat.Vecs {
		typ := types.T_int32.ToType()
		if i == 2 {
			typ = types.T_varchar.ToType()
		}
		bat.Vecs[i] = vector.NewVec(typ)
		if i == 2 {
			require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte("key"), false, mp))
		} else {
			require.NoError(t, vector.AppendFixed(bat.Vecs[i], int32(1), false, mp))
		}
	}
	bat.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1, 2}, 2, true, false, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 2}).AnyTimes()
	op.EXPECT().GetWorkspace().Return(&Transaction{engine: &Engine{fs: fs}}).AnyTimes()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	for _, tc := range []struct {
		name       string
		version    uint32
		filter     string
		missing    bool
		wantBlocks int
	}{
		{"inconsistent missing", 0, "c", true, 0},
		{"inconsistent missing with other filter", 0, "a", false, 1},
		{"inconsistent missing rowid with other filter", 0, "a", false, 1},
		{"inconsistent key marker with other filter", 0, "a", false, 1},
		{"missing key descriptor with other filter", 0, "a", false, 1},
		{"unrecognizable key with projected missing column", 0, "a", false, 1},
		{"unrecognizable key without projection", 0, "a", false, 1},
		{"unrecognizable key with incomplete projection", 0, "a", false, 1},
		{"unrecognizable key with foreign projection", 0, "a", false, 1},
		{"unrecognizable key with foreign database", 0, "a", false, 1},
		{"unrecognizable key with stale projection", 0, "a", false, 1},
		{"unrecognizable key with wrong type projection", 0, "a", false, 1},
		{"ordinary complete", 0, "a", false, 0},
		{"ordinary complete bound projection", 0, "a", false, 0},
		{"ordinary sparse unknown", 0, "a", false, 1},
		{"legal append", 1, "c", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := m0TestAppendTableDef()
			table.TblId = 42
			table.Version = tc.version
			for i, col := range table.Cols {
				col.ColId = uint64(i + 1)
			}
			table.Cols[0].Typ.Id = int32(types.T_int32)
			table.Cols[1].Typ.Id = int32(types.T_int32)
			table.Cols[2].Typ.Id = int32(types.T_varchar)
			table.Cols[3].Typ.Id = int32(types.T_int32)
			table.Name2ColIndex = map[string]int32{"a": 0, "b": 1, pkgcatalog.CPrimaryKeyColName: 2, "c": 3, pkgcatalog.Row_ID: 4}
			if tc.name == "inconsistent missing rowid with other filter" {
				table.Cols = table.Cols[:4]
			}
			if tc.name == "inconsistent key marker with other filter" {
				table.Pkey.CompPkeyCol.Hidden = false
			}
			if tc.name == "missing key descriptor with other filter" {
				table.Pkey = nil
			}
			if tc.name == "unrecognizable key with projected missing column" || tc.name == "unrecognizable key without projection" || tc.name == "unrecognizable key with incomplete projection" || tc.name == "unrecognizable key with foreign projection" || tc.name == "unrecognizable key with foreign database" || tc.name == "unrecognizable key with stale projection" || tc.name == "unrecognizable key with wrong type projection" {
				table.Pkey = nil
				table.Cols[2].Name = "damaged_hidden_key"
			}
			if tc.name == "ordinary complete" || tc.name == "ordinary complete bound projection" {
				table.Cols = append(table.Cols[:3], table.Cols[4])
				table.Cols[3].Seqnum = 3
				table.Name2ColIndex = map[string]int32{"a": 0, "b": 1, pkgcatalog.CPrimaryKeyColName: 2, pkgcatalog.Row_ID: 3}
				table.Pkey = nil // Explicit non-M0 control, with all physical columns present.
			}
			if tc.name == "ordinary sparse unknown" {
				table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
				table.Cols[2].Name = "ordinary_value"
				table.Cols[2].Hidden = false
				table.Cols[2].Primary = false
			}
			pos := table.Name2ColIndex[tc.filter]
			arg := &plan.Expr{Typ: table.Cols[pos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: tc.filter, ColPos: pos}}}
			filter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ISNULL, 0), ObjName: "isnull"}, Args: []*plan.Expr{arg}}}}
			tbl := &txnTable{db: &txnDatabase{op: op, databaseName: "db"}, tableName: "customer", tableDef: table}
			var blocks objectio.BlockInfoSlice
			param := engine.RangesParam{BlockFilters: []*plan.Expr{filter}}
			if tc.name == "ordinary complete bound projection" {
				param.Rsp = &engine.RangesShuffleParam{Node: &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: &plan.TableDef{TblId: table.TblId, Version: table.Version, Cols: []*plan.ColDef{table.Cols[0]}}}, CNCNT: 1}
			}
			if tc.name == "unrecognizable key with incomplete projection" {
				param.Rsp = &engine.RangesShuffleParam{Node: &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: &plan.TableDef{TblId: table.TblId, Version: table.Version, Cols: []*plan.ColDef{table.Cols[0]}}}, CNCNT: 1}
			}
			if tc.name == "unrecognizable key with projected missing column" || tc.name == "unrecognizable key with foreign projection" || tc.name == "unrecognizable key with foreign database" || tc.name == "unrecognizable key with stale projection" || tc.name == "unrecognizable key with wrong type projection" {
				param.Rsp = &engine.RangesShuffleParam{Node: &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer"}, TableDef: &plan.TableDef{TblId: table.TblId, Version: table.Version, Cols: []*plan.ColDef{table.Cols[3]}}}, CNCNT: 1}
				if tc.name == "unrecognizable key with foreign projection" {
					param.Rsp.Node.TableDef.TblId++
				}
				if tc.name == "unrecognizable key with foreign database" {
					param.Rsp.Node.ObjRef.SchemaName = "other"
				}
				if tc.name == "unrecognizable key with stale projection" {
					param.Rsp.Node.TableDef.Version++
				}
				if tc.name == "unrecognizable key with wrong type projection" {
					projected := *table.Cols[3]
					projected.Typ.Width++
					param.Rsp.Node.TableDef.Cols[0] = &projected
				}
			}
			err := tbl.rangesOnePart(ctx, state, table, param, &blocks, proc, nil)
			if tc.missing {
				require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
				require.Zero(t, blocks.Len())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantBlocks, blocks.Len())
			if tc.name == "legal append" {
				t.Run("remote_source_reconstructs_proof", func(t *testing.T) {
					data := readutil.NewBlockListRelationData(0)
					data.SetBlockList(blocks)
					snapshot := timestamp.Timestamp{PhysicalTime: 2}
					source := readutil.NewRemoteDataSource(ctx, fs, snapshot, data)
					reader, err := readutil.NewReader(ctx, proc.Mp(), nil, fs, table, snapshot, nil, source, 0, engine.FilterHint{})
					require.NoError(t, err)
					t.Cleanup(func() { _ = reader.Close() })
					out := batch.NewWithSize(2)
					out.Attrs = []string{"a", "c"}
					out.Vecs[0] = vector.NewVec(types.T_int32.ToType())
					out.Vecs[1] = vector.NewVec(types.T_int32.ToType())
					t.Cleanup(func() { out.Clean(proc.Mp()) })
					end, err := reader.Read(ctx, out.Attrs, nil, proc.Mp(), out)
					require.NoError(t, err)
					require.False(t, end)
					require.Equal(t, []int32{1}, vector.MustFixedColWithTypeCheck[int32](out.Vecs[0]))
					require.Equal(t, 1, out.Vecs[1].Length())
					require.True(t, out.Vecs[1].IsNull(0))
				})
			}
			if tc.name == "ordinary sparse unknown" {
				for _, test := range []struct {
					name    string
					runtime bool
					keep    bool
				}{
					{"static_reject", false, false},
					{"static_keep", false, true},
					{"runtime_reject", true, false},
					{"runtime_keep", true, true},
				} {
					t.Run(test.name, func(t *testing.T) {
						residual := filter
						if test.keep {
							residual = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ISNOTNULL, 0), ObjName: "isnotnull"}, Args: []*plan.Expr{arg}}}}
						}
						var selected objectio.BlockInfoSlice
						err := tbl.rangesOnePart(ctx, state, table, engine.RangesParam{BlockFilters: []*plan.Expr{residual}}, &selected, proc, nil)
						require.NoError(t, err)
						require.Equal(t, 1, selected.Len())
						data := readutil.NewBlockListRelationData(0)
						data.SetBlockList(selected)
						snapshot := timestamp.Timestamp{PhysicalTime: 2}
						source := readutil.NewRemoteDataSource(ctx, fs, snapshot, data)
						reader, err := readutil.NewReader(ctx, proc.Mp(), nil, fs, table, snapshot, nil, source, 0, engine.FilterHint{})
						require.NoError(t, err)
						t.Cleanup(func() { _ = reader.Close() })
						scan := &table_scan.TableScan{Reader: reader, Attrs: []string{"a"}, Types: []plan.Type{table.Cols[0].Typ}}
						if test.runtime {
							scan.RuntimeFilterExprs = []*plan.Expr{residual}
						} else {
							scan.FilterExprs = []*plan.Expr{residual}
						}
						t.Cleanup(func() { scan.Free(proc, false, nil) })
						require.NoError(t, scan.Prepare(proc))
						result, err := vm.Exec(scan, proc)
						require.NoError(t, err)
						if test.keep {
							require.NotNil(t, result.Batch)
							require.Equal(t, []int32{1}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
						} else {
							require.Nil(t, result.Batch, "the retained row must still be filtered")
						}
					})
				}
			}
		})
	}
	t.Run("unknown filter column", func(t *testing.T) {
		table := m0TestAppendTableDef()
		table.Name2ColIndex = map[string]int32{"a": 0}
		filter := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "unknown"}}}
		tbl := &txnTable{db: &txnDatabase{op: op}, tableDef: table}
		var blocks objectio.BlockInfoSlice
		err := tbl.rangesOnePart(ctx, state, table, engine.RangesParam{BlockFilters: []*plan.Expr{filter}}, &blocks, proc, nil)
		require.Error(t, err)
		require.Zero(t, blocks.Len())
	})
	t.Run("canceled", func(t *testing.T) {
		table := m0TestAppendTableDef()
		table.Name2ColIndex = map[string]int32{"a": 0}
		filter := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "a"}}}
		tbl := &txnTable{db: &txnDatabase{op: op}, tableDef: table}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		var blocks objectio.BlockInfoSlice
		err := tbl.rangesOnePart(canceled, state, table, engine.RangesParam{BlockFilters: []*plan.Expr{filter}}, &blocks, proc, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, blocks.Len())
	})
}
