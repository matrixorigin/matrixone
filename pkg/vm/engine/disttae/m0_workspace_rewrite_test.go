//go:build mo_m0

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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
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
	"github.com/stretchr/testify/require"
)

func TestM0WorkspacePhysicalRewriteRequiresPhysicalFidelity(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	write := func(seqnums []uint16, key int, version uint32, blocks int) objectio.ObjectStats {
		t.Helper()
		bat := batch.NewWithSize(len(seqnums))
		defer bat.Clean(mp)
		for i, seq := range seqnums {
			typ := types.T_int32.ToType()
			if seq == 2 {
				typ = types.T_varchar.ToType()
			}
			bat.Vecs[i] = vector.NewVec(typ)
			if seq == 2 {
				require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte("key"), false, mp))
			} else {
				require.NoError(t, vector.AppendFixed(bat.Vecs[i], int32(1), false, mp))
			}
		}
		bat.SetRowCount(1)
		writer := ioutil.ConstructWriter(version, seqnums, key, key >= 0, false, fs)
		for i := 0; i < blocks; i++ {
			_, err := writer.WriteBatch(bat)
			require.NoError(t, err)
		}
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		return writer.GetObjectStats()
	}
	valid := write([]uint16{0, 1, 2}, 2, 0, 1)
	twoBlocks := write([]uint16{0, 1, 2}, 2, 0, 2)
	wrongKey := write([]uint16{0, 1, 2}, 1, 0, 1)
	unknownKey := write([]uint16{0, 1, 2}, -1, 0, 1)
	sparse := write([]uint16{0, 2}, 1, 0, 1)
	newer := write([]uint16{0, 1, 2}, 2, 2, 1)
	full := write([]uint16{0, 1, 2, 3}, 2, 1, 1)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	workspace := &Transaction{isCloneTxn: true}
	op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	def := m0TestAppendTableDef()
	def.Cols[0].Typ.Id = int32(types.T_int32)
	def.Cols[1].Typ.Id = int32(types.T_int32)
	def.Cols[2].Typ.Id = int32(types.T_varchar)
	def.Cols[3].Typ.Id = int32(types.T_int32)
	tbl := &txnTable{db: &txnDatabase{op: op}, tableDef: def}
	result, err := prepareM0WorkspacePhysicalRewrite(ctx, tbl, valid, fs)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []uint16{0, 1, 2}, result.seqnums)
	require.Zero(t, result.tableDef.Version)
	require.Len(t, result.tableDef.Cols, 4)
	require.Len(t, def.Cols, 5, "physical projection must not mutate the relation schema")
	result, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, twoBlocks, fs)
	require.NoError(t, err)
	require.NotNil(t, result, "matching two-block object must remain eligible")
	shortStats := twoBlocks
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&shortStats, 1))
	_, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, shortStats, fs)
	require.Error(t, err, "a shortened object stats block count must not omit a block")
	wrongRows := twoBlocks
	require.NoError(t, objectio.SetObjectStatsRowCnt(&wrongRows, twoBlocks.Rows()-1))
	_, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, wrongRows, fs)
	require.Error(t, err, "object stats row count must match the block metadata")
	wrongExtent := twoBlocks
	require.NoError(t, objectio.SetObjectStatsExtent(&wrongExtent, objectio.NewExtent(0, 0, 1, 1)))
	_, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, wrongExtent, fs)
	require.Error(t, err, "rewrite location must use the verified metadata extent")
	for _, tc := range []struct {
		name string
		obj  objectio.ObjectStats
	}{{"wrong key", wrongKey}, {"unknown key", unknownKey}, {"sparse prefix", sparse}, {"newer version", newer}, {"no missing column", full}} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareM0WorkspacePhysicalRewrite(ctx, tbl, tc.obj, fs)
			require.NoError(t, err)
			require.Nil(t, got)
		})
	}
	wrongType := proto.Clone(def).(*plan.TableDef)
	wrongType.Cols[0].Typ.Id = int32(types.T_float64)
	tbl.tableDef = wrongType
	result, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, valid, fs)
	require.NoError(t, err)
	require.Nil(t, result)
	tbl.tableDef = def
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = prepareM0WorkspacePhysicalRewrite(cancelled, tbl, valid, fs)
	require.ErrorIs(t, err, context.Canceled)
	workspace.isCloneTxn = false
	result, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, valid, fs)
	require.NoError(t, err)
	require.Nil(t, result)
	// Explicit private writer binding, with a declared controlled authority.
	workspace.isCloneTxn = true
	def.DbId, def.TblId = 7, 42
	def.Pkey.Names = []string{"a", "b"}
	def.Pkey.CompPkeyCol = def.Cols[2]
	for i, col := range def.Cols {
		col.ColId = uint64(i + 1)
	}
	cols, err := evolutionColumns(def, 3)
	require.NoError(t, err)
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 42, DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 1, Columns: cols, SchemaDigest: m0read.EvolutionDigest(cols)}
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: c}))}}}}}}
	tbl.accountId = 42
	tbl.tableId = 42
	tbl.db.databaseId = 7
	tbl.extraInfo = &api.SchemaExtra{M0Evolution: c}
	before := def.Defs[0].GetProperties().Properties[0].Value
	result, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, valid, fs)
	require.NoError(t, err)
	require.NotNil(t, result)
	pinned, err := m0read.ValidatePlanEvolution(result.tableDef, 42)
	require.NoError(t, err)
	require.Equal(t, def.Version, result.tableDef.Version)
	require.Zero(t, pinned.LayoutGeneration)
	require.Len(t, pinned.Columns, 3)
	require.Equal(t, before, def.Defs[0].GetProperties().Properties[0].Value)
	require.Equal(t, uint32(1), tbl.extraInfo.M0Evolution.LayoutGeneration)
	c.FormatVersion++
	def.Defs[0].GetProperties().Properties[0].Value = string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: c}))
	_, err = prepareM0WorkspacePhysicalRewrite(ctx, tbl, valid, fs)
	require.Error(t, err, "bad authority cannot disappear through shape recognition")

}

func TestM0WorkspacePhysicalRewriteChecksCompleteVectorType(t *testing.T) {
	physical := types.T_varchar.ToType()
	physical.Width = 4
	target := physical
	target.Width = 8
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(physical)
	defer bat.Clean(nil)
	require.NoError(t, validateWorkspacePhysicalTypes(bat, []types.Type{physical}))
	require.Error(t, validateWorkspacePhysicalTypes(bat, []types.Type{target}), "same OID must not hide a physical type change")
	require.Error(t, validateWorkspacePhysicalTypes(bat, nil))
}
