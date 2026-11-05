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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
	"testing"
)

// Exercise the production sampled-stat accumulator on serialized old objects,
// not a reimplementation of its missing-column arithmetic.
func TestM0CollectedStatsMissingSuffix(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 42)
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
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	// Two objects cover both initialization and accumulation branches.
	for range 2 {
		writer := ioutil.ConstructWriter(0, []uint16{0, 1, 2}, 2, true, false, fs)
		_, err = writer.WriteBatch(bat)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: writer.GetObjectStats(), CreateTime: types.BuildTS(1, 0)}, false))
	}
	def := m0TestAppendTableDef()
	def.TblId = 42
	def.DbId = 7
	for i, col := range def.Cols {
		col.ColId = uint64(i + 1)
		col.Typ.Id = int32(types.T_int32)
	}
	def.Cols[2].Typ.Id = int32(types.T_varchar)
	def.Pkey.Names = []string{"a", "b"}
	def.Pkey.CompPkeyCol = def.Cols[2]
	cols, err := evolutionColumns(def, 3)
	require.NoError(t, err)
	contract := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 42, DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 1, Columns: cols, SchemaDigest: m0read.EvolutionDigest(cols)}
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: contract}))}}}}}}
	req := newUpdateStatsRequest(def, state, fs, types.BuildTS(2, 0), 2, nil)
	req.samplingMode = "full"
	info := planutil.NewTableStatsInfo(4)
	_, err = collectTableStats(ctx, req, info, nil)
	require.NoError(t, err)
	require.Equal(t, float64(2), info.TableRowCount)
	require.Equal(t, int64(2), info.NullCnts[3])
	require.Zero(t, info.ColumnNDVs[3])
	require.Zero(t, info.ColumnSize[3])
	require.False(t, info.ColumnZMs[3].IsInited())
	require.Positive(t, info.ColumnSize[0])
	require.Zero(t, info.NullCnts[0])
	// Invalid authority must be rejected even when it defeats shape recognition.
	contract.FormatVersion++
	def.Defs[0].GetProperties().Properties[0].Value = string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: contract}))
	_, domain := m0AppendBaseFromPlan(def)
	require.False(t, domain)
	_, err = collectTableStats(ctx, req, planutil.NewTableStatsInfo(4), nil)
	require.Error(t, err)
	// Same serialized bytes cannot acquire authority from shape alone.
	def.Defs = nil
	_, err = collectTableStats(ctx, req, planutil.NewTableStatsInfo(4), nil)
	require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	// Breaking the old shape recognizer cannot turn missing metadata into
	// ordinary NULL permission in the strict build.
	key := def.Pkey
	def.Pkey = nil
	_, err = collectTableStats(ctx, req, planutil.NewTableStatsInfo(4), nil)
	require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	def.Pkey = key
	// Ordinary complete objects need no evolution authority.
	def.Cols = append(def.Cols[:3], def.Cols[4])
	def.Version = 0
	_, err = collectTableStats(ctx, req, planutil.NewTableStatsInfo(3), nil)
	require.NoError(t, err)
}
