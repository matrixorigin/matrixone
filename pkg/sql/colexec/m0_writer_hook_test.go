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

package colexec

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestM0WriterSchemaVersionUsesCompleteBatchWidth(t *testing.T) {
	tableDef := &plan.TableDef{
		Version: 1,
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0},
			{Name: "b", Seqnum: 1},
			{Name: catalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true},
			{Name: "tier", Seqnum: 3},
			{Name: "credit", Seqnum: 4},
			{Name: "clone_only", Seqnum: 5},
			{Name: catalog.Row_ID, Seqnum: 6},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names:       []string{"a", "b"},
			CompPkeyCol: &plan.ColDef{
				Name: catalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true,
			},
		},
	}

	tableDef.TblId, tableDef.DbId = 42, 7
	version, err := m0WriterSchemaVersion(tableDef, []uint16{0, 1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.Equal(t, tableDef.Version, version, "ordinary writer must not infer provenance from width")
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 3}
	for i, col := range tableDef.Cols[:6] {
		col.ColId = uint64(i + 1)
		b, err := m0read.PlanEvolutionColumnSemantic(tableDef, col)
		require.NoError(t, err)
		g := uint32(0)
		if i >= 3 {
			g = uint32(i - 2)
		}
		c.Columns = append(c.Columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: g, Semantics: b})
	}
	c.SchemaDigest = m0read.EvolutionDigest(c.Columns)
	extra := &api.SchemaExtra{M0Evolution: c}
	tableDef.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(extra))}}}}}}
	version, err = m0WriterSchemaVersion(tableDef, []uint16{0, 1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.Equal(t, uint32(3), version)
	for _, seqs := range [][]uint16{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 5, 6}} {
		_, err = m0WriterSchemaVersion(tableDef, seqs)
		require.Error(t, err)
	}
	tableDef.ClusterBy = &plan.ClusterByDef{Name: "a"}
	_, err = m0WriterSchemaVersion(tableDef, []uint16{0, 1, 2, 3, 4, 5})
	require.Error(t, err)
}

func TestM0CNWriterRejectsMalformedContractBeforeWrite(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	extra := &api.SchemaExtra{M0Evolution: &api.M0EvolutionContract{FormatVersion: 2}}
	def := &plan.TableDef{TblId: 42, DbId: 7, Pkey: &plan.PrimaryKeyDef{PkeyColName: "a"}, Cols: []*plan.ColDef{{Name: "a", ColId: 1, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
		Defs: []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(extra))}}}}}}}
	w := NewCNS3DataWriter(mp, fs, def, 1, false)
	defer w.Close()
	require.Error(t, w.Write(ctx, nil))
	owned, err := w.WriteOwned(ctx, nil)
	require.False(t, owned)
	require.Error(t, err)
	_, err = w.Sync(ctx)
	require.Error(t, err)
	_, err = w.SyncAndFillBlockInfoBat(ctx)
	require.Error(t, err)
	_, err = w.FillBlockInfoBat()
	require.Error(t, err)
	w.Reset()
	require.Error(t, w.Write(ctx, nil), "reset cannot authorize an invalid schema")
	require.NoError(t, w.Close())
	require.Zero(t, mp.CurrNB())
}
