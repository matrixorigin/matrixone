//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	taecatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
	"testing"
)

func birthWriterTables() (*plan.TableDef, *plan.TableDef) {
	key := &plan.ColDef{Name: "__mo_cpkey_col", ColId: 3, Seqnum: 2, Hidden: true, Primary: true, Typ: types.NewProtoType(types.T_varchar)}
	flag := &plan.ColDef{Name: "flag", ColId: 4, Seqnum: 3, Typ: types.NewProtoType(types.T_bool), Default: &plan.Default{NullAbility: true, OriginString: "false"}}
	flag.Default.Expr = &plan.Expr{Typ: flag.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}
	table := &plan.TableDef{TblId: 42, Name: "birth_writer", Version: 1, Pkey: &plan.PrimaryKeyDef{PkeyColName: key.Name, CompPkeyCol: key},
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0, Typ: types.NewProtoType(types.T_int32)},
			{Name: "b", ColId: 2, Seqnum: 1, Typ: types.NewProtoType(types.T_int32)}, key, flag,
			{Name: "__mo_rowid", ColId: 5, Seqnum: 4, Hidden: true, Typ: types.NewProtoType(types.T_Rowid)},
		}, AppendMissingState: &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 3, PkSeqnum: 2,
			Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1, Type: flag.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}}
	projection := *table
	projection.Cols = []*plan.ColDef{table.Cols[0], table.Cols[1], flag, key}
	return table, &projection
}

func TestM0BirthCNWriterCompleteProjectionAndHeader(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	table, projection := birthWriterTables()
	require.NoError(t, plan.ValidateAppendMissingWriteTable(projection))
	require.Error(t, plan.ValidateAppendMissingTable(projection), "write projection cannot be used as a catalog/read proof")
	writer := NewCNS3DataWriter(mp, fs, projection, 1, false)
	defer writer.Close()
	bad := batch.NewWithSize(3)
	owned, err := writer.WriteOwned(ctx, bad)
	require.Error(t, err)
	require.False(t, owned)
	input := batch.NewWithSize(4)
	defer input.Clean(mp)
	for i, col := range projection.Cols {
		input.Vecs[i] = vector.NewVec(vector.ProtoTypeToType(col.Typ))
	}
	for row := range 2 {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], false, row == 1, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[3], []byte{byte('a' + row)}, false, mp))
	}
	input.SetRowCount(2)
	require.NoError(t, writer.Write(ctx, input))
	stats, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	reader, err := objectio.NewObjectReaderWithStr(stats[0].ObjectName().String(), fs)
	require.NoError(t, err)
	hdr, err := reader.ReadHeader(ctx, mp)
	require.NoError(t, err)
	version, err := objectio.M0ObjectSchemaVersion(hdr)
	require.NoError(t, err)
	require.Equal(t, uint32(1), version)
	meta, err := reader.ReadAllMeta(ctx, mp)
	require.NoError(t, err)
	data := meta.MustDataMeta()
	layout, err := taecatalog.M0AppendLayoutFromPlan(table, 4)
	require.NoError(t, err)
	width, err := objectio.ValidateM0Object(layout, hdr, data)
	require.NoError(t, err)
	require.Equal(t, uint32(4), width)
	output, err := objectio.ReadOneBlock(ctx, &data, stats[0].ObjectName().String(), 0, []uint16{3}, []types.Type{types.T_bool.ToType()}, mp, fs, fileservice.SkipAllCache)
	require.NoError(t, err)
	defer objectio.ReleaseIOVector(&output)
	decoded, err := objectio.DecodeCached(output.Entries[0].CachedData)
	require.NoError(t, err)
	values := decoded.(*vector.Vector)
	require.False(t, values.IsNull(0))
	require.True(t, values.IsNull(1), "a stored NULL must survive physical materialization")
	invalid := *projection
	invalid.Version = 0
	refused := NewCNS3DataWriter(mp, fs, &invalid, 1, false)
	defer refused.Close()
	_, err = refused.Sync(ctx)
	require.Error(t, err, "partial/current schema mismatch cannot publish through Sync")
}
