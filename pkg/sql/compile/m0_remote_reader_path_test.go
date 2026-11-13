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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type m0SerializedReaderEngine struct {
	*readerPathCaptureEngine
	fs fileservice.FileService
}

func (e *m0SerializedReaderEngine) BuildBlockReaders(
	ctx context.Context, p any, ts timestamp.Timestamp, expr *plan.Expr, def *plan.TableDef,
	data engine.RelData, _ int, _ ...engine.FilterHint,
) ([]engine.Reader, error) {
	proc := p.(*process.Process)
	source := readutil.NewRemoteDataSource(ctx, e.fs, ts, data)
	reader, err := readutil.NewReader(ctx, proc.Mp(), nil, e.fs, def, ts, expr, source, 0, engine.FilterHint{})
	if err != nil {
		return nil, err
	}
	return []engine.Reader{reader}, nil
}

// This joins the scope wire, receiver binding and real object reader. The
// relation's selection is controlled, so it is not a multi-CN membership test.
func TestM0RemoteDecodedScopeReadsSerializedAppendObject(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	c := newMergeTopFallbackTestCompile(t)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)
	t.Cleanup(c.proc.Free)
	t.Cleanup(func() { require.Zero(t, c.proc.Mp().CurrNB()) })
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	bat := batch.NewWithSize(3)
	t.Cleanup(func() { bat.Clean(c.proc.Mp()) })
	for i := range bat.Vecs {
		typ := types.T_int32.ToType()
		if i == 2 {
			typ = types.T_varchar.ToType()
		}
		bat.Vecs[i] = vector.NewVec(typ)
		if i == 2 {
			require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte("key"), false, c.proc.Mp()))
		} else {
			require.NoError(t, vector.AppendFixed(bat.Vecs[i], int32(1), false, c.proc.Mp()))
		}
	}
	bat.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1, 2}, 2, true, false, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	data := readutil.NewBlockListRelationData(0)
	info := stats.ConstructBlockInfo(0)
	data.AppendBlockInfo(&info)
	// The serialized sender data contains a different real object. The remote
	// receiver must select objects from its reopened relation, not read this
	// shipped block as a member of that relation.
	vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[0] = 9
	foreignWriter := ioutil.ConstructWriter(0, []uint16{0, 1, 2}, 2, true, false, fs)
	_, err = foreignWriter.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = foreignWriter.Sync(ctx)
	require.NoError(t, err)
	foreignStats := foreignWriter.GetObjectStats()
	foreignInfo := foreignStats.ConstructBlockInfo(0)
	require.NotEqual(t, info.BlockID, foreignInfo.BlockID)
	shipped := readutil.NewBlockListRelationData(0)
	shipped.AppendBlockInfo(&foreignInfo)

	def := &plan.TableDef{TblId: 42, DbId: 7, Name: "customer", Version: 1,
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{ColId: 2, Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}},
			{ColId: 3, Name: catalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{ColId: 4, Name: "c", Seqnum: 3, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{ColId: 5, Name: catalog.Row_ID, Seqnum: 4},
		},
		Name2ColIndex: map[string]int32{"a": 0, "b": 1, catalog.CPrimaryKeyColName: 2, "c": 3, catalog.Row_ID: 4},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, CompPkeyCol: &plan.ColDef{Name: catalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true}},
	}
	// This is explicitly authorized decoder input. Root issuance and member
	// selection are independently proved by the real two-CN SQL test.
	def.Pkey.Names = []string{"a", "b"}
	def.Pkey.CompPkeyCol = def.Cols[2]
	contract := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), AccountId: 42, DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 1}
	for i, col := range def.Cols[:4] {
		semantic, err := m0read.PlanEvolutionColumnSemantic(def, col)
		require.NoError(t, err)
		generation := uint32(0)
		if i == 3 {
			generation = 1
		}
		contract.Columns = append(contract.Columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: generation, Semantics: semantic})
	}
	contract.SchemaDigest = m0read.EvolutionDigest(contract.Columns)
	def.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: "schema_extra", Value: string(api.MustMarshalTblExtra(&api.SchemaExtra{M0Evolution: contract}))}}}}}}
	_, err = m0read.ValidatePlanEvolution(def, 42)
	require.NoError(t, err)

	ts := timestamp.Timestamp{PhysicalTime: 2}
	node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "customer", Obj: 42}, TableDef: def, Stats: &plan.Stats{}}
	sender := &Scope{Magic: Remote, NodeInfo: engine.Node{Mcpu: 1, CNCNT: 2, Data: shipped}, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(), DataSource: &Source{
		SchemaName: "db", RelationName: "customer", Attributes: []string{"a", "c"}, TableDef: def, node: node, Timestamp: ts,
	}}
	t.Cleanup(sender.release)
	wire, err := encodeRemoteScope(sender, c.proc)
	require.NoError(t, err)
	received, err := decodeScope(wire, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(received.release)
	require.Equal(t, foreignInfo.BlockID, received.NodeInfo.Data.GetBlockInfo(0).BlockID)

	ctrl := gomock.NewController(t)
	inner := mock_frontend.NewMockTxnOperator(ctrl)
	inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
	inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	c.proc.Base.TxnOperator = &m0StampedTxn{TxnOperator: inner}
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(data, nil)
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "customer", gomock.Any()).Return(rel, nil)
	c.e = &m0SerializedReaderEngine{readerPathCaptureEngine: &readerPathCaptureEngine{database: db}, fs: fs}
	readers, _, err := received.buildReaders(c)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	t.Cleanup(func() { _ = readers[0].Close() })
	out := batch.NewWithSize(2)
	out.Attrs = []string{"a", "c"}
	out.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	out.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { out.Clean(c.proc.Mp()) })
	end, err := readers[0].Read(ctx, out.Attrs, nil, c.proc.Mp(), out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, []int32{1}, vector.MustFixedColWithTypeCheck[int32](out.Vecs[0]))
	require.Equal(t, 1, out.Vecs[1].Length())
	require.True(t, out.Vecs[1].IsNull(0))
	ordinary := *def
	ordinary.Defs = nil
	source := readutil.NewRemoteDataSource(ctx, fs, ts, data)
	unproved, err := readutil.NewReader(ctx, c.proc.Mp(), nil, fs, &ordinary, ts, nil, source, 0, engine.FilterHint{})
	require.NoError(t, err)
	defer unproved.Close()
	out.CleanOnlyData()
	_, err = unproved.Read(ctx, out.Attrs, nil, c.proc.Mp(), out)
	require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
}
