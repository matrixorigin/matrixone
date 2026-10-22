// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"bytes"
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func appendMissingSchema(t *testing.T) *Schema {
	t.Helper()
	s := NewEmptySchema("birth_fill")
	s.Version = 1
	require.NoError(t, s.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, s.AppendCol("b", types.T_int32.ToType()))
	require.NoError(t, s.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	s.ColDefs[2].Hidden = true
	require.NoError(t, s.AppendCol("flag", types.T_bool.ToType()))
	s.ColDefs[3].Default, _ = types.Encode(&plan.Default{NullAbility: true, OriginString: "false",
		Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}})
	require.NoError(t, s.Finalize(false))
	s.Extra.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"),
		BaseWidth: 3, PkSeqnum: 2, Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1,
			Type: plan.Type{Id: int32(types.T_bool)}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
	require.NoError(t, s.validateAppendMissingColumns())
	return s
}

func TestAppendMissingSchemaWALAndClone(t *testing.T) {
	source := appendMissingSchema(t)
	wire, err := source.Marshal()
	require.NoError(t, err)
	restored := NewEmptySchema("restored")
	_, err = restored.ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	require.Equal(t, source.Extra.AppendMissingState, restored.Extra.AppendMissingState)
	require.Equal(t, source.ColDefs[3].Default, restored.ColDefs[3].Default)
	cloned := source.Clone()
	cloned.Extra.AppendMissingState.LayoutId[0] = 'x'
	require.Equal(t, byte('0'), source.Extra.AppendMissingState.LayoutId[0])
	// A change to the current SQL default must not edit the old-row descriptor.
	restored.ColDefs[3].Default, _ = types.Encode(&plan.Default{NullAbility: true, OriginString: "true"})
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, restored.Extra.AppendMissingState.Columns[0].Kind)
	restored.ColDefs[3].Type = types.T_int32.ToType()
	require.Error(t, restored.validateAppendMissingColumns())
	bad := appendMissingSchema(t)
	bad.Extra.AppendMissingState.FormatVersion = 2
	wire, err = bad.Marshal()
	require.NoError(t, err)
	_, err = NewEmptySchema("bad").ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
	require.Error(t, err)
}

func TestAppendMissingMVCCCopyOnWriteAndRollback(t *testing.T) {
	catalog := MockCatalog(nil)
	defer catalog.Close()
	mgr := txnbase.NewTxnManager(MockTxnStoreFactory(catalog), MockTxnFactory(catalog), types.NewMockHLCClock(1))
	mgr.Start(context.Background())
	defer mgr.Stop()
	create, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := create.CreateDatabase("birth_db", "", "")
	require.NoError(t, err)
	schema := appendMissingSchema(t)
	relation, err := db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, create.Commit(context.Background()))
	entry := relation.(*mockTableHandle).entry
	drop, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err = drop.GetDatabase("birth_db")
	require.NoError(t, err)
	_, err = db.DropRelationByName(schema.Name)
	require.NoError(t, err)
	_, changed, err := entry.AlterTable(context.Background(), drop, apipb.NewUpdateCommentReq(entry.GetDB().GetID(), entry.GetID(), "new comment"))
	require.NoError(t, err)
	require.Equal(t, schema.Extra.AppendMissingState, changed.Extra.AppendMissingState)
	require.NotSame(t, schema.Extra.AppendMissingState, changed.Extra.AppendMissingState)
	changed.Extra.AppendMissingState.LayoutId[0] = 'x'
	require.Equal(t, byte('0'), schema.Extra.AppendMissingState.LayoutId[0])
	require.NoError(t, drop.Rollback(context.Background()))
	reader, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	visible := entry.GetVisibleSchema(reader, false)
	require.NotNil(t, visible)
	require.Equal(t, schema.Extra.AppendMissingState, visible.Extra.AppendMissingState)
	require.NoError(t, reader.Rollback(context.Background()))
}
