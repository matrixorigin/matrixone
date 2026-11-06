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

package table_function

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// installOrdinaryMaintenanceRelations supplies real relation definitions to
// algorithm unit tests; SQL behavior stays at their existing mock boundary.
func installOrdinaryMaintenanceRelations(t *testing.T, proc *process.Process) *int {
	t.Helper()
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	lookups := new(int)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	proc.Base.TxnOperator = op
	proc.Ctx = context.WithValue(defines.AttachAccountId(proc.Ctx, 42), defines.EngineKey{}, engine.Engine(eng))
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, name string, _ any) (engine.Relation, error) {
		*lookups++
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableName().Return(name).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: name, TableType: catalog.SystemOrdinaryRel}).AnyTimes()
		return rel, nil
	}).AnyTimes()
	return lookups
}

func checkVectorMaintenanceReuse(t *testing.T, tf *TableFunction, proc *process.Process, state tvfState, lookups *int, policy **executor.IndexRelationPolicy) {
	t.Helper()
	require.NotNil(t, *policy, "initial start must admit the configured relations")
	before := *lookups
	state.reset(tf, proc)
	require.Nil(t, *policy, "reset must clear the previous admission")
	require.NoError(t, state.start(tf, proc, 0, nil))
	require.NotNil(t, *policy, "same config must be re-admitted")
	require.Greater(t, *lookups, before, "reset must re-admit the configured relations")
	after := *lookups
	// UnsafeGetStringAt aliases these bytes. Changing the original vector
	// verifies the saved admission config is detached from that backing memory.
	config := tf.ctr.argVecs[0].GetBytesAt(0)
	field := []byte(`"src":"src"`)
	pos := bytes.Index(config, field)
	require.NotEqual(t, -1, pos)
	copy(config[pos:pos+len(field)], []byte(`"src":"bad"`))
	state.reset(tf, proc)
	err := state.start(tf, proc, 0, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.ErrorContains(t, err, tf.FuncName+": config changed during operator reuse")
	require.Nil(t, *policy, "rejected config must not retain admission")
	require.Equal(t, after, *lookups, "changed config must be rejected before relation lookup")
}

func TestFulltext2MaintenanceAdmissionOncePerGeneration(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	lookups := installOrdinaryMaintenanceRelations(t, proc)
	st := &fulltext2CreateState{}
	tf, vecs := ft2CreateArgVecs(t, mp, ft2CreateCfg(t, fulltext2.ParserNgram, false), 1, "hello world")
	defer func() {
		st.free(tf, proc, false, nil)
		for _, v := range vecs {
			v.Free(mp)
		}
	}()
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 2, *lookups)
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 2, *lookups, "source rows must not trigger catalog lookups")
	st.reset(tf, proc)
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 4, *lookups, "a new execution must revalidate relation identities")
}

func TestMaintenanceRejectsInheritedTrustedContext(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: process.WithTrustedIndexRelationAccess(defines.AttachAccountId(context.Background(), 42))}
	proc.Base.SessionInfo.User = "reader"
	proc.Base.SessionInfo.Account = "tenant"
	proc.Base.SessionInfo.Role = "accountadmin"
	installOrdinaryMaintenanceRelations(t, proc)
	_, _, err := admitIndexMaintenance(proc, &TableFunction{}, "fulltext2_compact", "db", "",
		maintenanceOperand{Role: catalog.FullText2Index_TblType_Storage, Name: "__mo_index_secondary_123"},
		maintenanceOperand{Role: catalog.FullText2Index_TblType_Metadata, Name: "ordinary"})
	require.ErrorContains(t, err, "do not have privilege")
	// The admission probe must not clear or change the caller's own context.
	require.NoError(t, process.CheckIndexRelationAccess(proc, "__mo_index_secondary_123", "", 0))
}

func TestMaintenanceAttestationChecksRolesAndLiveIdentity(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		targetID              uint64
		swap, mismatchAccount bool
		wantErr               bool
	}{
		{name: "valid", targetID: 11},
		{name: "replaced target", targetID: 99, wantErr: true},
		{name: "swapped roles", targetID: 11, swap: true, wantErr: true},
		{name: "different account", targetID: 11, mismatchAccount: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.WithValue(defines.AttachAccountId(context.Background(), 0), defines.EngineKey{}, engine.Engine(eng))}
			proc.Base.TxnOperator = op
			proc.Base.SessionInfo.User, proc.Base.SessionInfo.Account, proc.Base.SessionInfo.Role = "alice", "tenant", "reader"
			eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil).AnyTimes()
			db.EXPECT().GetDatabaseId(gomock.Any()).Return("8").AnyTimes()
			defs := map[string]struct {
				id   uint64
				kind string
			}{"base": {10, catalog.SystemOrdinaryRel}, "idx": {tc.targetID, catalog.SystemIndexRel}, "meta": {12, catalog.SystemIndexRel}}
			db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, name string, _ any) (engine.Relation, error) {
				def := defs[name]
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableName().Return(name).AnyTimes()
				rel.EXPECT().GetTableID(gomock.Any()).Return(def.id).AnyTimes()
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: name, TableType: def.kind}).AnyTimes()
				rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{ParentTableID: 10}).AnyTimes()
				return rel, nil
			}).AnyTimes()
			spec := &executor.IndexMaintenanceSpec{Function: "fulltext2_create", AccountID: 0, Database: "db", DatabaseID: 8, BaseName: "base", BaseID: 10,
				Targets: []executor.IndexMaintenanceTarget{{Role: catalog.FullText2Index_TblType_Storage, Name: "idx", ID: 11}, {Role: catalog.FullText2Index_TblType_Metadata, Name: "meta", ID: 12}}}
			if tc.mismatchAccount {
				spec.AccountID = 1
			}
			store, meta := "idx", "meta"
			if tc.swap {
				store, meta = meta, store
			}
			policy, _, err := admitIndexMaintenance(proc, &TableFunction{IndexMaintenance: spec}, "fulltext2_create", "db", "",
				maintenanceOperand{Role: catalog.FullText2Index_TblType_Storage, Name: store}, maintenanceOperand{Role: catalog.FullText2Index_TblType_Metadata, Name: meta})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, policy.Spec)
			require.False(t, policy.CallerMayAccess)
		})
	}
}

func TestMaintenanceResetEndWithoutStartIsNoop(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.Background()}
	tf := &TableFunction{}
	hnsw := &hnswCreateState{inited: true, started: true}
	hnsw.reset(tf, proc)
	require.NoError(t, hnsw.end(tf, proc))
	ft := &fulltext2CreateState{inited: true, segIdx: 1}
	ft.reset(tf, proc)
	require.NoError(t, ft.end(tf, proc))
	ivf := &ivfCreateState{inited: true, data32: [][]float32{{1}}}
	ivf.reset(tf, proc)
	require.NoError(t, ivf.end(tf, proc))
	compact := &fulltext2CompactState{inited: true}
	compact.reset(tf, proc)
	require.NoError(t, compact.end(tf, proc))
}
