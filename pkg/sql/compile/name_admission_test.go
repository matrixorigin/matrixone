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

package compile

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type nameAdmissionLockService struct {
	lifecycleAdmissionLockService
	capture func(uint64, [][]byte, lock.LockOptions) (lock.Result, error)
}

func (s nameAdmissionLockService) Lock(_ context.Context, table uint64, rows [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
	return s.capture(table, rows, opts)
}

func TestMode2CreateAdmissionPinsCompleteDomainBeforePhysicalNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachMode2NameResolution(defines.AttachAccountId(proc.Ctx, 0), true)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = op
	now := timestamp.Timestamp{PhysicalTime: 20}
	op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC, SnapshotTS: now}).AnyTimes()
	op.EXPECT().SnapshotTS().Return(now).AnyTimes()
	op.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	op.EXPECT().CreateTS().Return(timestamp.Timestamp{}).AnyTimes()
	op.EXPECT().AddWaitLock(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1)).AnyTimes()
	op.EXPECT().RemoveWaitLock(uint64(1)).AnyTimes()
	op.EXPECT().AddLockTable(gomock.Any()).Return(nil).AnyTimes()
	eng := newStubEngine()
	eng.dbs["db"] = newStubDatabase("db")
	eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_DATABASE].tableID = catalog.MO_DATABASE_ID
	eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalog.MO_TABLES_ID
	binding := lock.LockTable{Table: catalog.MO_TABLES_ID, OriginTable: catalog.MO_TABLES_ID, Valid: true, Version: 1, ServiceID: "owner", AllocatorID: "allocator"}
	var events []string
	proc.Base.LockService = nameAdmissionLockService{capture: func(table uint64, rows [][]byte, opts lock.LockOptions) (lock.Result, error) {
		require.True(t, opts.KeepRows)
		require.Equal(t, lock.Granularity_Row, opts.Granularity)
		require.Equal(t, lock.WaitPolicy_FastFail, opts.Policy, "FK publication retains nonwaiting policy")
		require.Len(t, rows, 1)
		outer, err := types.Unpack(rows[0])
		require.NoError(t, err)
		key, err := types.Unpack(outer[0].([]byte))
		require.NoError(t, err)
		var names []string
		for _, value := range key[1:] {
			names = append(names, string(value.([]byte)))
		}
		if names[0] == foldedNamespaceLockTag {
			kind := "fold:"
			if len(names) == 4 {
				kind = "prefix:"
			}
			events = append(events, kind+strings.Join(names[1:], "/"))
		} else if table == catalog.MO_DATABASE_ID {
			events = append(events, "db:"+names[0])
		} else {
			events = append(events, "exact:"+strings.Join(names, "/"))
		}
		grant := binding
		grant.Table, grant.OriginTable = table, table
		return lock.Result{LockedOn: grant, Timestamp: timestamp.Timestamp{PhysicalTime: 1}}, nil
	}}
	q := &plan.CreateTable{Database: "db", TableDef: &plan.TableDef{Name: "Child"},
		FkDbs: []string{"db"}, FkTables: []string{"Parent"},
		UnresolvedFkParents: []*plan.ObjectRef{{DbName: "db", ObjName: "Absent"}},
		IndexTables:         []*plan.TableDef{{Name: catalog.PrefixIndexTableName + "test"}}, FkNameBinding: &binding}
	c := &Compile{proc: proc, e: eng}
	rebuild, err := c.pinCreateNames(q, "db")
	require.NoError(t, err)
	require.False(t, rebuild)
	require.True(t, c.createNamePinsAdmitted)
	require.Equal(t, []string{"db:db", "prefix:1/" + catalog.PrefixIndexTableName + "/all",
		"fold:1/" + catalog.PrefixIndexTableName + "test", "fold:1/absent", "fold:1/child", "fold:1/parent",
		"exact:db/Absent", "exact:db/Child", "exact:db/Parent"}, events)

	// The same owner/frontier is borrowed; no engine, lock or workspace call
	// may refresh or extend it inside nested creation after replacement effects.
	count := len(events)
	c.createNamePinsAdmitted = false
	admission := dropLifecycleAdmission{createNames: createNameDomain(q, "db")}
	require.NoError(t, c.withDropLifecycle(admission, func() error { return c.admitCreateNames(q, "db") }))
	require.True(t, c.createNamePinsAdmitted)
	require.Len(t, events, count)
}
