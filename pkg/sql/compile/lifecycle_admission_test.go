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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type lifecycleAdmissionLockService struct {
	lockservice.LockService
	lockFn func(uint64, lock.LockOptions) (lock.Result, error)
}

func (s lifecycleAdmissionLockService) GetServiceID() string { return "" }
func (s lifecycleAdmissionLockService) GetConfig() lockservice.Config {
	return lockservice.Config{MaxLockRowCount: 1024}
}
func (s lifecycleAdmissionLockService) Lock(_ context.Context, table uint64, _ [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
	return s.lockFn(table, opts)
}

func TestLifecycleAdmissionWaitPolicyFollowsRegistryOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exclusive bool
		retained  bool
		policy    lock.WaitPolicy
	}{
		{"fresh_exclusive", true, false, lock.WaitPolicy_Wait},
		{"retained_exclusive", true, true, lock.WaitPolicy_FastFail},
		{"shared", false, false, lock.WaitPolicy_Wait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, catalog.System_Account)
			proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithModeIsolation(
				ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_RC)
			op := proc.Base.TxnOperator
			// Use the real admission/lockop path, stopping at G before the
			// applied-frontier work. C must still be acquired first.
			mockOp := op.(*mock_frontend.MockTxnOperator)
			mockOp.EXPECT().CreateTS().Return(timestamp.Timestamp{}).AnyTimes()
			mockOp.EXPECT().AddWaitLock(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1)).AnyTimes()
			mockOp.EXPECT().RemoveWaitLock(uint64(1)).Times(2)
			mockOp.EXPECT().AddLockTable(gomock.Any()).Return(nil).Times(1)
			const registryID uint64 = 99
			if tc.exclusive {
				mockOp.EXPECT().HasLockTable(registryID).Return(tc.retained)
			}
			eng := newStubEngine()
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalog.MO_TABLES_ID
			registry := newStubRelation(catalog.MO_FEATURE_REGISTRY)
			registry.tableID = registryID
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_FEATURE_REGISTRY] = registry
			stop := errors.New("stop at lifecycle gate")
			var tables []uint64
			proc.Base.LockService = lifecycleAdmissionLockService{lockFn: func(table uint64, opts lock.LockOptions) (lock.Result, error) {
				tables = append(tables, table)
				require.True(t, opts.KeepRows)
				if table == registryID {
					require.Equal(t, tc.policy, opts.Policy)
					mode := lock.LockMode_Shared
					if tc.exclusive {
						mode = lock.LockMode_Exclusive
					}
					require.Equal(t, mode, opts.Mode)
					return lock.Result{}, stop
				}
				require.Equal(t, uint64(catalog.MO_TABLES_ID), table)
				require.Equal(t, lock.LockMode_Shared, opts.Mode)
				return lock.Result{LockedOn: lock.LockTable{Table: table, Valid: true, Version: 1}}, nil
			}}
			c := &Compile{proc: proc, e: eng}
			require.ErrorIs(t, c.admitLifecycleRC(nil, tc.exclusive), stop)
			require.Equal(t, []uint64{catalog.MO_TABLES_ID, registryID}, tables)
			require.Equal(t, lock.WaitPolicy_Wait, proc.GetWaitPolicy(), "override must remain request-local")
		})
	}
}
