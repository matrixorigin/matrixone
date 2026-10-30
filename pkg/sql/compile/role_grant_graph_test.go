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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestLockRoleGrantGraphFailsClosed(t *testing.T) {
	ctx := context.Background()
	eng := newStubEngine()
	require.ErrorContains(t, LockRoleGrantGraph(ctx, eng, nil), "engine process")
	proc := testutil.NewProcess(t)
	require.ErrorContains(t, LockRoleGrantGraph(ctx, eng, proc), "pessimistic read-committed")

	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC,
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	dbErr := errors.New("catalog unavailable")
	eng.dbErr = dbErr
	require.ErrorIs(t, LockRoleGrantGraph(ctx, eng, proc), dbErr)
	eng.dbErr = nil
	require.ErrorContains(t, LockRoleGrantGraph(ctx, eng, proc), catalog.MO_ROLE_GRANT)

	eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_ROLE_GRANT] = newStubRelation(catalog.MO_ROLE_GRANT)
	lockErr := errors.New("catalog lock unavailable")
	stub := gostub.Stub(&lockTableForSnapshotRefresh,
		func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
			return lockErr
		})
	defer stub.Reset()
	require.ErrorIs(t, LockRoleGrantGraph(ctx, eng, proc), lockErr)
}
