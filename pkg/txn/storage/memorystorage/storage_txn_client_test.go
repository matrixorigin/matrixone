// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memorystorage

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestStorageTxnOperatorSnapshotTS(t *testing.T) {
	orig := timestamp.Timestamp{PhysicalTime: 1, LogicalTime: 2}
	op := &StorageTxnOperator{
		meta: txn.TxnMeta{
			SnapshotTS: orig,
		},
	}

	require.Equal(t, orig, op.SnapshotTS())

	updated := timestamp.Timestamp{PhysicalTime: 3, LogicalTime: 4}
	op.SetSnapshotTS(updated)
	require.Equal(t, updated, op.SnapshotTS())
	require.Equal(t, updated, op.Txn().SnapshotTS)
}

func TestStorageTxnOperatorRunSQLLifecycle(t *testing.T) {
	op := &StorageTxnOperator{}
	running1, cancel1 := context.WithCancel(context.Background())
	running2, cancel2 := context.WithCancel(context.Background())
	token1, err := op.TryEnterRunSqlWithTokenAndSQL(cancel1, "select 1")
	require.NoError(t, err)
	token2, err := op.TryEnterRunSqlWithTokenAndSQL(cancel2, "select 2")
	require.NoError(t, err)
	require.NotZero(t, token1)
	require.NotZero(t, token2)
	require.NotEqual(t, token1, token2)

	commitC := make(chan error, 1)
	go func() {
		commitC <- op.Commit(context.Background())
	}()
	select {
	case <-running1.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal call did not cancel the first running SQL")
	}
	select {
	case <-running2.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal call did not cancel the second running SQL")
	}

	rejected, rejectedCancel := context.WithCancel(context.Background())
	token, err := op.TryEnterRunSqlWithTokenAndSQL(rejectedCancel, "select after commit")
	require.Zero(t, token)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	select {
	case <-rejected.Done():
	default:
		t.Fatal("sealed storage operator did not cancel rejected SQL")
	}

	op.ExitRunSqlWithToken(token1)
	select {
	case err := <-commitC:
		t.Fatalf("commit returned with a running SQL token: %v", err)
	default:
	}
	op.ExitRunSqlWithToken(token1) // duplicate exit is idempotent
	op.ExitRunSqlWithToken(token2)
	select {
	case err := <-commitC:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("commit did not resume after all SQL tokens exited")
	}
}
