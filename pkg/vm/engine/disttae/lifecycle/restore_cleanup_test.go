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

package lifecycle

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLExpiredRestorePagerReturnsBoundedTenantWork(t *testing.T) {
	mp := mpool.MustNewZero()
	restoreID := uuid.MustParse("c09efc39-6dd8-4fd7-af30-ac2f65c2828e")
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account where account_id > 0",
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, 17),
			},
			{
				contains:  "state in ('importing','publishing')",
				accountID: 17,
				result: expiredRestoreResult(
					t, mp, hex.EncodeToString(restoreID[:]), "db",
				),
			},
		},
	}
	refs, next, err := (SQLExpiredRestorePager{Executor: fake}).Next(
		context.Background(),
		ExpiredRestoreCursor{},
		time.Now(),
		8,
		64,
	)
	require.NoError(t, err)
	require.Equal(t, ExpiredRestoreCursor{AccountID: 17}, next)
	require.Equal(t, []ExpiredRestoreAttempt{{
		AccountID:          17,
		RestoreID:          restoreID.String(),
		TargetDatabaseName: "db",
	}}, refs)
}

func TestSQLExpiredRestorePagerContinuesPastFailedAttempt(t *testing.T) {
	mp := mpool.MustNewZero()
	first := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	second := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "a.restore_id>unhex('00000000000000000000000000000001')",
			accountID: 17,
			result: expiredRestoreResult(
				t, mp, hex.EncodeToString(second[:]), "db",
			),
		}},
	}
	refs, next, err := (SQLExpiredRestorePager{Executor: fake}).Next(
		context.Background(),
		ExpiredRestoreCursor{AccountID: 17, RestoreID: first.String()},
		time.Now(),
		8,
		1,
	)
	require.NoError(t, err)
	require.Equal(t, []ExpiredRestoreAttempt{{
		AccountID:          17,
		RestoreID:          second.String(),
		TargetDatabaseName: "db",
	}}, refs)
	require.Equal(t, ExpiredRestoreCursor{
		AccountID: 17,
		RestoreID: second.String(),
	}, next)
}

func expiredRestoreResult(
	t *testing.T,
	mp *mpool.MPool,
	restoreID string,
	databaseName string,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(2)
	for index, item := range []string{restoreID, databaseName} {
		value.Vecs[index] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(
			value.Vecs[index],
			[]byte(item),
			false,
			mp,
		))
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}
