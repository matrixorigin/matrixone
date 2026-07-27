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

package frontend

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func newPreparedStmtQuotaForTest(limit int64) *SystemVariables {
	return &SystemVariables{
		mp: map[string]interface{}{
			maxPreparedStmtCount: limit,
		},
	}
}

func newPreparedStmtQuotaSessionForTest(quota *SystemVariables) *Session {
	return &Session{
		feSessionImpl: feSessionImpl{
			gSysVars: quota,
		},
		prepareStmts: make(map[string]*PrepareStmt),
	}
}

func TestPreparedStmtQuotaSharedAcrossSessions(t *testing.T) {
	ctx := context.Background()
	quota := newPreparedStmtQuotaForTest(2)
	ses1 := newPreparedStmtQuotaSessionForTest(quota)
	ses2 := newPreparedStmtQuotaSessionForTest(quota)

	require.NoError(t, ses1.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	require.NoError(t, ses2.SetPrepareStmt(ctx, "s2", &PrepareStmt{Name: "s2"}))
	require.Equal(t, uint64(2), quota.getPrepareStmtCount())

	require.NoError(t, ses1.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	require.Equal(t, uint64(2), quota.getPrepareStmtCount())

	err := ses1.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"})
	require.Error(t, err)
	require.Equal(t, moerr.ER_MAX_PREPARED_STMT_COUNT_REACHED, err.(*moerr.Error).MySQLCode())
	require.Equal(t, "42000", err.(*moerr.Error).SqlState())
	require.Equal(t,
		"Can't create more than max_prepared_stmt_count statements (current value: 2)",
		err.Error())
	require.NotContains(t, ses1.prepareStmts, "s3")
	require.Equal(t, uint64(2), quota.getPrepareStmtCount())

	quota.Set(maxPreparedStmtCount, int64(1))
	ses2.RemovePrepareStmt("s2")
	require.Equal(t, uint64(1), quota.getPrepareStmtCount())
	err = ses1.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"})
	require.Error(t, err)

	ses1.RemovePrepareStmt("s1")
	require.NoError(t, ses1.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"}))
	require.Equal(t, uint64(1), quota.getPrepareStmtCount())

	ses1.RemoveAllPrepareStmts()
	require.Equal(t, uint64(0), quota.getPrepareStmtCount())
	ses1.RemoveAllPrepareStmts()
	require.Equal(t, uint64(0), quota.getPrepareStmtCount())
}

func TestPreparedStmtQuotaZeroAndAccountIsolation(t *testing.T) {
	ctx := context.Background()
	disabledQuota := newPreparedStmtQuotaForTest(0)
	enabledQuota := newPreparedStmtQuotaForTest(1)
	disabledSession := newPreparedStmtQuotaSessionForTest(disabledQuota)
	enabledSession := newPreparedStmtQuotaSessionForTest(enabledQuota)

	err := disabledSession.SetPrepareStmt(ctx, "disabled", &PrepareStmt{Name: "disabled"})
	require.Error(t, err)
	require.Equal(t, moerr.ER_MAX_PREPARED_STMT_COUNT_REACHED, err.(*moerr.Error).MySQLCode())
	require.Equal(t, uint64(0), disabledQuota.getPrepareStmtCount())

	require.NoError(t, enabledSession.SetPrepareStmt(ctx, "enabled", &PrepareStmt{Name: "enabled"}))
	require.Equal(t, uint64(1), enabledQuota.getPrepareStmtCount())
	require.Equal(t, uint64(0), disabledQuota.getPrepareStmtCount())
	enabledSession.RemoveAllPrepareStmts()
}

func TestPreparedStmtQuotaConcurrentAdmission(t *testing.T) {
	const (
		limit      = 8
		competitor = 64
	)

	ctx := context.Background()
	quota := newPreparedStmtQuotaForTest(limit)
	sessions := make([]*Session, competitor)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, competitor)
	for i := range sessions {
		sessions[i] = newPreparedStmtQuotaSessionForTest(quota)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("s%d", idx)
			if err := sessions[idx].SetPrepareStmt(ctx, name, &PrepareStmt{Name: name}); err == nil {
				admitted.Add(1)
			} else if moErr, ok := err.(*moerr.Error); !ok ||
				moErr.MySQLCode() != moerr.ER_MAX_PREPARED_STMT_COUNT_REACHED {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	require.Empty(t, errCh)
	require.Equal(t, int64(limit), admitted.Load())
	require.Equal(t, uint64(limit), quota.getPrepareStmtCount())
	for _, ses := range sessions {
		ses.RemoveAllPrepareStmts()
	}
	require.Equal(t, uint64(0), quota.getPrepareStmtCount())
}
