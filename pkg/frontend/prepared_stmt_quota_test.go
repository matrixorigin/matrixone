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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func newPreparedStmtLimitForTest(limit int64) *SystemVariables {
	return &SystemVariables{
		mp: map[string]interface{}{
			maxPreparedStmtCount: limit,
		},
	}
}

func newPreparedStmtLimitSessionForTest(limit *SystemVariables) *Session {
	return &Session{
		feSessionImpl: feSessionImpl{
			gSysVars: limit,
		},
		prepareStmts: make(map[string]*PrepareStmt),
	}
}

func TestPreparedStmtLimitPerSession(t *testing.T) {
	ctx := context.Background()
	globalVars := newPreparedStmtLimitForTest(2)
	ses1 := newPreparedStmtLimitSessionForTest(globalVars)
	ses2 := newPreparedStmtLimitSessionForTest(globalVars)

	require.NoError(t, ses1.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	require.NoError(t, ses1.SetPrepareStmt(ctx, "s2", &PrepareStmt{Name: "s2"}))

	err := ses1.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"})
	require.Error(t, err)
	require.Equal(t, moerr.ER_MAX_PREPARED_STMT_COUNT_REACHED, err.(*moerr.Error).MySQLCode())
	require.Equal(t, "42000", err.(*moerr.Error).SqlState())
	require.Equal(t,
		"Can't create more than max_prepared_stmt_count statements (current value: 2)",
		err.Error())
	require.NotContains(t, ses1.prepareStmts, "s3")

	require.NoError(t, ses1.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	require.Len(t, ses1.prepareStmts, 2)

	require.NoError(t, ses2.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	require.NoError(t, ses2.SetPrepareStmt(ctx, "s2", &PrepareStmt{Name: "s2"}))
	require.Len(t, ses2.prepareStmts, 2)

	ses1.RemovePrepareStmt("s1")
	require.NoError(t, ses1.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"}))
	require.Len(t, ses1.prepareStmts, 2)
}

func TestPreparedStmtLimitDynamicChange(t *testing.T) {
	ctx := context.Background()
	globalVars := newPreparedStmtLimitForTest(2)
	ses := newPreparedStmtLimitSessionForTest(globalVars)

	require.NoError(t, ses.SetPrepareStmt(ctx, "s1", &PrepareStmt{Name: "s1"}))
	globalVars.Set(maxPreparedStmtCount, int64(1))

	err := ses.SetPrepareStmt(ctx, "s2", &PrepareStmt{Name: "s2"})
	require.Error(t, err)
	require.Equal(t,
		"Can't create more than max_prepared_stmt_count statements (current value: 1)",
		err.Error())

	ses.RemovePrepareStmt("s1")
	require.NoError(t, ses.SetPrepareStmt(ctx, "s2", &PrepareStmt{Name: "s2"}))

	globalVars.Set(maxPreparedStmtCount, int64(0))
	err = ses.SetPrepareStmt(ctx, "s3", &PrepareStmt{Name: "s3"})
	require.Error(t, err)
	require.Equal(t,
		"Can't create more than max_prepared_stmt_count statements (current value: 0)",
		err.Error())
}

func TestGetMaxPrepareStmtCount(t *testing.T) {
	const configuredLimit = uint32(100)

	originalLimit := MaxPrepareNumberInOneSession.Swap(configuredLimit)
	t.Cleanup(func() {
		MaxPrepareNumberInOneSession.Store(originalLimit)
	})

	ses := newPreparedStmtLimitSessionForTest(newPreparedStmtLimitForTest(16382))
	require.Equal(t, uint64(configuredLimit), ses.getMaxPrepareStmtCountLocked())

	ses.gSysVars.Set(maxPreparedStmtCount, int64(50))
	require.Equal(t, uint64(50), ses.getMaxPrepareStmtCountLocked())

	ses.gSysVars = nil
	require.Equal(t, uint64(configuredLimit), ses.getMaxPrepareStmtCountLocked())
}
