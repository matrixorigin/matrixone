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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestAuthorizationEpochOwnershipAndEnd(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{TenantID: 5, UserID: 7, DefaultRoleID: 9})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := &ExecCtx{}
	ctx, e := beginAuthorizationEpoch(parent, ses, exec)
	require.Same(t, e, authorizationEpochFor(ctx, ses))
	other := newTestSession(t, gomock.NewController(t))
	defer other.Close()
	require.Nil(t, authorizationEpochFor(ctx, other))
	childCtx, child := beginAuthorizationEpoch(ctx, ses, &ExecCtx{})
	child.end()
	require.Error(t, childCtx.Err())
	require.NoError(t, ctx.Err(), "child completion cannot end its parent")
	changed := defines.AttachAccountId(ctx, 6)
	_, err := e.snapshot(changed, nil)
	require.ErrorContains(t, err, "context account changed")
	ses.GetTenantInfo().SetDefaultRoleID(10)
	_, err = e.snapshot(ctx, nil)
	require.ErrorContains(t, err, "principal changed")
	e.end()
	require.Error(t, ctx.Err())
	require.True(t, e.prepared.ended)
	require.NoError(t, parent.Err(), "completion cancels only the statement")
	_, err = statementSnapshotOption(ctx, ses, nil)
	require.Error(t, err)
}

func TestAuthorizationBatchParsingKeepsEagerErrorsAndRawContinuations(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	defer ses.Close()
	ctx := context.Background()
	_, _, _, err := prepareAuthorizationStagedExecution(ctx, ses, ses.GetMySQLParser(), "select 1; select '")
	require.Error(t, err, "bad ordinary tail must be rejected before executing statement one")
	first, rest, staged, err := prepareAuthorizationStagedExecution(ctx, ses, ses.GetMySQLParser(), "select ';'; /*! select 2; select 3 */")
	require.NoError(t, err)
	require.True(t, staged)
	require.Contains(t, first, "';'")
	next, rest, err := nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), &UserInput{}, rest)
	require.NoError(t, err)
	require.Contains(t, next.sql, "select 2")
	next, _, err = nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), &UserInput{}, rest)
	require.NoError(t, err)
	require.Contains(t, next.sql, "select 3")
}
