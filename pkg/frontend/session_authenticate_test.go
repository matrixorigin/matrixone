// Copyright 2021 Matrix Origin
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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

func TestAdvanceAuthenticationSnapshot(t *testing.T) {
	newSessionWithClock := func(t *testing.T, physicalTime int64, maxOffset time.Duration) *Session {
		t.Helper()
		service := "auth-snapshot-" + t.Name()
		rt := moruntime.NewRuntime(
			metadata.ServiceType_CN,
			service,
			nil,
			moruntime.WithClock(clock.NewHLCClock(
				func() int64 { return physicalTime },
				maxOffset,
			)),
		)
		moruntime.SetupServiceBasedRuntime(service, rt)
		return &Session{feSessionImpl: feSessionImpl{service: service}}
	}

	t.Run("uses uncertainty upper bound", func(t *testing.T) {
		ses := newSessionWithClock(t, 100, 20*time.Nanosecond)
		require.NoError(t, ses.advanceAuthenticationSnapshot(t.Context()))
		require.Equal(t,
			timestamp.Timestamp{PhysicalTime: 121},
			ses.getLastCommitTS(),
		)
	})

	t.Run("does not lower existing minimum", func(t *testing.T) {
		ses := newSessionWithClock(t, 100, 20*time.Nanosecond)
		ses.lastCommitTS = timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 7}
		require.NoError(t, ses.advanceAuthenticationSnapshot(t.Context()))
		require.Equal(t,
			timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 7},
			ses.getLastCommitTS(),
		)
	})

	t.Run("missing runtime fails closed", func(t *testing.T) {
		ses := &Session{feSessionImpl: feSessionImpl{service: "missing-auth-snapshot-runtime"}}
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"missing service runtime",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("missing clock fails closed", func(t *testing.T) {
		service := "missing-auth-snapshot-clock"
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
		moruntime.SetupServiceBasedRuntime(service, rt)
		ses := &Session{feSessionImpl: feSessionImpl{service: service}}
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"missing transaction clock",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("negative clock offset fails closed", func(t *testing.T) {
		ses := newSessionWithClock(t, 100, -time.Nanosecond)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"negative transaction clock offset",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("timestamp overflow fails closed", func(t *testing.T) {
		ses := newSessionWithClock(t, math.MaxInt64, 0)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"timestamp overflow",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("clock upper bound overflow fails closed", func(t *testing.T) {
		ses := newSessionWithClock(t, math.MaxInt64, time.Nanosecond)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"timestamp overflow",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})
}

func TestAuthenticateUserAdvancesSnapshotBeforeBackgroundTransaction(t *testing.T) {
	const physicalTime = int64(100)
	const maxOffset = 20 * time.Nanosecond
	service := "authenticate-snapshot-integration"
	rt := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		service,
		nil,
		moruntime.WithClock(clock.NewHLCClock(
			func() int64 { return physicalTime },
			maxOffset,
		)),
	)
	moruntime.SetupServiceBasedRuntime(service, rt)

	ses := &Session{
		feSessionImpl: feSessionImpl{service: service},
		timestampMap:  make(map[TS]time.Time),
	}
	bh := &backgroundExecTest{}
	bh.init()
	wantErr := moerr.NewInternalErrorNoCtx("stop after transaction begin")
	bh.sql2err["begin;"] = wantErr

	previous := NewBackgroundExec
	t.Cleanup(func() { NewBackgroundExec = previous })
	var (
		gotMinimum     timestamp.Timestamp
		gotRealUser    bool
		gotCancellable bool
	)
	NewBackgroundExec = func(_ context.Context, upstream FeSession, opts ...*BackgroundExecOption) BackgroundExec {
		gotMinimum = upstream.getLastCommitTS()
		if len(opts) == 1 && opts[0] != nil {
			gotRealUser = opts[0].fromRealUser
			gotCancellable = opts[0].cancelTxnCreateWithRequest
		}
		return bh
	}

	_, err := ses.AuthenticateUser(
		t.Context(),
		"tenant:user",
		"",
		nil,
		nil,
		func([]byte, []byte, []byte) bool { return false },
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 121}, gotMinimum)
	require.True(t, gotRealUser)
	require.True(t, gotCancellable)
}

func TestResolveImplicitDefaultRole(t *testing.T) {
	const (
		userID   = int64(42)
		readerID = int64(7)
	)
	ctx := context.Background()
	readerSQL := getSqlForRoleNameOfUserRole(userID, readerID)
	publicSQL := getSqlForRoleNameOfUserRole(userID, publicRoleID)

	t.Run("granted default role", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{"reader"}})

		roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.NoError(t, err)
		require.Equal(t, readerID, roleID)
		require.Equal(t, "reader", roleName)
		require.Equal(t, []string{readerSQL}, bh.executedSQLs)
	})

	for _, name := range []string{
		"revoked default role",
		"missing default role metadata",
		"NULL default role name",
		"empty default role name",
	} {
		t.Run(name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			switch name {
			case "NULL default role name":
				bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{nil}})
			case "empty default role name":
				bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{""}})
			default:
				bh.sql2result[readerSQL] = newMrsForRoleName(nil)
			}
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{publicRoleName}})

			roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
			require.NoError(t, err)
			require.Equal(t, int64(publicRoleID), roleID)
			require.Equal(t, publicRoleName, roleName)
			require.Equal(t, []string{readerSQL, publicSQL}, bh.executedSQLs)
		})
	}

	for _, tc := range []struct {
		name  string
		id    int64
		valid bool
	}{
		{name: "NULL catalog default", valid: false},
		{name: "negative catalog default", id: -1, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{publicRoleName}})

			roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, tc.id, tc.valid)
			require.NoError(t, err)
			require.Equal(t, int64(publicRoleID), roleID)
			require.Equal(t, publicRoleName, roleName)
			require.Equal(t, []string{publicSQL}, bh.executedSQLs)
		})
	}

	t.Run("regrant restores stored default", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{"reader"}})

		roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.NoError(t, err)
		require.Equal(t, readerID, roleID)
		require.Equal(t, "reader", roleName)
	})

	t.Run("missing public grant rejects login", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName(nil)
		bh.sql2result[publicSQL] = newMrsForRoleName(nil)

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorContains(t, err, "get a valid default role")
	})

	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{name: "NULL public role name", value: nil},
		{name: "empty public role name", value: ""},
		{name: "mismatched public role name", value: "reader"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{tc.value}})

			_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, 0, false)
			require.ErrorContains(t, err, "get a valid default role")
		})
	}

	t.Run("catalog query error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		wantErr := moerr.NewInternalErrorNoCtx("role lookup failed")
		bh.sql2err[readerSQL] = wantErr

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("public fallback query error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName(nil)
		wantErr := moerr.NewInternalErrorNoCtx("public role lookup failed")
		bh.sql2err[publicSQL] = wantErr

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, []string{readerSQL, publicSQL}, bh.executedSQLs)
	})

	t.Run("invalid role name type is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{struct{}{}}})

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.Error(t, err)
	})

	t.Run("role name NULL check error is returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		result := mock_frontend.NewMockExecResult(ctrl)
		result.EXPECT().GetRowCount().Return(uint64(1))
		wantErr := moerr.NewInternalErrorNoCtx("role name NULL check failed")
		result.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(0)).Return(false, wantErr)

		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = result

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestReadStoredDefaultRoleID(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		value     interface{}
		wantID    int64
		wantValid bool
	}{
		{name: "valid", value: int64(7), wantID: 7, wantValid: true},
		{name: "NULL", value: nil},
		{name: "negative", value: int64(-1)},
		{name: "out of range", value: uint64(^uint32(0)) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := newMrsForPasswordOfUser([][]interface{}{{int64(42), "password", tc.value}})
			roleID, valid, err := readStoredDefaultRoleID(ctx, result)
			require.NoError(t, err)
			require.Equal(t, tc.wantID, roleID)
			require.Equal(t, tc.wantValid, valid)
		})
	}

	t.Run("missing row returns error", func(t *testing.T) {
		result := newMrsForPasswordOfUser(nil)
		_, _, err := readStoredDefaultRoleID(ctx, result)
		require.Error(t, err)
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		result := newMrsForPasswordOfUser([][]interface{}{{int64(42), "password", struct{}{}}})
		_, _, err := readStoredDefaultRoleID(ctx, result)
		require.Error(t, err)
	})
}
