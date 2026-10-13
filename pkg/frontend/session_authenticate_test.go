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
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type authenticationBarrierEngine struct {
	engine.Engine
	acquire func(context.Context) (timestamp.Timestamp, error)
}

type unsupportedAuthenticationBarrierEngine struct {
	engine.Engine
}

func (e *authenticationBarrierEngine) AcquireLogtailReadBarrier(
	ctx context.Context,
) (timestamp.Timestamp, error) {
	return e.acquire(ctx)
}

func newAuthenticationSnapshotTestSession(
	t *testing.T,
	physicalTime int64,
	maxOffset time.Duration,
) *Session {
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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
	InitServerLevelVars(service)
	return &Session{feSessionImpl: feSessionImpl{service: service}}
}

func TestAdvanceAuthenticationSnapshot(t *testing.T) {

	t.Run("uses uncertainty upper bound", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		require.NoError(t, ses.advanceAuthenticationSnapshot(t.Context()))
		require.Equal(t,
			timestamp.Timestamp{PhysicalTime: 121},
			ses.getLastCommitTS(),
		)
	})

	t.Run("does not lower existing minimum", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
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
		ses := newAuthenticationSnapshotTestSession(t, 100, -time.Nanosecond)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"negative transaction clock offset",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("timestamp overflow fails closed", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, math.MaxInt64, 0)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"timestamp overflow",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})

	t.Run("clock upper bound overflow fails closed", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, math.MaxInt64, time.Nanosecond)
		require.ErrorContains(t,
			ses.advanceAuthenticationSnapshot(t.Context()),
			"timestamp overflow",
		)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})
}

func TestLogtailReadBarrierSupportedProtocolBoundary(t *testing.T) {
	ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
	rt := moruntime.ServiceRuntime(ses.GetService())

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
	require.False(t, logtailReadBarrierSupported(ses),
		"v38 advertises temporary-table migration, not the logtail read barrier")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
	require.True(t, logtailReadBarrierSupported(ses))
}

func TestPrepareAuthenticationSnapshotFailsClosed(t *testing.T) {
	t.Run("missing parameter unit", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		require.ErrorContains(t,
			ses.prepareAuthenticationSnapshot(t.Context()),
			"missing transaction client",
		)
	})

	t.Run("missing transaction client", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		setPu(ses.GetService(), &mo_config.ParameterUnit{})
		require.ErrorContains(t,
			ses.prepareAuthenticationSnapshot(t.Context()),
			"missing transaction client",
		)
	})

	t.Run("timestamp wait failure", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("logtail unavailable")
		txnClient.EXPECT().WaitLogTailAppliedAt(
			gomock.Any(),
			timestamp.Timestamp{PhysicalTime: 121},
		).Return(timestamp.Timestamp{}, wantErr)
		setPu(ses.GetService(), &mo_config.ParameterUnit{TxnClient: txnClient})

		require.ErrorIs(t, ses.prepareAuthenticationSnapshot(t.Context()), wantErr)
	})

	t.Run("timestamp waiter cannot claim success below fence", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().WaitLogTailAppliedAt(
			gomock.Any(),
			timestamp.Timestamp{PhysicalTime: 121},
		).Return(timestamp.Timestamp{PhysicalTime: 120}, nil)
		setPu(ses.GetService(), &mo_config.ParameterUnit{TxnClient: txnClient})

		require.ErrorContains(t,
			ses.prepareAuthenticationSnapshot(t.Context()),
			"did not reach the required timestamp",
		)
	})
}

func TestPrepareAuthenticationSnapshotUsesTNOrderedBarrier(t *testing.T) {
	ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
	rt := moruntime.ServiceRuntime(ses.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)

	barrierFrontier := timestamp.Timestamp{PhysicalTime: 80, LogicalTime: 9}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), barrierFrontier).
		Return(barrierFrontier.Next(), nil)
	setPu(ses.GetService(), &mo_config.ParameterUnit{
		TxnClient: txnClient,
		StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
			timestamp.Timestamp, error,
		) {
			return barrierFrontier, nil
		}},
	})

	require.NoError(t, ses.prepareAuthenticationSnapshot(t.Context()))
	require.Equal(t, barrierFrontier, ses.getLastCommitTS())
}

func TestPrepareAuthenticationSnapshotBarrierPreservesLaterSessionMinimum(t *testing.T) {
	ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
	moruntime.ServiceRuntime(ses.GetService()).SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion39)
	barrierFrontier := timestamp.Timestamp{PhysicalTime: 80, LogicalTime: 9}
	wantMinimum := timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 7}
	ses.lastCommitTS = wantMinimum

	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), wantMinimum).
		Return(wantMinimum.Next(), nil)
	setPu(ses.GetService(), &mo_config.ParameterUnit{
		TxnClient: txnClient,
		StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
			timestamp.Timestamp, error,
		) {
			return barrierFrontier, nil
		}},
	})

	require.NoError(t, ses.prepareAuthenticationSnapshot(t.Context()))
	require.Equal(t, wantMinimum, ses.getLastCommitTS())
}

func TestPrepareAuthenticationSnapshotBarrierFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		engine  engine.Engine
		wantErr string
	}{
		{name: "missing engine", wantErr: "missing storage engine"},
		{name: "unsupported engine", engine: &unsupportedAuthenticationBarrierEngine{}, wantErr: "does not support"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
			moruntime.ServiceRuntime(ses.GetService()).SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCVersion39)
			ctrl := gomock.NewController(t)
			setPu(ses.GetService(), &mo_config.ParameterUnit{
				TxnClient:     mock_frontend.NewMockTxnClient(ctrl),
				StorageEngine: test.engine,
			})
			require.ErrorContains(t, ses.prepareAuthenticationSnapshot(t.Context()), test.wantErr)
			require.True(t, ses.getLastCommitTS().IsEmpty())
		})
	}

	t.Run("barrier failure", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		moruntime.ServiceRuntime(ses.GetService()).SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion39)
		ctrl := gomock.NewController(t)
		wantErr := moerr.NewInternalErrorNoCtx("barrier unavailable")
		setPu(ses.GetService(), &mo_config.ParameterUnit{
			TxnClient: mock_frontend.NewMockTxnClient(ctrl),
			StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return timestamp.Timestamp{}, wantErr
			}},
		})
		require.ErrorIs(t, ses.prepareAuthenticationSnapshot(t.Context()), wantErr)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})
}

func TestPrepareAuthenticationSnapshotWaitsForEffectiveSessionMinimum(t *testing.T) {
	ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
	wantMinimum := timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 7}
	ses.lastCommitTS = wantMinimum

	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), wantMinimum).
		Return(wantMinimum.Next(), nil)
	setPu(ses.GetService(), &mo_config.ParameterUnit{TxnClient: txnClient})

	require.NoError(t, ses.prepareAuthenticationSnapshot(t.Context()))
	require.Equal(t, wantMinimum, ses.getLastCommitTS())
}

func TestPrepareAuthenticationSnapshotHonorsRequestCancellation(t *testing.T) {
	ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	entered := make(chan struct{})
	txnClient.EXPECT().WaitLogTailAppliedAt(
		gomock.Any(),
		timestamp.Timestamp{PhysicalTime: 121},
	).DoAndReturn(func(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
		close(entered)
		<-ctx.Done()
		return timestamp.Timestamp{}, ctx.Err()
	})
	setPu(ses.GetService(), &mo_config.ParameterUnit{TxnClient: txnClient})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	errC := make(chan error, 1)
	go func() {
		errC <- ses.prepareAuthenticationSnapshot(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("authentication snapshot wait did not start")
	}
	cancel()
	select {
	case err := <-errC:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("authentication snapshot wait did not stop after request cancellation")
	}
}

func TestAuthenticateUserWaitsForLogtailBarrierBeforeBackgroundTransaction(t *testing.T) {
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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
	InitServerLevelVars(service)
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	wantMinimum := timestamp.Timestamp{PhysicalTime: 80, LogicalTime: 7}
	barrierCompleted := false
	waitCompleted := false
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), wantMinimum).
		DoAndReturn(func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
			require.True(t, barrierCompleted)
			waitCompleted = true
			return wantMinimum.Next(), nil
		})
	setPu(service, &mo_config.ParameterUnit{
		TxnClient: txnClient,
		StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
			timestamp.Timestamp, error,
		) {
			barrierCompleted = true
			return wantMinimum, nil
		}},
	})

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
		require.True(t, waitCompleted, "freshness wait must finish before transaction admission")
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
	require.Equal(t, wantMinimum, gotMinimum)
	require.True(t, gotRealUser)
	require.True(t, gotCancellable)
}

func TestAuthenticateUserMarksCanonicalCatalogRejection(t *testing.T) {
	service := "auth-rejection-" + t.Name()
	rt := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		service,
		nil,
		moruntime.WithClock(clock.NewHLCClock(
			func() int64 { return 100 },
			0,
		)),
	)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
	InitServerLevelVars(service)

	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).
		Return(timestamp.Timestamp{PhysicalTime: 100}, nil)
	setPu(service, &mo_config.ParameterUnit{
		TxnClient: txnClient,
		StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
			timestamp.Timestamp, error,
		) {
			return timestamp.Timestamp{PhysicalTime: 100}, nil
		}},
	})

	ses := &Session{
		feSessionImpl: feSessionImpl{service: service},
		rt:            &Routine{},
		timestampMap:  make(map[TS]time.Time),
	}
	bh := &backgroundExecTest{}
	bh.init()
	tenantSQL, err := getSqlForCheckTenant(t.Context(), "tenant")
	require.NoError(t, err)
	tenantResult := mock_frontend.NewMockExecResult(ctrl)
	tenantResult.EXPECT().GetRowCount().Return(uint64(1))
	tenantResult.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(42), nil)
	tenantResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(2)).Return("open", nil)
	tenantResult.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(3)).Return(uint64(1), nil)
	tenantResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("1.0.0", nil)
	bh.sql2result[tenantSQL] = tenantResult
	userSQL, err := getSqlForPasswordOfUser(t.Context(), "dump")
	require.NoError(t, err)
	bh.sql2result[userSQL] = newMrsForPasswordOfUser(nil)

	previous := NewBackgroundExec
	t.Cleanup(func() { NewBackgroundExec = previous })
	NewBackgroundExec = func(context.Context, FeSession, ...*BackgroundExecOption) BackgroundExec {
		return bh
	}

	_, err = ses.AuthenticateUser(
		t.Context(),
		"tenant:dump",
		"",
		nil,
		nil,
		func([]byte, []byte, []byte) bool { return false },
	)
	require.ErrorContains(t, err, "there is no user dump")
	require.True(t, isAuthenticationRejected(err))
	code, state, message := RewriteError(err, "tenant:dump")
	require.Equal(t, moerr.ER_ACCESS_DENIED_ERROR, code)
	require.Equal(t, "28000", state)
	require.Equal(t, "Access denied for user tenant:dump. "+err.Error(), message)
}

func TestAuthenticationRequestRejectedClassifier(t *testing.T) {
	require.True(t, isAuthenticationRequestRejected(
		moerr.NewBadDBNoCtx("missing_db")))
	require.False(t, isAuthenticationRequestRejected(
		moerr.NewInternalErrorNoCtx("catalog temporarily unavailable")))
}

func TestAuthenticateSpecialUserSnapshotBoundary(t *testing.T) {
	const userName = "issue27743-special-user"
	SetSpecialUser(userName, []byte("Issue27743Pass01"))
	t.Cleanup(func() {
		specialUsers.Lock()
		delete(specialUsers.users, userName)
		specialUsers.Unlock()
	})

	t.Run("external special user waits for barrier", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		moruntime.ServiceRuntime(ses.GetService()).SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion39)
		barrierFrontier := timestamp.Timestamp{PhysicalTime: 80, LogicalTime: 7}
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		barrierCompleted := false
		txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), barrierFrontier).
			DoAndReturn(func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
				require.True(t, barrierCompleted)
				return barrierFrontier, nil
			})
		setPu(ses.GetService(), &mo_config.ParameterUnit{
			TxnClient: txnClient,
			StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				barrierCompleted = true
				return barrierFrontier, nil
			}},
		})

		_, err := ses.AuthenticateUser(
			t.Context(), userName, "", nil, nil,
			func([]byte, []byte, []byte) bool { return false },
		)
		require.NoError(t, err)
		require.True(t, barrierCompleted)
		require.Equal(t, barrierFrontier, ses.getLastCommitTS())
	})

	t.Run("external special user fails closed", func(t *testing.T) {
		ses := newAuthenticationSnapshotTestSession(t, 100, 20*time.Nanosecond)
		moruntime.ServiceRuntime(ses.GetService()).SetGlobalVariables(
			moruntime.MOProtocolVersion, defines.MORPCVersion39)
		wantErr := moerr.NewInternalErrorNoCtx("barrier unavailable")
		setPu(ses.GetService(), &mo_config.ParameterUnit{
			TxnClient: mock_frontend.NewMockTxnClient(gomock.NewController(t)),
			StorageEngine: &authenticationBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return timestamp.Timestamp{}, wantErr
			}},
		})

		_, err := ses.AuthenticateUser(
			t.Context(), userName, "", nil, nil,
			func([]byte, []byte, []byte) bool { return false },
		)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("internal special user keeps bootstrap path", func(t *testing.T) {
		ses := &Session{
			feSessionImpl: feSessionImpl{service: "issue27743-internal-special-user"},
			isInternal:    true,
		}

		_, err := ses.AuthenticateUser(
			t.Context(), userName, "", nil, nil,
			func([]byte, []byte, []byte) bool { return false },
		)
		require.NoError(t, err)
		require.True(t, ses.getLastCommitTS().IsEmpty())
	})
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
		require.False(t, isAuthenticationRejected(err))
	})

	t.Run("public fallback query error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName(nil)
		wantErr := moerr.NewInternalErrorNoCtx("public role lookup failed")
		bh.sql2err[publicSQL] = wantErr

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
		require.False(t, isAuthenticationRejected(err))
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
		require.False(t, isAuthenticationRejected(err))
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
