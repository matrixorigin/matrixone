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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

func TestMigrationPrincipalGuard(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tenant    *TenantInfo
		principal *query.MigrateSessionPrincipal
		ok        bool
	}{
		{"same", &TenantInfo{TenantID: 7, UserID: 8}, &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}, true},
		{"zero", &TenantInfo{}, &query.MigrateSessionPrincipal{}, true},
		{"missing-proof", &TenantInfo{}, nil, false},
		{"missing-auth", nil, &query.MigrateSessionPrincipal{}, false},
		{"account", &TenantInfo{TenantID: 7, UserID: 8}, &query.MigrateSessionPrincipal{AccountID: 9, UserID: 8}, false},
		{"reused-name", &TenantInfo{TenantID: 7, UserID: 9, User: "a"}, &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{}
			ses.SetTenantInfo(tc.tenant)
			req := &query.MigrateConnToRequest{Principal: tc.principal, VerifyPrincipalOnly: true, LastInsertID: 99, FoundRows: 99, DB: "must_not_import"}
			err := Migrate(context.Background(), ses, req)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
			}
			require.Empty(t, ses.GetDatabaseName())
			require.Zero(t, ses.GetLastInsertID())
			require.Zero(t, ses.GetLastFoundRows())
			if !tc.ok {
				req.VerifyPrincipalOnly = false
				require.Error(t, Migrate(context.Background(), ses, req))
			}
		})
	}
}

func TestMigrationPrincipalPreflightPreservesOnce(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	tenant := ses.GetTenantInfo()
	principal := &query.MigrateSessionPrincipal{AccountID: tenant.GetTenantID(), UserID: tenant.GetUserID()}
	req := &query.MigrateConnToRequest{Principal: principal, VerifyPrincipalOnly: true}
	for i := 0; i < 2; i++ {
		require.NoError(t, rt.migrateConnectionTo(context.Background(), req))
	}
	require.Nil(t, rt.mc.migrateErr)
	ran := false
	rt.mc.migrateOnce.Do(func() { ran = true })
	require.True(t, ran, "preflight cannot consume final import")
	bad := *principal
	bad.UserID++
	req.Principal = &bad
	req.VerifyPrincipalOnly = false
	require.Error(t, rt.migrateConnectionTo(context.Background(), req), "consumed once cannot bypass identity guard")
	req.Principal = principal
	req.VerifyPrincipalOnly = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, rt.migrateConnectionTo(ctx, req), context.Canceled)
	require.NoError(t, rt.migrateConnectionTo(context.Background(), req), "canceled preflight permits retry")
	exported := &query.MigrateConnFromResponse{}
	require.NoError(t, rt.migrateConnectionFrom(exported))
	require.Equal(t, principal, exported.Principal)
	exported.Principal.UserID++
	require.Equal(t, principal.UserID, ses.GetTenantInfo().GetUserID(), "response owns scalar copy")
	ses.SetTenantInfo(nil)
	require.Error(t, rt.migrateConnectionFrom(&query.MigrateConnFromResponse{}))
}

func TestMigrationPrincipalLeaseCancellation(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	tenant := ses.GetTenantInfo()
	req := &query.MigrateConnToRequest{Principal: &query.MigrateSessionPrincipal{AccountID: tenant.GetTenantID(), UserID: tenant.GetUserID()}, VerifyPrincipalOnly: true}
	waited := make(chan struct{})
	rt.mc.operationWaitHook = func() { close(waited) }
	require.True(t, rt.mc.beginOperation())
	released := false
	defer func() {
		if !released {
			rt.mc.endOperation()
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- rt.migrateConnectionTo(ctx, req) }()
	<-waited
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	rt.mc.endOperation()
	released = true
	rt.mc.operationWaitHook = nil
	require.NoError(t, rt.migrateConnectionTo(t.Context(), req))
	ran := false
	rt.mc.migrateOnce.Do(func() { ran = true })
	require.True(t, ran)
}
