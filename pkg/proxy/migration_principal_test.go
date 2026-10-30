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

package proxy

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"net"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

func TestMigrationPrincipalAttestation(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *query.Response
		err  error
		ok   bool
	}{
		{"valid", &query.Response{MigrateConnToResponse: &query.MigrateConnToResponse{Success: true, PrincipalVerified: true}}, nil, true},
		{"no-envelope", nil, nil, false},
		{"no-body", &query.Response{}, nil, false},
		{"legacy-success", &query.Response{MigrateConnToResponse: &query.MigrateConnToResponse{Success: true}}, nil, false},
		{"negative-success", &query.Response{MigrateConnToResponse: &query.MigrateConnToResponse{PrincipalVerified: true}}, nil, false},
		{"transport-with-owned-response", &query.Response{}, errors.New("transport failure"), false},
		{"cancel", nil, context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal := &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}
			client := &disconnectQueryClient{send: func(_ context.Context, _ string, r *query.Request) (*query.Response, error) {
				require.Equal(t, query.CmdMethod_MigrateConnTo, r.CmdMethod)
				require.Equal(t, principal, r.MigrateConnToRequest.Principal)
				return tc.resp, tc.err
			}}
			c := &clientConn{queryClient: client}
			err := c.sendMigrationWithPrincipal(context.Background(), "target", &query.MigrateConnToRequest{Principal: principal, VerifyPrincipalOnly: true})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			expected := 0
			if tc.resp != nil {
				expected = 1
			}
			require.Equal(t, expected, client.released)
		})
	}
}

func TestMigrationMissingPrincipalBeforeReplay(t *testing.T) {
	c := &clientConn{}
	// No backend/query client exists: absence must be rejected before either is used.
	require.Error(t, c.migrateConnToContext(context.Background(), nil, nil))
	require.Error(t, c.migrateConnToContext(context.Background(), nil, &query.MigrateConnFromResponse{LastInsertIDExported: true}))
}

func TestMigrationPrincipalBeforeReplay(t *testing.T) {
	cluster := clusterservice.NewMOCluster("", nil, 0, clusterservice.WithDisableRefresh(), clusterservice.WithServices([]metadata.CNService{{ServiceID: "principal", SQLAddress: "pipe", QueryAddress: "query"}}, nil))
	defer cluster.Close()
	for _, mode := range []string{"mismatch", "missing-attestation", "final-mismatch", "success"} {
		t.Run(mode, func(t *testing.T) {
			local, remote := net.Pipe()
			defer remote.Close()
			sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
			defer sc.Close()
			calls := 0
			client := &disconnectQueryClient{send: func(_ context.Context, _ string, r *query.Request) (*query.Response, error) {
				calls++
				require.Equal(t, &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}, r.MigrateConnToRequest.Principal)
				if calls == 1 {
					require.True(t, r.MigrateConnToRequest.VerifyPrincipalOnly)
					require.Empty(t, sc.statements)
					require.False(t, r.MigrateConnToRequest.LastInsertIDExported)
				} else {
					require.False(t, r.MigrateConnToRequest.VerifyPrincipalOnly)
					require.Len(t, sc.statements, 2)
				}
				if mode == "mismatch" || mode == "final-mismatch" && calls == 2 {
					return &query.Response{}, errors.New("principal mismatch")
				}
				return &query.Response{MigrateConnToResponse: &query.MigrateConnToResponse{Success: true, PrincipalVerified: mode != "missing-attestation"}}, nil
			}}
			c := &clientConn{queryClient: client, moCluster: cluster, log: runtime.DefaultRuntime().Logger()}
			c.migration.setVarStmts = []string{"set @replayed=1"}
			err := c.migrateConnToContext(context.Background(), sc, &query.MigrateConnFromResponse{Principal: &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}, LastInsertIDExported: true})
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if mode == "mismatch" || mode == "missing-attestation" {
				require.Equal(t, 1, calls)
				require.Empty(t, sc.statements)
			} else {
				require.Equal(t, 2, calls)
				require.Equal(t, []string{"/* cloud_nonuser */ set transferred=1;", "set @replayed=1"}, sc.statements)
			}
			require.Equal(t, calls, client.released)
		})
	}
}

func TestMigrationPrincipalSourceOwnership(t *testing.T) {
	cluster := clusterservice.NewMOCluster("", nil, 0, clusterservice.WithDisableRefresh(), clusterservice.WithServices([]metadata.CNService{{ServiceID: "principal", SQLAddress: "pipe", QueryAddress: "query"}}, nil))
	defer cluster.Close()
	for _, mode := range []string{"valid", "missing", "nil-response", "transport-response"} {
		t.Run(mode, func(t *testing.T) {
			principal := &query.MigrateSessionPrincipal{AccountID: 7, UserID: 8}
			body := &query.MigrateConnFromResponse{Principal: principal, PreparedStmtCursorsChecked: true, LastInsertIDExported: true, TempTableStateExported: true}
			if mode == "missing" {
				body.Principal = nil
			}
			response := &query.Response{MigrateConnFromResponse: body}
			client := &disconnectQueryClient{send: func(context.Context, string, *query.Request) (*query.Response, error) {
				if mode == "nil-response" {
					return nil, nil
				}
				if mode == "transport-response" {
					return response, context.Canceled
				}
				return response, nil
			}}
			c := &clientConn{queryClient: client, moCluster: cluster, log: runtime.DefaultRuntime().Logger()}
			got, err := c.migrateConnFromContext(context.Background(), "pipe")
			if mode == "valid" {
				require.NoError(t, err)
				principal.UserID = 9
				require.Equal(t, uint32(8), got.Principal.UserID)
				response.Reset()
				require.Equal(t, uint32(8), got.Principal.UserID)
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
			count := 1
			if mode == "nil-response" {
				count = 0
			}
			require.Equal(t, count, client.released)
		})
	}
}
