package proxy

import (
	"context"
	"errors"
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
