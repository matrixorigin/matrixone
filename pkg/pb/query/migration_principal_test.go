package query

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMigrationPrincipalWirePresence(t *testing.T) {
	for _, principal := range []*MigrateSessionPrincipal{nil, {}, {AccountID: 7, UserID: 8}} {
		from := &MigrateConnFromResponse{Principal: principal}
		b, err := from.Marshal()
		require.NoError(t, err)
		var got MigrateConnFromResponse
		require.NoError(t, got.Unmarshal(b))
		require.Equal(t, principal, got.Principal)
		to := &MigrateConnToRequest{Principal: principal, VerifyPrincipalOnly: true}
		b, err = to.Marshal()
		require.NoError(t, err)
		var target MigrateConnToRequest
		require.NoError(t, target.Unmarshal(b))
		require.Equal(t, to, &target)
	}
	ack := &MigrateConnToResponse{Success: true, PrincipalVerified: true}
	b, err := ack.Marshal()
	require.NoError(t, err)
	var got MigrateConnToResponse
	require.NoError(t, got.Unmarshal(b))
	require.Equal(t, ack, &got)
}
