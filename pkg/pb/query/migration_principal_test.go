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
