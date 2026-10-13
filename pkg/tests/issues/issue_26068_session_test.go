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

package issues

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/stretchr/testify/require"
)

func TestIssue26068SessionTxnID(t *testing.T) {
	const (
		nodeID       = "cn-1"
		connectionID = uint32(7)
		validTxnID   = "0123456789abcdef0123456789abcdef"
		otherTxnID   = "fedcba9876543210fedcba9876543210"
	)

	tests := []struct {
		name     string
		sessions []*status.Session
		wantErr  bool
	}{
		{
			name: "exact match",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID, TxnID: validTxnID},
			},
		},
		{
			name: "ignores other node and connection",
			sessions: []*status.Session{
				{NodeID: "cn-2", ConnID: connectionID, TxnID: otherTxnID},
				{NodeID: nodeID, ConnID: connectionID + 1, TxnID: otherTxnID},
				{NodeID: nodeID, ConnID: connectionID, TxnID: validTxnID},
			},
		},
		{
			name: "missing",
			sessions: []*status.Session{
				{NodeID: "cn-2", ConnID: connectionID, TxnID: otherTxnID},
			},
			wantErr: true,
		},
		{
			name: "duplicate",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID, TxnID: validTxnID},
				{NodeID: nodeID, ConnID: connectionID, TxnID: validTxnID},
			},
			wantErr: true,
		},
		{
			name: "malformed transaction ID",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID, TxnID: "not-hex"},
			},
			wantErr: true,
		},
		{
			name: "empty transaction ID",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID},
			},
			wantErr: true,
		},
		{
			name: "zero transaction ID",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID, TxnID: strings.Repeat("0", 32)},
			},
			wantErr: true,
		},
		{
			name: "wrong transaction ID length",
			sessions: []*status.Session{
				{NodeID: nodeID, ConnID: connectionID, TxnID: "01"},
			},
			wantErr: true,
		},
	}

	wantTxnID, err := hex.DecodeString(validTxnID)
	require.NoError(t, err)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := issue26068SessionTxnID(tc.sessions, nodeID, connectionID)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, wantTxnID, got)
		})
	}
}
