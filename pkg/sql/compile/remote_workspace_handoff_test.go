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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemoteScopeIdentifiesWorkspaceObjectProducers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope *Scope
		want  bool
	}{
		{"nil", nil, false},
		{"normal insert", &Scope{RootOp: &insert.Insert{}}, false},
		{"S3 insert", &Scope{RootOp: &insert.Insert{ToWriteS3: true}}, true},
		{"remote delete", &Scope{RootOp: &deletion.Deletion{RemoteDelete: true}}, true},
		{"multi update", &Scope{RootOp: &multi_update.MultiUpdate{Action: multi_update.UpdateWriteS3}}, true},
		{"nested", &Scope{PreScopes: []*Scope{{RootOp: &insert.Insert{ToWriteS3: true}}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := remoteScopeMayPersistWorkspaceObjects(tc.scope, "cn:1")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestWorkspaceHandoffPreflightIncludesForwardingReceivers(t *testing.T) {
	d := dispatch.NewArgument()
	defer d.Release()
	d.RemoteRegs = []colexec.ReceiveInfo{{NodeAddr: "cn:3"}, {NodeAddr: "cn:3"}}
	scopes := []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: "cn:2"}, RootOp: d,
		PreScopes: []*Scope{{RootOp: &insert.Insert{ToWriteS3: true}}}}}
	writer, addresses, err := workspaceHandoffParticipants(scopes, "cn:1")
	require.NoError(t, err)
	require.True(t, writer)
	require.Equal(t, []string{"cn:2", "cn:3"}, addresses)
	d.RemoteRegs = append(d.RemoteRegs, colexec.ReceiveInfo{})
	_, _, err = workspaceHandoffParticipants(scopes, "cn:1")
	require.Error(t, err, "an unknown downstream cannot be skipped before persistence")
	scopes[0].PreScopes = nil
	writer, addresses, err = workspaceHandoffParticipants(scopes, "cn:1")
	require.NoError(t, err, "read-only scopes do not require handoff capability")
	require.False(t, writer)
	require.Empty(t, addresses)
}
