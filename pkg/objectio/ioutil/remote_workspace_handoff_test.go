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

package ioutil

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteWorkspaceHandoffScopeCodec(t *testing.T) {
	s := RemoteWorkspaceHandoffScope{
		Version: RemoteWorkspaceHandoffVersion1, TxnID: string([]byte{0, 1, 255}),
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 17, ProducerCN: "cn:1234",
	}
	encoded, err := MarshalRemoteWorkspaceHandoffScope(s)
	require.NoError(t, err)
	decoded, present, err := ParseRemoteWorkspaceHandoffScope(encoded)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, s, decoded)
	for n := 0; n < len(encoded); n++ {
		_, present, err = ParseRemoteWorkspaceHandoffScope(encoded[:n])
		if n < len(remoteWorkspaceHandoffMagic) {
			require.False(t, present)
			require.NoError(t, err)
		} else {
			require.True(t, present)
			require.Error(t, err)
		}
	}
	_, present, err = ParseRemoteWorkspaceHandoffScope(append(encoded, 1))
	require.True(t, present)
	require.Error(t, err)
	_, present, err = ParseRemoteWorkspaceHandoffScope([]byte("unrelated metadata"))
	require.False(t, present)
	require.NoError(t, err)

	invalid := s
	invalid.ProducerCN = string(bytes.Repeat([]byte{'a'}, 257))
	_, err = MarshalRemoteWorkspaceHandoffScope(invalid)
	require.Error(t, err)
	invalid = s
	invalid.Version++
	_, err = MarshalRemoteWorkspaceHandoffScope(invalid)
	require.Error(t, err)
}

func TestRemoteWorkspaceHandoffDecisionCodec(t *testing.T) {
	s := RemoteWorkspaceHandoffScope{
		Version: RemoteWorkspaceHandoffVersion1, TxnID: "txn",
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 3, ProducerCN: "cn:1",
	}
	for _, decision := range []RemoteWorkspaceHandoffDecision{RemoteWorkspaceHandoffAdopted, RemoteWorkspaceHandoffRejected} {
		encoded, err := MarshalRemoteWorkspaceHandoffDecision(s, "object", decision)
		require.NoError(t, err)
		got, name, actual, err := ParseRemoteWorkspaceHandoffDecision(encoded)
		require.NoError(t, err)
		require.Equal(t, s, got)
		require.Equal(t, "object", name)
		require.Equal(t, decision, actual)
		for n := 0; n < len(encoded); n++ {
			_, _, _, err := ParseRemoteWorkspaceHandoffDecision(encoded[:n])
			require.Error(t, err)
		}
		_, _, _, err = ParseRemoteWorkspaceHandoffDecision(append(encoded, 0))
		require.Error(t, err)
	}
	_, err := MarshalRemoteWorkspaceHandoffDecision(s, "object", RemoteWorkspaceHandoffUnknown)
	require.Error(t, err)
}

func TestRemoteWorkspaceHandoffDecisionBatchCodec(t *testing.T) {
	s := RemoteWorkspaceHandoffScope{
		Version: RemoteWorkspaceHandoffVersion1, TxnID: "txn",
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 3, ProducerCN: "cn:1",
	}
	names := []string{"first", "second"}
	data, err := MarshalRemoteWorkspaceHandoffDecisionBatch(s, names, RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	got, gotNames, decision, err := ParseRemoteWorkspaceHandoffDecisionBatch(data)
	require.NoError(t, err)
	require.Equal(t, s, got)
	require.Equal(t, names, gotNames)
	require.Equal(t, RemoteWorkspaceHandoffAdopted, decision)
	for n := 0; n < len(data); n++ {
		_, _, _, err = ParseRemoteWorkspaceHandoffDecisionBatch(data[:n])
		require.Error(t, err)
	}
	_, _, _, err = ParseRemoteWorkspaceHandoffDecisionBatch(append(data, 1))
	require.Error(t, err)
	_, err = MarshalRemoteWorkspaceHandoffDecisionBatch(s, make([]string, RemoteWorkspaceHandoffDecisionBatchLimit+1), RemoteWorkspaceHandoffAdopted)
	require.Error(t, err)
}
