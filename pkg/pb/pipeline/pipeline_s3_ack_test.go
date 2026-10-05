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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3OwnershipAckWire(t *testing.T) {
	for _, retained := range []bool{false, true} {
		msg := &Message{Cmd: Method_PipelineBatchAck, BatchAckSequence: 7,
			ProtocolVersion: 42, BatchAckS3OwnershipRetained: retained}
		data, err := msg.Marshal()
		require.NoError(t, err)
		require.Equal(t, msg.ProtoSize(), len(data))
		var decoded Message
		require.NoError(t, decoded.Unmarshal(data))
		require.Equal(t, retained, decoded.GetBatchAckS3OwnershipRetained())
		require.Equal(t, uint64(7), decoded.GetBatchAckSequence())
		require.Equal(t, int64(42), decoded.GetProtocolVersion())
		decoded.Reset()
		require.False(t, decoded.GetBatchAckS3OwnershipRetained())
		require.Zero(t, decoded.GetProtocolVersion())
	}
	// Pin the two independent wire fields, not just a self-consistent round trip.
	// An ordinary credit ACK with protocol_version=1 must not become a receipt.
	var credit Message
	require.NoError(t, credit.Unmarshal([]byte{0x10, 7, 0x90, 1, 7, 0x98, 1, 1}))
	require.Equal(t, uint64(7), credit.GetBatchAckSequence())
	require.Equal(t, int64(1), credit.GetProtocolVersion())
	require.False(t, credit.GetBatchAckS3OwnershipRetained())
	var receipt Message
	require.NoError(t, receipt.Unmarshal([]byte{0x10, 7, 0x90, 1, 7, 0xa0, 1, 1}))
	require.True(t, receipt.GetBatchAckS3OwnershipRetained())
	require.Zero(t, receipt.GetProtocolVersion())
}
