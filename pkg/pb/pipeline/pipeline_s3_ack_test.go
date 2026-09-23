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

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// The pre-field-19 ACK shape exercises gogo's unknown-field wire handling.
type legacyS3Ack struct {
	Cmd      uint32 `protobuf:"varint,2,opt,name=cmd,proto3"`
	Sequence uint64 `protobuf:"varint,18,opt,name=batch_ack_sequence,proto3"`
}

func (m *legacyS3Ack) Reset()         { *m = legacyS3Ack{} }
func (m *legacyS3Ack) String() string { return proto.CompactTextString(m) }
func (*legacyS3Ack) ProtoMessage()    {}

func TestS3OwnershipAckWire(t *testing.T) {
	for _, retained := range []bool{false, true} {
		msg := &Message{Cmd: Method_PipelineBatchAck, BatchAckSequence: 7, BatchAckS3OwnershipRetained: retained}
		data, err := msg.Marshal()
		require.NoError(t, err)
		require.Equal(t, msg.ProtoSize(), len(data))
		var decoded Message
		require.NoError(t, decoded.Unmarshal(data))
		require.Equal(t, retained, decoded.GetBatchAckS3OwnershipRetained())
		require.Equal(t, uint64(7), decoded.GetBatchAckSequence())
		var oldDecoder legacyS3Ack
		require.NoError(t, proto.Unmarshal(data, &oldDecoder))
		require.Equal(t, uint32(Method_PipelineBatchAck), oldDecoder.Cmd)
		require.Equal(t, uint64(7), oldDecoder.Sequence)
		decoded.Reset()
		require.False(t, decoded.GetBatchAckS3OwnershipRetained())
	}
	// Legacy ACK: field 2 (command 7), field 18 (sequence 7); no field 19.
	var legacy Message
	require.NoError(t, legacy.Unmarshal([]byte{0x10, 7, 0x90, 1, 7}))
	require.Equal(t, uint64(7), legacy.GetBatchAckSequence())
	require.False(t, legacy.GetBatchAckS3OwnershipRetained())
}
