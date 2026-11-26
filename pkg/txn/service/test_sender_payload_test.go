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

package service

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

// The in-process sender gives the handler its own copy of every CN payload,
// as the wire does, so the CN may reuse its payload buffer after the RPC.
func TestTestSenderGivesHandlerOwnPayloads(t *testing.T) {
	cnPayload := []byte("cn-payload")
	write := txn.TxnRequest{Method: txn.TxnMethod_Write, CNRequest: &txn.CNOpRequest{Payload: cnPayload, Target: NewTestTNShard(1)}}
	commit := txn.TxnRequest{Method: txn.TxnMethod_Commit, Txn: NewTestTxn(1, 1, 1), CommitRequest: &txn.TxnCommitRequest{
		Payload: []*txn.TxnRequest{&write},
	}}

	var handled []*txn.TxnRequest
	s := NewTestSender()
	handler := func(_ context.Context, req *txn.TxnRequest, _ *txn.TxnResponse) error {
		handled = append(handled, req)
		return nil
	}
	shard := write.GetTargetTN()
	s.router[s.getRouteKey(txn.TxnMethod_Write, shard)] = handler
	s.router[s.getRouteKey(txn.TxnMethod_Commit, shard)] = handler
	_, err := s.Send(context.Background(), []txn.TxnRequest{write, commit})
	require.NoError(t, err)
	require.Len(t, handled, 2)

	// The CN reuses its buffer: the handler's payloads are unaffected.
	copy(cnPayload, "XXXXXXXXXX")
	require.Equal(t, []byte("cn-payload"), handled[0].CNRequest.Payload)
	require.Equal(t, []byte("cn-payload"), handled[1].CommitRequest.Payload[0].CNRequest.Payload)
	require.Same(t, &write, commit.CommitRequest.Payload[0], "the caller's requests are not modified")
	require.Equal(t, []byte("XXXXXXXXXX"), write.CNRequest.Payload)
}
