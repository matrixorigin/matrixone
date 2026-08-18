// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func makePrecommitPayload(t *testing.T, entryCount int) []byte {
	entries := make([]*api.Entry, entryCount)
	for i := range entries {
		entries[i] = &api.Entry{PkCheckByTn: int32(i % 2)}
	}
	payload, err := (&api.PrecommitWriteCmd{
		EntryList:           entries,
		SyncProtectionJobId: "job-1",
	}).MarshalBinary()
	require.NoError(t, err)
	return payload
}

func makeCommitRequest(payload []byte) *txnpb.TxnCommitRequest {
	return &txnpb.TxnCommitRequest{
		Payload: []*txnpb.TxnRequest{{
			CNRequest: &txnpb.CNOpRequest{Payload: payload},
		}},
	}
}

func newCommitRequestsIter(req *txnpb.TxnCommitRequest) *txnCommitRequestsIter {
	return (&Handle{}).newTxnCommitRequestsIter(req, txnpb.TxnMeta{})
}

func TestTxnCommitRequestsIterPreloadsAndConsumesPayload(t *testing.T) {
	req := makeCommitRequest(makePrecommitPayload(t, 2))
	iter := newCommitRequestsIter(req)

	cmd, err := iter.current()
	require.NoError(t, err)
	require.Equal(t, "job-1", cmd.SyncProtectionJobId)
	require.True(t, iter.loaded)

	// A second decode would restore the serialized value and erase this marker.
	cmd.SyncProtectionJobId = "reuse-marker"
	entry, err := iter.Entry()
	require.NoError(t, err)
	require.IsType(t, &api.Entry{}, entry)
	require.Equal(t, "reuse-marker", iter.curNorReq.SyncProtectionJobId)
	require.True(t, iter.loaded)
	require.Len(t, iter.curNorReq.EntryList, 1)
	require.Equal(t, 0, iter.cursor)

	entry, err = iter.Entry()
	require.NoError(t, err)
	require.IsType(t, &api.Entry{}, entry)
	require.False(t, iter.loaded)
	require.Nil(t, iter.loadErr)
	require.Equal(t, 1, iter.cursor)
	require.False(t, iter.Next())
}

func TestTxnCommitRequestsIterAdvancesEmptyAndMultiplePayloads(t *testing.T) {
	first, err := (&api.PrecommitWriteCmd{}).MarshalBinary()
	require.NoError(t, err)
	second, err := (&api.PrecommitWriteCmd{
		EntryList:           []*api.Entry{{PkCheckByTn: 1}},
		SyncProtectionJobId: "job-2",
	}).MarshalBinary()
	require.NoError(t, err)
	req := &txnpb.TxnCommitRequest{
		Payload: []*txnpb.TxnRequest{
			{CNRequest: &txnpb.CNOpRequest{Payload: first}},
			{CNRequest: &txnpb.CNOpRequest{Payload: second}},
		},
	}
	iter := newCommitRequestsIter(req)

	_, err = iter.current()
	require.NoError(t, err)
	entry, err := iter.Entry()
	require.NoError(t, err)
	require.Nil(t, entry)
	require.Equal(t, 1, iter.cursor)
	require.False(t, iter.loaded)
	require.True(t, iter.Next())

	cmd, err := iter.current()
	require.NoError(t, err)
	require.Equal(t, "job-2", cmd.SyncProtectionJobId)
	entry, err = iter.Entry()
	require.NoError(t, err)
	require.IsType(t, &api.Entry{}, entry)
	require.Equal(t, 2, iter.cursor)
	require.False(t, iter.loaded)
	require.False(t, iter.Next())
}

func TestTxnCommitRequestsIterRetainsDecodeError(t *testing.T) {
	req := makeCommitRequest([]byte{0x0a})
	iter := newCommitRequestsIter(req)

	_, err := iter.current()
	require.Error(t, err)
	require.True(t, iter.loaded)
	require.Equal(t, 0, iter.cursor)

	_, entryErr := iter.Entry()
	require.ErrorIs(t, entryErr, err)
	require.Equal(t, 0, iter.cursor)
	require.True(t, iter.loaded)
}

func BenchmarkTxnCommitRequestsIterDecode(b *testing.B) {
	for _, entryCount := range []int{1, 2, 8, 16, 64, 256} {
		b.Run(fmt.Sprintf("entries-%d", entryCount), func(b *testing.B) {
			req := makeCommitRequest(makeBenchmarkPayload(entryCount))

			b.Run("single-decode", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					iter := newCommitRequestsIter(req)
					_, _ = iter.current()
					for iter.Next() {
						_, _ = iter.Entry()
					}
				}
			})

			b.Run("legacy-dual-decode", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					runLegacyDualDecode(req)
				}
			})
		})
	}
}

func runLegacyDualDecode(req *txnpb.TxnCommitRequest) {
	var metadata api.PrecommitWriteCmd
	_ = metadata.UnmarshalBinary(req.Payload[0].CNRequest.Payload)

	iter := newCommitRequestsIter(req)
	for iter.Next() {
		cnReq := iter.commitRequests.Payload[iter.cursor].CNRequest
		if iter.curNorReq == nil {
			iter.curNorReq = new(api.PrecommitWriteCmd)
		}
		if len(iter.curNorReq.EntryList) == 0 {
			_ = iter.curNorReq.UnmarshalBinary(cnReq.Payload)
		}
		_, iter.curNorReq.EntryList, _ = pkgcatalog.ParseEntryList(iter.curNorReq.EntryList)
		if len(iter.curNorReq.EntryList) == 0 {
			iter.cursor++
		}
	}
}

func makeBenchmarkPayload(entryCount int) []byte {
	vecs := make([]api.Vector, 4)
	for i := range vecs {
		vecs[i] = api.Vector{Data: make([]byte, 256), Area: make([]byte, 512), Len: 64}
	}
	entries := make([]*api.Entry, entryCount)
	for i := range entries {
		entries[i] = &api.Entry{
			PkCheckByTn: int32(i % 2),
			Bat: &api.Batch{
				Attrs: []string{"a", "b", "c", "d"},
				Vecs:  vecs,
			},
		}
	}
	payload, err := (&api.PrecommitWriteCmd{EntryList: entries}).MarshalBinary()
	if err != nil {
		panic(err)
	}
	return payload
}
