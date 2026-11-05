// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// RuntimeKeyPayload adapts an existing valid encoded-vector fixture to the
// chunked contract. The test owns the initial reference through Cleanup;
// receivers retaining additional references must release those themselves.
func RuntimeKeyPayload(t testing.TB, data []byte) *keycodec.KeyPayload {
	t.Helper()
	if len(data) == 0 {
		return nil
	}
	var v vector.Vector
	require.NoError(t, v.UnmarshalBinary(data))
	defer v.Free(nil)
	var chunks [][]byte
	if v.Length() <= keycodec.KeyChunkRows && v.Length() != 0 {
		chunks = [][]byte{data}
	} else if v.Length() != 0 {
		mp := mpool.MustNewZero()
		for start := 0; start < v.Length(); start += keycodec.KeyChunkRows {
			func() {
				chunk := vector.NewVec(*v.GetType())
				defer chunk.Free(mp)
				require.NoError(t, chunk.UnionBatch(&v, int64(start), min(keycodec.KeyChunkRows, v.Length()-start), nil, mp))
				encoded, err := chunk.MarshalBinary()
				require.NoError(t, err)
				chunks = append(chunks, encoded)
			}()
		}
		require.Zero(t, mp.CurrNB())
	}
	p, err := keycodec.NewKeyPayload(context.Background(), chunks, 0, nil)
	require.NoError(t, err)
	t.Cleanup(p.Release)
	return p
}

// AssertInvalidRuntimeKeys keeps malformed-wire regression oracles at the
// validation boundary, before an invalid payload can reach a search reader.
func AssertInvalidRuntimeKeys(t testing.TB, data []byte) {
	t.Helper()
	p, err := keycodec.NewKeyPayload(context.Background(), [][]byte{data}, 0, nil)
	if p != nil {
		t.Cleanup(p.Release)
	}
	require.Error(t, err)
}
