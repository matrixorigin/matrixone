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

package fulltext2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// TestSinkInsertUpdateDelete: a base build then a CDC batch that updates pk 2,
// deletes pk 3, and inserts pk 4. The assembled Index reflects the converged
// state: old text of an updated row gone, its new text visible, deleted row
// absent, new row present.
func TestSinkInsertUpdateDelete(t *testing.T) {
	tok := tokenizer.NewSimpleTokenizer()

	// Base build (recency 0): full source scan.
	base := NewSink(int32(types.T_int64), tok)
	base.Add(RowEvent{Pk: int64(1), Text: []byte("alpha")})
	base.Add(RowEvent{Pk: int64(2), Text: []byte("beta")})
	base.Add(RowEvent{Pk: int64(3), Text: []byte("gamma")})
	baseSeg, baseDel, err := base.Build("base", 0)
	require.NoError(t, err)
	require.Empty(t, baseDel)

	// CDC batch (recency 1): UPDATE pk2 (delete+insert), DELETE pk3, INSERT pk4.
	tail := NewSink(int32(types.T_int64), tok)
	tail.Add(RowEvent{Delete: true, Pk: int64(2)})
	tail.Add(RowEvent{Pk: int64(2), Text: []byte("omega")})
	tail.Add(RowEvent{Delete: true, Pk: int64(3)})
	tail.Add(RowEvent{Pk: int64(4), Text: []byte("delta")})
	tailSeg, tailDel, err := tail.Build("tail", 1)
	require.NoError(t, err)
	// pk2 was reinserted (UPDATE) → no tombstone; pk3 is a pure delete.
	require.Equal(t, map[any]int64{normalizeKey(int64(3)): 1}, tailDel)

	idx := NewIndex([]*Segment{baseSeg, tailSeg}, MergeDeletes(baseDel, tailDel))

	require.Equal(t, int64(3), idx.NumDocs()) // pk 1, 2(new), 4
	require.Equal(t, []any{int64(1)}, iquery(t, idx, "alpha"))
	require.Empty(t, iquery(t, idx, "beta"))                   // pk2 old text superseded
	require.Equal(t, []any{int64(2)}, iquery(t, idx, "omega")) // pk2 new text
	require.Empty(t, iquery(t, idx, "gamma"))                  // pk3 deleted
	require.Equal(t, []any{int64(4)}, iquery(t, idx, "delta"))
}

// TestSinkThroughSerialize: sink → serialize → deserialize → Index still
// converges (the maintenance path persists segments).
func TestSinkThroughSerialize(t *testing.T) {
	tok := tokenizer.NewSimpleTokenizer()
	sink := NewSink(int32(types.T_int64), tok)
	sink.Add(RowEvent{Pk: int64(10), Text: []byte("quick brown fox")})
	sink.Add(RowEvent{Pk: int64(11), Text: []byte("lazy dog")})
	seg, _, err := sink.Build("base", 0)
	require.NoError(t, err)

	data, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("base", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	idx := NewIndex([]*Segment{loaded}, nil)
	require.Equal(t, []any{int64(10)}, iquery(t, idx, "quick brown fox"))
	require.Equal(t, []any{int64(11)}, iquery(t, idx, "lazy dog"))
}
