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
	"github.com/stretchr/testify/require"
)

// TestOffHeapPostings pins the off-heap loaded-postings contract: a deserialized
// segment keeps docIDs/tfs/positions off the Go heap (positions field nil, served
// via posAt from the flat posFlat/posOff), the decoded data round-trips exactly,
// and Free() is safe (releases the buffers, idempotent, and a no-op on a build-side
// segment).
func TestOffHeapPostings(t *testing.T) {
	// Build a multi-term, multi-doc, multi-position segment.
	b := NewBuilder("oh", int32(types.T_int64))
	feed(t, b, int64(0), "alpha", "beta", "alpha", "gamma")
	feed(t, b, int64(1), "beta", "beta", "delta")
	feed(t, b, int64(2), "alpha", "gamma", "gamma")
	built, err := b.Finish()
	require.NoError(t, err)

	// Build-side segment: positions are the Go-heap [][]int32, no off-heap buffers.
	require.Nil(t, built.deallocators, "build-side segment holds no off-heap buffers")
	built.Free() // no-op, must not panic

	// Serialize -> deserialize into an off-heap loaded segment.
	blob, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("oh", bytes.NewReader(blob))
	require.NoError(t, err)
	require.NotEmpty(t, loaded.deallocators, "loaded segment holds off-heap buffers")

	// Every loaded term: docIDs/tfs off-heap; positions kept COMPRESSED (posRaw, the
	// [][]int32 field nil) and materialized on demand to reproduce the build-side
	// positions exactly.
	for _, term := range built.sortedTerms {
		want, ok := built.Lookup(term)
		require.True(t, ok, term)
		got, ok := loaded.LookupLoaded(term)
		require.True(t, ok, term)
		require.Nil(t, got.positions, "%s: loaded positions must be lazy (nil [][]int32)", term)
		require.NotNil(t, got.posRaw, "%s: loaded positions kept compressed in posRaw", term)
		require.Equal(t, want.docIDs, got.docIDs, term)
		require.Equal(t, want.tfs, got.tfs, term)
		gotPos := got.materializePositions()
		require.Equal(t, len(want.positions), len(gotPos), term)
		for i := range want.positions {
			require.Equalf(t, want.positions[i], gotPos[i], "%s positions[%d]", term, i)
		}
	}

	// Free releases the off-heap buffers and is idempotent (double free must not crash
	// or double-deallocate).
	loaded.Free()
	require.Nil(t, loaded.deallocators)
	loaded.Free()
	freeSegs([]*Segment{loaded, nil, built}) // nil-safe + already-freed safe
}
