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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// Every exit of a conjunctive search returns each block buffer it took. Count
// buffers taken and returned directly: pool reuse is not a usable signal,
// because the race runtime deliberately drops some sync.Pool puts.
func TestConjunctiveWandBuffersReleasedOnEveryExit(t *testing.T) {
	assertReleased := func(t *testing.T, run func() []Result, wantTaken int) []Result {
		t.Helper()
		taken, returned := 0, 0
		oldGet, oldPut := getWandBuf, putWandBuf
		getWandBuf = func() *wandBuf { taken++; return oldGet() }
		putWandBuf = func(b *wandBuf) { returned++; oldPut(b) }
		defer func() { getWandBuf, putWandBuf = oldGet, oldPut }()

		first := run()
		require.Equal(t, wantTaken, taken, "one buffer per unique present term")
		require.Equal(t, taken, returned, "every buffer taken must be returned")
		second := run()
		require.Equal(t, first, second)
		require.Equal(t, 2*wantTaken, taken)
		require.Equal(t, taken, returned)
		return second
	}

	build := func(t *testing.T, docs ...[]string) *Segment {
		t.Helper()
		b := NewBuilder("release", int32(types.T_int64))
		for ord, terms := range docs {
			feed(t, b, int64(ord+1), terms...)
		}
		s, err := b.Finish()
		require.NoError(t, err)
		return s
	}

	runQuery := func(t *testing.T, s *Segment, pattern string) func() []Result {
		t.Helper()
		q, err := ParseBoolean([]byte(pattern), tokenizer.NewSimpleTokenizer())
		require.NoError(t, err)
		_, ok := conjunctiveTerms(q)
		require.True(t, ok)
		return func() []Result {
			got, err := s.SearchBoolean(q, BM25, 10, nil, nil)
			require.NoError(t, err)
			return got
		}
	}

	t.Run("success", func(t *testing.T) {
		s := build(t, []string{"alpha", "beta"}, []string{"alpha"})
		run := runQuery(t, s, "+alpha +beta")
		require.Len(t, assertReleased(t, run, 2), 1)
	})

	t.Run("missing-after-allocation", func(t *testing.T) {
		s := build(t, []string{"alpha"})
		run := runQuery(t, s, "+alpha +missing")
		require.Empty(t, assertReleased(t, run, 1))
	})

	t.Run("probe-exhausted", func(t *testing.T) {
		// Both lists have df=2, so query order makes alpha the driver. After ord 0
		// matches, alpha advances to ord 2 while beta ends at ord 1; probing ord 2
		// exhausts beta and returns through the early heap-to-results branch.
		s := build(t,
			[]string{"alpha", "beta"},
			[]string{"beta"},
			[]string{"alpha"},
		)
		run := runQuery(t, s, "+alpha +beta")
		require.Len(t, assertReleased(t, run, 2), 1)
	})
}
