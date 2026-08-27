// Copyright 2025 Matrix Origin
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

package table_function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The guard is the engine half of the "can this index answer the query?" test the
// planner runs for a literal threshold. True means a document with relevance 0 -- one
// the index never returns -- would satisfy the predicate, so the rewrite must be
// refused with the same 20105 a literal in that range raises.
func TestCheckFulltextZeroRelevanceGuard(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	boolVec := func(v bool) *vector.Vector {
		vec := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(vec, v, false, m))
		return vec
	}
	nullVec := func() *vector.Vector {
		vec := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(vec, false, true, m))
		return vec
	}

	isRefusal := func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "cannot be replaced by FULLTEXT INDEX")
	}

	t.Run("absent argument is not a violation", func(t *testing.T) {
		// A literal threshold: the planner already checked it, so no argument is
		// attached and nothing is charged at runtime.
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, nil, 4, 0))
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{boolVec(true)}, 4, 0))
	})

	t.Run("nil or empty vector is not a violation", func(t *testing.T) {
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{nil}, 0, 0))
		empty := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{empty}, 0, 0))
	})

	t.Run("false admits the query", func(t *testing.T) {
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{boolVec(false)}, 0, 0))
	})

	t.Run("true refuses with the fulltext-index error", func(t *testing.T) {
		err := checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{boolVec(true)}, 0, 0)
		require.True(t, isRefusal(err), "want the 20105 refusal, got %v", err)
	})

	t.Run("a NULL bound is not a violation", func(t *testing.T) {
		// `MATCH(...) > NULL` is NULL, so the query returns no rows either way; the
		// index is not being asked for something it cannot supply.
		require.NoError(t, checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{nullVec()}, 0, 0))
	})

	t.Run("a non-bool argument is rejected", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, m))
		err := checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{vec}, 0, 0)
		require.Error(t, err)
		require.False(t, isRefusal(err), "a malformed argument is not a query refusal")
	})

	t.Run("a constant vector is read at row 0 whatever the row asked for", func(t *testing.T) {
		cv, err := vector.NewConstFixed(types.T_bool.ToType(), true, 8, m)
		require.NoError(t, err)
		require.True(t, isRefusal(checkFulltextZeroRelevanceGuard(proc, []*vector.Vector{cv}, 0, 5)),
			"a const guard applies to every row, not only row 0")
	})
}
