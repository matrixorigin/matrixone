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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

// The guard restates a refusal the planner makes from the threshold alone, so it
// must decide the outcome before any path that can return -- including the ones
// that answer without consulting the threshold at all. A NULL search term is the
// case that exposes ordering: fulltext2 bails out early on it, and fulltext_index_scan
// rejects it, so a guard evaluated after either one never runs.
func TestZeroRelevanceGuardPrecedesNullPatternPaths(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	nullPattern := func() *vector.Vector {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, m))
		return vec
	}
	guardVec := func(unsafe bool) *vector.Vector {
		vec := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(vec, unsafe, false, m))
		return vec
	}
	int64Vec := func(v int64) *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, v, false, m))
		return vec
	}
	isRefusal := func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "cannot be replaced by FULLTEXT INDEX")
	}

	newFt2State := func() *fulltext2SearchState {
		st := &fulltext2SearchState{inited: true}
		st.batch = batch.NewWithSize(1)
		st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		return st
	}
	// fulltext2_search(cfg, pattern, mode, includePreds, scoreRange, guard)
	ft2Args := func(unsafe bool) []*vector.Vector {
		return []*vector.Vector{nil, nullPattern(), int64Vec(0), nil, nil, guardVec(unsafe)}
	}

	t.Run("fulltext2 refuses an unsafe threshold even when the pattern is NULL", func(t *testing.T) {
		st := newFt2State()
		tf := &TableFunction{}
		tf.ctr.argVecs = ft2Args(true)
		// Without the guard ordering this returns nil and the query yields an empty
		// result, while the identical literal threshold is refused at plan time.
		require.True(t, isRefusal(st.start(tf, proc, 0, nil)))
	})

	t.Run("fulltext2 keeps the NULL-pattern bail when the threshold is safe", func(t *testing.T) {
		st := newFt2State()
		tf := &TableFunction{}
		tf.ctr.argVecs = ft2Args(false)
		require.NoError(t, st.start(tf, proc, 0, nil))
	})

	// Operators are reused across queries. A safe threshold answering first must not
	// let a later unsafe one through on the same reused state.
	t.Run("fulltext2 still refuses after a safe threshold reused the operator", func(t *testing.T) {
		st := newFt2State()
		tf := &TableFunction{}

		tf.ctr.argVecs = ft2Args(false)
		require.NoError(t, st.start(tf, proc, 0, nil))

		tf.ctr.argVecs = ft2Args(true)
		require.True(t, isRefusal(st.start(tf, proc, 0, nil)))
	})

	// fulltext_index_scan rejects a NULL pattern with its own error. The refusal the
	// guard carries is the plan-time one, so it has to win.
	t.Run("fulltext_index_scan refuses an unsafe threshold before the pattern error", func(t *testing.T) {
		st := &fulltextState{inited: true}
		tf := &TableFunction{}
		// fulltext_index_scan(src, index, pattern, mode, guard)
		tf.ctr.argVecs = []*vector.Vector{
			vector.NewVec(types.T_varchar.ToType()),
			vector.NewVec(types.T_varchar.ToType()),
			nullPattern(),
			int64Vec(0),
			guardVec(true),
		}
		require.NoError(t, vector.AppendBytes(tf.ctr.argVecs[0], []byte("src"), false, m))
		require.NoError(t, vector.AppendBytes(tf.ctr.argVecs[1], []byte("idx"), false, m))
		require.True(t, isRefusal(st.start(tf, proc, 0, nil)))
	})
}
