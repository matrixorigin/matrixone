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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// vectorSearchStateLimit returns the resolved candidate limit (k) after running a
// vector-search TVF Prepare. Only the CPU TVFs (hnsw, ivfflat) are covered here so
// the test runs in the default (non-GPU) build; ivfpq/cagra share the identical
// Prepare limit-resolution code and are covered end-to-end by the GPU BVT.
func vectorSearchStateLimit(t *testing.T, st tvfState) uint64 {
	t.Helper()
	switch s := st.(type) {
	case *hnswSearchState:
		return s.limit
	case *ivfSearchState:
		return s.limit
	default:
		t.Fatalf("unexpected tvf state type %T", st)
		return 0
	}
}

// TestVectorSearchPrepareResolvesLimitFromIndexReaderParam verifies the k-channel
// wiring the prepared-LIMIT fix relies on (#26869/#26878): for a prepared
// `LIMIT ?` the raw k travels on IndexReaderParam.Limit (node.Limit is dropped to
// avoid a plan-level top truncating the over-fetched candidates), and each TVF's
// Prepare must resolve k from it. arg.Limit still feeds the literal path, and when
// both are present the larger budget wins.
func TestVectorSearchPrepareResolvesLimitFromIndexReaderParam(t *testing.T) {
	proc := testutil.NewProcess(t)
	lit := plan2.MakePlan2Uint64ConstExprWithType

	prepares := []struct {
		name string
		fn   func(*process.Process, *TableFunction) (tvfState, error)
	}{
		{"hnsw", hnswSearchPrepare},
		{"ivf", ivfSearchPrepare},
	}

	run := func(t *testing.T, fn func(*process.Process, *TableFunction) (tvfState, error), argLimit, idxLimit *plan.Expr) uint64 {
		arg := &TableFunction{FuncName: "vector_search", Limit: argLimit}
		if idxLimit != nil {
			arg.IndexReaderParam = &plan.IndexReaderParam{Limit: idxLimit}
		}
		st, err := fn(proc, arg)
		require.NoError(t, err)
		return vectorSearchStateLimit(t, st)
	}

	for _, p := range prepares {
		t.Run(p.name, func(t *testing.T) {
			// literal path: only arg.Limit (already over-fetched at plan time).
			require.Equal(t, uint64(5), run(t, p.fn, lit(5), nil))
			// prepared path: only IndexReaderParam.Limit (the k channel the fix adds).
			require.Equal(t, uint64(7), run(t, p.fn, nil, lit(7)))
			// both present: the larger candidate budget wins.
			require.Equal(t, uint64(9), run(t, p.fn, lit(3), lit(9)))
			require.Equal(t, uint64(9), run(t, p.fn, lit(9), lit(3)))
		})
	}
}
