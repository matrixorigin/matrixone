//go:build gpu

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

// TestVectorSearchPrepareResolvesLimitFromIndexReaderParamGPU is the ivfpq/cagra
// counterpart of TestVectorSearchPrepareResolvesLimitFromIndexReaderParam: their
// real (GPU) Prepare implementations must resolve k from IndexReaderParam.Limit
// (the prepared-LIMIT k channel), fall back to arg.Limit for the literal path,
// and take the larger of the two. The CPU stubs have no limit logic, so this must
// run under -tags gpu.
func TestVectorSearchPrepareResolvesLimitFromIndexReaderParamGPU(t *testing.T) {
	proc := testutil.NewProcess(t)
	lit := plan2.MakePlan2Uint64ConstExprWithType

	prepares := []struct {
		name    string
		fn      func(*process.Process, *TableFunction) (tvfState, error)
		limitOf func(tvfState) uint64
	}{
		{"ivfpq", ivfpqSearchPrepare, func(st tvfState) uint64 { return st.(*ivfpqSearchState).limit }},
		{"cagra", cagraSearchPrepare, func(st tvfState) uint64 { return st.(*cagraSearchState).limit }},
	}

	run := func(t *testing.T, fn func(*process.Process, *TableFunction) (tvfState, error), argLimit, idxLimit *plan.Expr) tvfState {
		arg := &TableFunction{FuncName: "vector_search", Limit: argLimit}
		if idxLimit != nil {
			arg.IndexReaderParam = &plan.IndexReaderParam{Limit: idxLimit}
		}
		st, err := fn(proc, arg)
		require.NoError(t, err)
		return st
	}

	for _, p := range prepares {
		t.Run(p.name, func(t *testing.T) {
			// literal path: only arg.Limit (already over-fetched at plan time).
			require.Equal(t, uint64(5), p.limitOf(run(t, p.fn, lit(5), nil)))
			// prepared path: only IndexReaderParam.Limit (the k channel the fix adds).
			require.Equal(t, uint64(7), p.limitOf(run(t, p.fn, nil, lit(7))))
			// both present: the larger candidate budget wins.
			require.Equal(t, uint64(9), p.limitOf(run(t, p.fn, lit(3), lit(9))))
			require.Equal(t, uint64(9), p.limitOf(run(t, p.fn, lit(9), lit(3))))
		})
	}
}
