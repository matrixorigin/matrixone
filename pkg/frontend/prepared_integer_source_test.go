// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func BenchmarkPreparedIntegerSourceBinding(b *testing.B) {
	cc := plan2.NewMockOptimizer(false).CurrentContext()
	const query = "insert into constraint_test.emp(empno,sal) select q,q+0E0 from (select ?/2 q) s"
	values := []any{plan2.ParamValue{Value: "5", IsBinaryProtocol: true,
		HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := buildPreparedIntegerSource(context.Background(), nil, cc, query, "", values, []int32{0})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPreparedIntegerSourceRetryKeepsRuntimeDomain(t *testing.T) {
	cc := plan2.NewMockOptimizer(false).CurrentContext()
	original := cc.GetContext()
	const sql = "insert into constraint_test.emp(empno,sal) select q,q+0E0 from (select ?/2 q) s"
	for _, typ := range []types.T{types.T_int64, types.T_float64, types.T_int64} {
		retry := newPreparedExecutionRetry([]any{plan2.ParamValue{
			Value: "5", IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: typ.ToType(),
		}}, true)
		retry.integerSourceSQL = sql
		retry.integerSourcePositions = []int32{0}
		p, err := buildPlanForCompileRetry(t.Context(), nil, cc, nil, false, retry)
		require.NoError(t, err)
		want := types.T_decimal256
		if typ.IsFloat() {
			want = types.T_float64
		}
		found := 0
		var inspect func(*plan2.Expr)
		inspect = func(e *plan2.Expr) {
			if f := e.GetF(); f != nil {
				if f.Func.ObjName == "/" {
					found++
					require.Equal(t, int32(want), e.Typ.Id)
				}
				for _, a := range f.Args {
					inspect(a)
				}
			}
		}
		for _, n := range p.GetQuery().Nodes {
			for _, e := range n.ProjectList {
				inspect(e)
			}
		}
		require.Positive(t, found)
		require.Equal(t, original, cc.GetContext())
	}
	_, err := buildPreparedIntegerSource(t.Context(), nil, cc, "insert into", "", nil, nil)
	require.Error(t, err)
	require.Equal(t, original, cc.GetContext())
}
