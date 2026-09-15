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

package plan

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestPreparedIntegerSourceDependencies(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      []int32
	}{
		{"shared", "insert into constraint_test.emp(empno,sal) select q,q+0E0 from (select ?/2 q) s", []int32{0}},
		{"predicate_is_not_result", "insert into constraint_test.emp(empno) select q from (select ?/2 q) s where ?>0", []int32{0}},
		{"sum", "insert into constraint_test.emp(empno) select sum(?/2)", []int32{0}},
		{"window", "insert into constraint_test.emp(empno) select sum(?/2) over ()", []int32{0}},
		{"union", "insert into constraint_test.emp(empno) select ?/2 union all select ?/2", []int32{0, 1}},
		{"group", "insert into constraint_test.emp(empno) select q from (select ?/2 q) s group by q", []int32{0}},
		{"scalar_relation", "insert into constraint_test.emp(empno) select (select ?/2 from constraint_test.dept limit 1)", []int32{0}},
		// The optimizer inlines this scalar expression; it uses write-root specialization.
		{"scalar_inlined", "insert into constraint_test.emp(empno) select (select ?/2)", []int32{}},
		{"direct_write", "insert into constraint_test.emp(empno) values (?/2)", []int32{}},
		{"explicit_boundary", "insert into constraint_test.emp(empno) select cast(q as double) from (select ?/2 q) s", []int32{}},
		{"non_integer_target", "insert into constraint_test.emp(sal) select ?/2", []int32{}},
		{"select", "select ?/2", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPreparedAggregatePlan(t, tc.sql).Plan
			before := proto.Clone(p)
			require.Equal(t, tc.want, PreparedIntegerSourceParamPositions(p))
			require.True(t, proto.Equal(before, p), "dependency discovery must not mutate the prepared plan")
		})
	}
}

func TestPreparedIntegerBindingContextIsolation(t *testing.T) {
	original := NewMockOptimizer(false).CurrentContext()
	ctx := original.GetContext()
	wrapped := WithPreparedIntegerBindings(t.Context(), original, []any{
		ParamValue{Value: "5", HasRuntimeType: true, RuntimeType: types.T_float64.ToType(), IsBinaryProtocol: true},
		ParamValue{Value: "5", HasSourceType: true, SourceType: types.T_int64.ToType()},
	}, []int32{1})
	_, ok := preparedIntegerBinding(wrapped.GetContext(), 1)
	require.False(t, ok)
	typ, ok := preparedIntegerBinding(wrapped.GetContext(), 2)
	require.True(t, ok)
	require.Equal(t, types.T_int64, typ.Oid)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	wrapped = WithPreparedIntegerBindings(cancelled, original, nil, nil)
	require.ErrorIs(t, wrapped.GetContext().Err(), context.Canceled)
	require.Equal(t, ctx, original.GetContext())

	wrapped.SetContext(t.Context())
	require.Equal(t, ctx, original.GetContext())
	_, ok = preparedIntegerBinding(original.GetContext(), 2)
	require.False(t, ok)
	original.SetContext(nil)
	wrapped = WithPreparedIntegerBindings(cancelled, original, nil, nil)
	require.ErrorIs(t, wrapped.GetContext().Err(), context.Canceled)
	require.Nil(t, original.GetContext())
	wrapped = WithPreparedIntegerBindings(nil, original, nil, nil)
	require.NotNil(t, wrapped.GetContext())
}
