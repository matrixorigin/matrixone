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

package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func newViewSchemaWideDefaultFixture(t testing.TB) *viewSchemaTestFixture {
	t.Helper()
	f := newViewSchemaTestFixture(t)
	value := strings.Repeat("x", 1<<20)
	source := f.compiler.tables["nation"].Cols[1]
	source.Typ.Width, source.Typ.NotNullable = int32(len(value)), true
	source.Default = &planpb.Default{
		Expr: makePlan2StringConstExprWithType(value), OriginString: "'" + value + "'",
	}
	items := make([]string, 32)
	for i := range items {
		items[i] = fmt.Sprintf("n_name as c%d", i)
	}
	f.addView(t, "wide_v", "select "+strings.Join(items, ",")+" from nation")
	f.addView(t, "first_v", "select c0 as label from wide_v")
	f.addView(t, "second_v", "select c31 as label from wide_v")
	return f
}

type viewSchemaWideResult struct {
	columns      []*ColDef
	origins      []ViewSchemaColumnProvenance
	dependencies []ViewDependency
	protocol     int64
}

func TestViewSchemaWideNestedDefaultDeclinesMemoBeforeEncoding(t *testing.T) {
	f := newViewSchemaWideDefaultFixture(t)
	before := proto.Clone(f.compiler.tables["nation"])
	source := f.compiler.tables["nation"].Cols[1]
	// The input and selected output fit the request's normal limits. Only the
	// optional intermediate boundary would repeat the large default 32 times.
	require.Less(t, f.compiler.tables["nation"].ProtoSize(), viewSchemaInputLimit)
	require.Greater(t, 32*source.Default.ProtoSize(), viewSchemaMemoLimit)
	want := make(map[string]viewSchemaWideResult)
	for _, disabled := range []bool{true, false} {
		request := f.request(t)
		request.memoDisabled = disabled
		for _, name := range []string{"first_v", "second_v", "first_v"} {
			result := viewSchemaTestResult(t, request, name)
			columns, err := result.Columns()
			require.NoError(t, err)
			origins, err := result.Provenance()
			require.NoError(t, err)
			dependencies, err := result.Dependencies()
			require.NoError(t, err)
			protocol, err := result.RequiredProtocolVersion()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.True(t, proto.Equal(source.Default, columns[0].Default), "the narrow root must retain its complete default")
			require.Len(t, origins, 1)
			require.Equal(t, ProvenanceSingleSource, origins[0].State)
			require.NotNil(t, origins[0].Source)
			require.True(t, proto.Equal(source.Default, origins[0].Source.Default))
			require.Len(t, dependencies, 3)
			got := viewSchemaWideResult{columns: columns, origins: origins, dependencies: dependencies, protocol: protocol}
			if previous, ok := want[name]; ok {
				require.True(t, proto.Equal(&TableDef{Cols: previous.columns}, &TableDef{Cols: got.columns}), "memo and authoritative binding return the same complete columns")
				require.Equal(t, previous.dependencies, got.dependencies)
				require.Equal(t, previous.protocol, got.protocol)
				require.Equal(t, previous.origins[0].State, got.origins[0].State)
				require.Equal(t, previous.origins[0].CTASDefaultPolicy, got.origins[0].CTASDefaultPolicy)
				require.Equal(t, previous.origins[0].SourceTableID, got.origins[0].SourceTableID)
				require.Equal(t, previous.origins[0].Source.Typ, got.origins[0].Source.Typ)
				require.Equal(t, previous.origins[0].Source.NullAbility, got.origins[0].Source.NullAbility)
				require.True(t, proto.Equal(previous.origins[0].Source.Default, got.origins[0].Source.Default))
			} else {
				want[name] = got
			}
			result.Release()
			require.Empty(t, request.nestedMemo, "the oversized optional boundary must not be retained")
		}
		if disabled {
			require.Zero(t, request.hits)
		} else {
			require.Equal(t, uint64(1), request.hits, "the small completed root remains eligible for ordinary memo reuse")
			require.Len(t, request.memo, 2)
		}
		request.Close()
		require.Zero(t, f.generation.Used())
		require.False(t, f.generation.Closed())
	}
	require.True(t, proto.Equal(before, f.compiler.tables["nation"]), "memo sizing cannot mutate the catalog default")
}

func BenchmarkViewSchemaWideNestedDefault(b *testing.B) {
	for _, disabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("memo_disabled=%t", disabled), func(b *testing.B) {
			f := newViewSchemaWideDefaultFixture(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A new request keeps each iteration on the intermediate-boundary
				// path instead of measuring only a previously memoized root.
				request := NewViewSchemaRequest(b.Context(), f.provider)
				request.memoDisabled = disabled
				result, err := request.Describe("tpch", "first_v", nil)
				if err != nil {
					request.Close()
					b.Fatal(err)
				}
				_, columnErr := result.Columns()
				_, originErr := result.Provenance()
				_, dependencyErr := result.Dependencies()
				_, protocolErr := result.RequiredProtocolVersion()
				result.Release()
				request.Close()
				require.NoError(b, columnErr)
				require.NoError(b, originErr)
				require.NoError(b, dependencyErr)
				require.NoError(b, protocolErr)
			}
			b.StopTimer()
			require.Zero(b, f.generation.Used())
		})
	}
}
