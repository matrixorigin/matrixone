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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaActualBinderWorkAndRecursionBoundaries(t *testing.T) {
	for _, depth := range []int{256, 257} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			request := f.request(t)
			require.NoError(t, request.open())
			state := newViewSchemaDerivation(request)
			defer state.close()
			builder := NewQueryBuilder(planpb.Query_SELECT, state.compiler, false, false)
			bindCtx := NewBindContext(builder, nil)
			binder := NewProjectionBinder(builder, bindCtx, NewHavingBinder(builder, bindCtx))
			var expr tree.Expr = tree.NewNumVal(int64(1), "1", false, tree.P_int64)
			for i := 1; i < depth; i++ {
				expr = &tree.ParenExpr{Expr: expr}
			}
			result, err := binder.BindExpr(expr, 0, true)
			if depth == 256 {
				require.NoError(t, err)
				require.NotNil(t, result)
			} else {
				require.ErrorIs(t, err, ErrViewSchemaLimit)
				require.Nil(t, result)
			}
			require.Zero(t, state.recursion, "all recursive frames must unwind after rejection")
		})
	}
	t.Run("operations", func(t *testing.T) {
		f := newViewSchemaTestFixture(t)
		request := f.request(t)
		require.NoError(t, request.open())
		state := newViewSchemaDerivation(request)
		defer state.close()
		builder := NewQueryBuilder(planpb.Query_SELECT, state.compiler, false, false)
		bindCtx := NewBindContext(builder, nil)
		binder := NewProjectionBinder(builder, bindCtx, NewHavingBinder(builder, bindCtx))
		expr := tree.NewNumVal(int64(1), "1", false, tree.P_int64)
		for i := 0; i < 65536; i++ {
			_, err := binder.BindExpr(expr, 0, true)
			require.NoError(t, err)
		}
		result, err := binder.BindExpr(expr, 0, true)
		require.ErrorIs(t, err, ErrViewSchemaLimit)
		require.Nil(t, result)
		require.Zero(t, state.recursion)
	})
}

func TestViewSchemaMemoByteLimitRetainsCompleteResults(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	// One large but bounded catalog default is shared by a few roots. This
	// crosses the retained-byte bound without thousands of large objects.
	source := f.compiler.tables["nation"].Cols[1]
	source.Default = &planpb.Default{NullAbility: false, OriginString: strings.Repeat("x", 1<<20)}
	request := f.request(t)
	var previousBytes, previousEntries int
	declined := false
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("bytes_%d", i)
		f.addView(t, name, "select n_name from nation")
		result := viewSchemaTestResult(t, request, name)
		columns, err := result.Columns()
		require.NoError(t, err)
		require.True(t, proto.Equal(source.Default, columns[0].Default), "the complete large default must survive memo admission")
		result.Release()
		require.LessOrEqual(t, request.memoBytes, viewSchemaMemoLimit)
		if len(request.memo) == previousEntries {
			declined = true
			require.Equal(t, previousBytes, request.memoBytes)
		}
		previousBytes, previousEntries = request.memoBytes, len(request.memo)
	}
	require.True(t, declined, "the real descriptions must reach the byte admission boundary")
	require.Greater(t, request.binds, uint64(len(request.memo)))
	request.Close()
	require.Zero(t, f.generation.Used())
}
