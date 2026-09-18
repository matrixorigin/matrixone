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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiresJSONAggregateOpaqueValuesOnlyMatchesValueArguments(t *testing.T) {
	text := &Expr{Typ: Type{Id: 61}}
	binary := &Expr{Typ: Type{Id: jsonAggregateBinaryType}}
	array := jsonAggregateOpaqueTestExpr(jsonArrayAggFunctionID, binary)
	objectKeyOnly := jsonAggregateOpaqueTestExpr(jsonObjectAggFunctionID, binary, text)
	objectValue := jsonAggregateOpaqueTestExpr(jsonObjectAggFunctionID, text, binary)

	required, err := RequiresJSONAggregateOpaqueValues(&Query{
		Nodes: []*Node{{
			ProjectList: []*Expr{objectKeyOnly, objectValue},
		}},
	})
	require.NoError(t, err)
	require.True(t, required)

	required, err = RequiresJSONAggregateOpaqueValues(array)
	require.NoError(t, err)
	require.True(t, required)

	required, err = RequiresJSONAggregateOpaqueValues(objectKeyOnly)
	require.NoError(t, err)
	require.False(t, required)

	for _, typeID := range []int32{
		jsonAggregateBitType,
		jsonAggregateBinaryType,
		jsonAggregateVarbinaryType,
		jsonAggregateBlobType,
	} {
		required, err = RequiresJSONAggregateOpaqueValues(
			jsonAggregateOpaqueTestExpr(jsonArrayAggFunctionID, &Expr{Typ: Type{Id: typeID}}))
		require.NoError(t, err)
		require.True(t, required)
	}

	// DISTINCT is encoded in the high bit of Function.Obj and must not hide
	// the aggregate function ID from the rollout gate.
	distinctObj := int64(uint64(jsonArrayAggFunctionID) << 32)
	distinctObj = int64(uint64(distinctObj) | (uint64(1) << 63))
	distinct := &Expr{Expr: &Expr_F{F: &Function{
		Func: &ObjectRef{Obj: distinctObj},
		Args: []*Expr{binary},
	}}}
	required, err = RequiresJSONAggregateOpaqueValues(distinct)
	require.NoError(t, err)
	require.True(t, required)
}

func jsonAggregateOpaqueTestExpr(functionID int32, args ...*Expr) *Expr {
	return &Expr{
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: int64(functionID) << 32},
			Args: args,
		}},
	}
}
