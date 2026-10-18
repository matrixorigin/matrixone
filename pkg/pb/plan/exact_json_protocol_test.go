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

func TestExactJSONExecutionContracts(t *testing.T) {
	col := func(oid int32) *Expr {
		return &Expr{Typ: Type{Id: oid}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}
	}
	fn := func(id, overload, result int32, args ...*Expr) *Expr {
		return &Expr{Typ: Type{Id: result}, Expr: &Expr_F{F: &Function{
			// Bound identity, not a display name, defines execution semantics.
			Func: &ObjectRef{Obj: int64(id)<<32 | int64(overload), ObjName: "renamed"}, Args: args,
		}}}
	}
	parser := fn(601, 0, 62, col(61))
	for _, test := range []struct {
		name        string
		expr        *Expr
		parse, cast bool
	}{
		{"exact parser", parser, true, false},
		{"nested parser", fn(87, 0, 61, parser), true, false},
		{"parser and cast", fn(21, 0, 33, parser), true, true},
		{"JSON decimal64", fn(21, 0, 32, col(62)), false, true},
		{"JSON decimal128", fn(21, 1, 33, col(62)), false, true},
		{"comparison cast", fn(21, 2, 33, col(62)), false, true},
		{"set cast", fn(21, 3, 33, col(62)), false, true},
		{"strict cast", fn(541, 0, 33, col(62)), false, true},
		{"assignment cast", fn(554, 0, 33, col(62)), false, true},
		{"ignore cast", fn(555, 0, 33, col(62)), false, true},
		{"JSON float", fn(21, 0, 31, col(62)), false, false},
		{"FLOAT decimal", fn(21, 0, 33, col(31)), false, false},
		{"string decimal", fn(21, 0, 33, col(61)), false, false},
		{"JSON decimal256 unchanged", fn(21, 0, 34, col(62)), false, false},
		{"calendar interval identity", fn(598, 0, 61, col(61)), false, false},
		{"main datalink identity", fn(599, 0, 10, col(61)), false, false},
		{"main table collation identity", fn(600, 0, 61, col(61)), false, false},
		{"legacy least identity", fn(122, 0, 61, col(61), col(61)), false, false},
		{"collation least identity", fn(122, 4, 61, col(61), col(61)), false, false},
		{"unrelated function", fn(1, 0, 33, col(62)), false, false},
		{"no source", fn(21, 0, 33), false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.expr.Marshal()
			require.NoError(t, err)
			decoded := new(Expr)
			require.NoError(t, decoded.Unmarshal(data))
			for _, expr := range []*Expr{test.expr, decoded} {
				features, err := RequiredRemoteExpressionFeatures(expr)
				require.NoError(t, err)
				require.Equal(t, test.name == "calendar interval identity", features.CalendarIntervalNormalization)
				require.Equal(t, test.name == "main table collation identity", features.TableCollationFunction)
				require.Equal(t, test.name == "collation least identity", features.LeastGreatestCollationSemantics)
				require.Equal(t, test.parse, features.ExactJSONParsing)
				require.Equal(t, test.cast, features.ExactJSONDecimalCast)
				if test.parse || test.cast {
					require.True(t, features.Any())
				}
			}
		})
	}
	// An incomplete in-memory builder node has no wire representation: gogo's
	// generated marshaller requires non-nil repeated expression elements.
	features, err := RequiredRemoteExpressionFeatures(fn(21, 0, 33, nil))
	require.NoError(t, err)
	require.False(t, features.ExactJSONDecimalCast)
}
