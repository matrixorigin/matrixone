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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEnumSourceAssignmentFeature(t *testing.T) {
	for _, tc := range []struct {
		id, overload int64
		name         string
		want         bool
	}{
		{306, 1, "", true}, {306, 1, "unrelated_name", true}, {306, 0, "cast_value_to_index", false}, {305, 1, "cast_value_to_index", false},
	} {
		expr := &Expr{Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: tc.id<<32 | tc.overload, ObjName: tc.name}}}}
		owner := &TableDef{Cols: []*ColDef{{GeneratedCol: &GeneratedCol{Expr: expr}}}}
		data, err := owner.Marshal()
		require.NoError(t, err)
		var decoded TableDef
		require.NoError(t, decoded.Unmarshal(data))
		for _, input := range []any{expr, owner, &decoded} {
			features, err := RequiredRemoteExpressionFeatures(input)
			require.NoError(t, err)
			require.Equal(t, tc.want, features.EnumSourceAssignment)
			require.Equal(t, tc.want, features.Any())
		}
	}
}

func TestEnumSourceAssignmentInDependentExpressions(t *testing.T) {
	expr := &Expr{Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: 306<<32 | 1}}}}
	for _, owner := range []*Query{
		{Nodes: []*Node{{ProjectList: []*Expr{{Expr: &Expr_Dependent{Dependent: &DependentExpr{Inputs: []*Expr{expr}}}}}}}},
		{Nodes: []*Node{{}, {ProjectList: []*Expr{expr}}}, Correlation: &CorrelationSpec{Programs: []*CorrelationProgram{{RootNode: 1}}}},
	} {
		data, err := owner.Marshal()
		require.NoError(t, err)
		var decoded Query
		require.NoError(t, decoded.Unmarshal(data))
		features, err := RequiredRemoteExpressionFeatures(&decoded)
		require.NoError(t, err)
		require.True(t, features.EnumSourceAssignment)
		require.True(t, features.Any())
	}
}
