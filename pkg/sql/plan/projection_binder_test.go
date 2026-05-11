// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIsNRange(t *testing.T) {
	tests := []struct {
		name     string
		frame    *tree.FrameClause
		expected bool
	}{
		{
			name: "RANGE UNBOUNDED PRECEDING to CURRENT ROW - not N range",
			frame: &tree.FrameClause{
				Type:  tree.Range,
				Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
				End:   &tree.FrameBound{Type: tree.CurrentRow},
			},
			expected: false,
		},
		{
			name: "RANGE UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING - not N range",
			frame: &tree.FrameClause{
				Type:  tree.Range,
				Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
				End:   &tree.FrameBound{Type: tree.Following, UnBounded: true},
			},
			expected: false,
		},
		{
			name: "RANGE N PRECEDING to CURRENT ROW - is N range",
			frame: &tree.FrameClause{
				Type: tree.Range,
				Start: &tree.FrameBound{
					Type: tree.Preceding,
					Expr: tree.NewNumVal(int64(5), "5", false, tree.P_int64),
				},
				End: &tree.FrameBound{Type: tree.CurrentRow},
			},
			expected: true,
		},
		{
			name: "RANGE CURRENT ROW to N FOLLOWING - is N range",
			frame: &tree.FrameClause{
				Type:  tree.Range,
				Start: &tree.FrameBound{Type: tree.CurrentRow},
				End: &tree.FrameBound{
					Type: tree.Following,
					Expr: tree.NewNumVal(int64(10), "10", false, tree.P_int64),
				},
			},
			expected: true,
		},
		{
			name: "RANGE N PRECEDING to N FOLLOWING - is N range",
			frame: &tree.FrameClause{
				Type: tree.Range,
				Start: &tree.FrameBound{
					Type: tree.Preceding,
					Expr: tree.NewNumVal(int64(5), "5", false, tree.P_int64),
				},
				End: &tree.FrameBound{
					Type: tree.Following,
					Expr: tree.NewNumVal(int64(5), "5", false, tree.P_int64),
				},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isNRange(tc.frame)
			require.Equal(t, tc.expected, result)
		})
	}
}
