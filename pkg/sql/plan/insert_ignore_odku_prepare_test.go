// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedNoKeyODKUParameters(t *testing.T) {
	for _, modifier := range []string{"", "ignore "} {
		cases := []struct {
			name     string
			rhs      string
			expected int
		}{
			{name: "parameter", rhs: "?", expected: 3},
			{name: "right arithmetic parameter", rhs: "? + 1", expected: 3},
			{name: "left arithmetic parameter", rhs: "1 + ?", expected: 3},
			{name: "target reference and parameter", rhs: "pid + ?", expected: 3},
			{name: "two parameters", rhs: "? + ?", expected: 4},
			{name: "nested function parameter", rhs: "coalesce(?, 1)", expected: 3},
			{name: "user variable", rhs: "@x", expected: 2},
			{name: "literal scalar subquery", rhs: "(select 1)", expected: 2},
			{name: "scalar subquery parameter", rhs: "(select ?)", expected: 3},
		}
		for _, tc := range cases {
			t.Run(modifier+tc.name, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				sql := fmt.Sprintf(
					"prepare s from insert %sinto insert_fk_no_key_c values (?, ?) on duplicate key update pid = %s",
					modifier,
					tc.rhs,
				)
				p, err := runOneStmt(mock, t, sql)
				require.NoError(t, err)

				require.Equal(t, tc.expected, len(p.GetDcl().GetPrepare().GetParamTypes()))
			})
		}
	}
}
