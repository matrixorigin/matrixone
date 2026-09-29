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

func TestUnqualifiedStarRightJoinUsingOrder(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "explicit using",
			sql: "select * from (select 1 as id, 2 as left_only) l " +
				"right join (select 1 as id, 3 as right_only) r using (id)",
			want: []string{"id", "right_only", "left_only"},
		},
		{
			name: "natural right",
			sql: "select * from (select 1 as id, 2 as left_only) l " +
				"natural right join (select 1 as id, 3 as right_only) r",
			want: []string{"id", "right_only", "left_only"},
		},
		{
			name: "right join on keeps existing order",
			sql: "select * from (select 1 as left_id, 2 as left_only) l " +
				"right join (select 1 as right_id, 3 as right_only) r " +
				"on l.left_id = r.right_id",
			want: []string{"left_id", "left_only", "right_id", "right_only"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			p, err := runOneStmt(optimizer, t, test.sql)
			require.NoError(t, err)
			require.Equal(t, test.want, p.GetQuery().Headings)
		})
	}
}
