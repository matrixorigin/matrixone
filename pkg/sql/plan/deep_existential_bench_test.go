// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

// These old-success controls also run unchanged against the parent revision.
// Include an eligible type at depth two that needs no new decorrelation.
func BenchmarkExistentialPlanningControls(b *testing.B) {
	for _, tc := range []struct{ name, sql string }{
		{"shallow", `select o.n_nationkey from nation o where exists(select 1 from nation i where i.n_regionkey=o.n_regionkey)`},
		{"depth_two_old", `select o.n_nationkey from nation o where exists(select 1 from nation i where i.n_regionkey=o.n_regionkey and exists(select 1 from nation j where j.n_nationkey=i.n_nationkey))`},
		{"scalar", `select (select max(i.n_nationkey) from nation i where i.n_regionkey=o.n_regionkey) from nation o`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			opt := NewMockOptimizer(false)
			ctx := opt.CurrentContext()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stmts, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
				if err != nil {
					b.Fatal(err)
				}
				_, err = BuildPlan(ctx, stmts[0], false)
				stmts[0].Free()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
