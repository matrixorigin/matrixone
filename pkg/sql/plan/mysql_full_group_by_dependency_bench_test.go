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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func BenchmarkGroupByDependencyPlanning(b *testing.B) {
	cases := []struct{ name, sql string }{{"ordinary", "select empno,ename from constraint_test.emp where empno=1"}}
	for _, depth := range []int{8, 32} {
		query := "select empno,ename,sal from constraint_test.emp"
		for i := 0; i < depth; i++ {
			query = "select empno,ename,sal from (" + query + ") d"
		}
		for _, explicit := range []bool{false, true} {
			group := "empno"
			if explicit {
				group += ",ename"
			}
			cases = append(cases, struct{ name, sql string }{fmt.Sprintf("projection_%d_explicit_%t", depth, explicit), "select empno,ename,sum(sal) from (" + query + ") d group by " + group})
		}
	}
	for _, joins := range []int{4, 16} {
		from := "constraint_test.emp e"
		previous := "e"
		for i := 0; i < joins; i++ {
			next := fmt.Sprintf("d%d", i)
			from += " left join constraint_test.dept " + next + " on " + previous + ".deptno=" + next + ".deptno"
			previous = next
		}
		for _, explicit := range []bool{false, true} {
			group := "e.empno"
			if explicit {
				group += "," + previous + ".dname"
			}
			cases = append(cases, struct{ name, sql string }{fmt.Sprintf("joins_%d_explicit_%t", joins, explicit), "select e.empno," + previous + ".dname,count(*) from " + from + " group by " + group})
		}
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			opt := NewMockOptimizer(false)
			opt.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
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
