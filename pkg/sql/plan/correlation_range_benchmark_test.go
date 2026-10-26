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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func BenchmarkScalarExistingRoutes(b *testing.B) {
	for _, tc := range []struct{ name, sql string }{
		{"ordinary", `SELECT N_NATIONKEY FROM NATION WHERE N_REGIONKEY=1`},
		{"equality", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY) FROM NATION o`},
		{"range", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"inline_cte", `WITH q AS (SELECT * FROM NATION) SELECT (SELECT MAX(i.N_NATIONKEY) FROM q i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM q o`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(b))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stmt, err := mysql.ParseOne(b.Context(), tc.sql, 1)
				require.NoError(b, err)
				p, err := BuildPlan(ctx, stmt, false)
				stmt.Free()
				require.NoError(b, err)
				require.Nil(b, p.GetQuery().Correlation)
			}
		})
	}
}
