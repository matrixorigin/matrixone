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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// Keep declaration resolution visible on ordinary SQL, not only FIELD/CASE.
func BenchmarkStringSourceDeclarationBinding(b *testing.B) {
	for _, depth := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("reverse-depth-%d", depth), func(b *testing.B) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(b))
			sql := "select " + strings.Repeat("reverse(", depth) + "n_name" + strings.Repeat(")", depth) + " from nation"
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(stmt.Free)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := BuildPlan(ctx, stmt, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
