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

package mysql

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkNestedSetQueryContext(b *testing.B) {
	for _, depth := range []int{128, 512, 1536} {
		sql := strings.Repeat("SELECT 1 UNION (", depth) + "SELECT 1" + strings.Repeat(")", depth)
		b.Run(fmt.Sprintf("parser/%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(sql)))
			parser := &MySQLParser{}
			for b.Loop() {
				statements, err := parser.ParseWithSQLMode(b.Context(), sql, 0, "")
				if err != nil {
					b.Fatal(err)
				}
				for _, statement := range statements {
					statement.Free()
				}
			}
		})
		b.Run(fmt.Sprintf("digest/%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(sql)))
			for b.Loop() {
				if _, _, err := StatementDigest(b.Context(), sql, ""); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
