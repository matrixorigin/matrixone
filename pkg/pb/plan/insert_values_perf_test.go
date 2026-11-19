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

import "testing"

// INSERT VALUES plans retain one expression per cell. Measure the shared
// protocol feature traversal with both single-row and batched inputs.
func BenchmarkRequiredRemoteExpressionFeaturesValues(b *testing.B) {
	for _, shape := range []struct {
		name       string
		rows, cols int
	}{
		{"single", 1, 2},
		{"narrow", 1000, 2},
		{"wide", 1000, 100},
	} {
		b.Run(shape.name, func(b *testing.B) {
			rowset := &RowsetData{Cols: make([]*ColData, shape.cols)}
			for c := range rowset.Cols {
				column := &ColData{Data: make([]*RowsetExpr, shape.rows)}
				for r := range column.Data {
					column.Data[r] = &RowsetExpr{Expr: &Expr{Typ: Type{Id: 23}, Expr: &Expr_Lit{Lit: &Literal{Value: &Literal_I64Val{I64Val: int64(r)}}}}}
				}
				rowset.Cols[c] = column
			}
			owner := &Plan{Plan: &Plan_Query{Query: &Query{Nodes: []*Node{{RowsetData: rowset}}}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := RequiredRemoteExpressionFeatures(owner); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
