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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// COPY rebinds each column's default against the complete row schema.
func BenchmarkDefaultBinderWideRow(b *testing.B) {
	for _, width := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			ctx := context.Background()
			typ := Type{Id: int32(types.T_int64)}
			cols := make([]*ColDef, width)
			for i := range cols {
				cols[i] = &ColDef{Name: fmt.Sprintf("c%d", i), Typ: typ}
			}
			null := &tree.NumVal{ValType: tree.P_null}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for range cols {
					expr, err := NewDefaultBinderWithColumns(ctx, typ, cols).BindExpr(null, 0, true)
					if err != nil || expr.GetLit() == nil || !expr.GetLit().Isnull {
						b.Fatalf("NULL binding: expr=%v err=%v", expr, err)
					}
				}
			}
		})
	}
}

func BenchmarkDefaultBinderNamedScope(b *testing.B) {
	ctx := context.Background()
	typ := Type{Id: int32(types.T_varchar)}
	cols := []string{"collation", "charset", "id", "default", "compiled", "sortlen", "pad_attribute"}
	ref := tree.NewUnresolvedColName("pad_attribute")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		expr, err := NewDefaultBinder(ctx, nil, nil, typ, cols).BindColRef(ref, 0, true)
		if err != nil || expr.GetCol().ColPos != 6 || expr.Typ.Id != typ.Id {
			b.Fatalf("named binding: expr=%v err=%v", expr, err)
		}
	}
}

func BenchmarkConstructCreateTableWideRow(b *testing.B) {
	for _, width := range []int{1, 8, 256, 1024, 4096} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			def := &TableDef{Name: "wide", Cols: make([]*ColDef, width)}
			for i := range def.Cols {
				def.Cols[i] = &ColDef{
					Name: fmt.Sprintf("c%d", i), Typ: Type{Id: int32(types.T_int64)},
					Default: &planpb.Default{NullAbility: true},
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ddl, _, err := ConstructCreateTableSQL(nil, def, nil, false, nil)
				if err != nil || ddl == "" {
					b.Fatalf("SHOW CREATE: ddl=%q err=%v", ddl, err)
				}
			}
		})
	}
}
