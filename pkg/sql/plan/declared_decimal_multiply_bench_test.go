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

package plan_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkDeclaredDecimalMultiply(b *testing.B) {
	for _, widths := range [][2]int32{{15, 15}, {20, 17}, {20, 18}, {20, 19}} {
		for _, rows := range []int{1, 256, 8192} {
			benchmarkMultiply(b, widths, rows, "flat")
		}
	}
	for _, shape := range []string{"nullable", "allnull", "constant"} {
		benchmarkMultiply(b, [2]int32{20, 18}, 256, shape)
	}
}

func benchmarkMultiply(b *testing.B, widths [2]int32, rows int, shape string) {
	b.Run(fmt.Sprintf("d%dx%d_rows%d_%s", widths[0], widths[1], rows, shape), func(b *testing.B) {
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithOwnedMPool(b, "", mp)
		input := batch.NewWithSize(2)
		b.Cleanup(func() { input.Clean(mp) })
		args := make([]*pb.Expr, 2)
		for col, width := range widths {
			oid := types.T_decimal64
			if width > 18 {
				oid = types.T_decimal128
			}
			typ := types.New(oid, width, 0)
			text := ""
			for range width {
				text += "9"
			}
			var err error
			if oid == types.T_decimal64 {
				value, e := types.ParseDecimal64(text, width, 0)
				if e != nil {
					b.Fatal(e)
				}
				if shape == "constant" {
					input.Vecs[col], err = vector.NewConstFixed(typ, value, rows, mp)
				} else {
					input.Vecs[col] = vector.NewVec(typ)
					for row := range rows {
						if err = vector.AppendFixed(input.Vecs[col], value, shape == "allnull" || shape == "nullable" && row%17 == 0, mp); err != nil {
							b.Fatal(err)
						}
					}
				}
			} else {
				value, e := types.ParseDecimal128(text, width, 0)
				if e != nil {
					b.Fatal(e)
				}
				if shape == "constant" {
					input.Vecs[col], err = vector.NewConstFixed(typ, value, rows, mp)
				} else {
					input.Vecs[col] = vector.NewVec(typ)
					for row := range rows {
						if err = vector.AppendFixed(input.Vecs[col], value, shape == "allnull" || shape == "nullable" && row%17 == 0, mp); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			if err != nil {
				b.Fatal(err)
			}
			args[col] = &pb.Expr{Typ: pb.Type{Id: int32(typ.Oid), Width: width, Scale: 0}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: int32(col)}}}
		}
		input.SetRowCount(rows)
		expr, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "*", args)
		if err != nil {
			b.Fatal(err)
		}
		executor, err := colexec.NewExpressionExecutor(proc, expr)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(executor.Free)
		// Record dispatch metadata, never inspect or verify benchmark values.
		b.Logf("planned result oid=%d precision=%d scale=%d", expr.Typ.Id, expr.Typ.Width, expr.Typ.Scale)
		if _, err = executor.Eval(proc, []*batch.Batch{input}, nil); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err = executor.Eval(proc, []*batch.Batch{input}, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
