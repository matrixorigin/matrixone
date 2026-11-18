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

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestExtractColRefInFilterNested(t *testing.T) {
	col := func(rel, pos int32) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	fn := func(args ...*pb.Expr) *pb.Expr { return &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: args}}} }
	literal := &pb.Expr{Expr: &pb.Expr_Lit{Lit: &pb.Literal{}}}
	param := &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{}}}
	unsupported := &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{}}}
	a, b, c := col(1, 1), col(1, 2), col(2, 1)
	cases := []struct {
		name string
		expr *pb.Expr
		ok   bool
	}{
		{"single", fn(a, literal), true},
		{"same_nested", fn(a, fn(a, param)), true},
		{"multi_nested", fn(a, fn(b, c)), false},
		{"deep_multi", fn(a, fn(a, fn(b, c))), false},
		{"swapped_multi", fn(fn(b, c), a), false},
		{"different_tag", fn(a, c), false},
		{"different_position", fn(a, b), false},
		{"unsupported", fn(a, unsupported), false},
		{"constant_first", fn(literal, a), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractColRefInFilter(tc.expr)
			if tc.ok {
				require.Equal(t, a.GetCol(), got)
			} else {
				require.Nil(t, got)
			}
		})
	}
}

func BenchmarkExtractColRefInFilter(b *testing.B) {
	col := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 1}}}
	lit := &pb.Expr{Expr: &pb.Expr_Lit{Lit: &pb.Literal{}}}
	wide := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: append([]*pb.Expr{col}, make([]*pb.Expr, 63)...)}}}
	for i := 1; i < len(wide.GetF().Args); i++ {
		wide.GetF().Args[i] = lit
	}
	deep := col
	for i := 0; i < 64; i++ {
		deep = &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{deep, lit}}}}
	}
	for _, tc := range []struct {
		name string
		expr *pb.Expr
	}{{"wide", wide}, {"deep", deep}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if extractColRefInFilter(tc.expr) == nil {
					b.Fatal("missing column")
				}
			}
		})
	}
}
