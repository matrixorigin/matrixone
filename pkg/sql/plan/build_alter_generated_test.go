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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyPreservedGeneratedSourceProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*TableDef, *TableDef, *AlterTableContext)
		want   map[string]bool
	}{
		{"unchanged chain", nil, map[string]bool{"g": true, "h": true}},
		{"assignment cast", func(old, _ *TableDef, _ *AlterTableContext) {
			old.Cols[1].GeneratedCol.Expr = &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{
				old.Cols[1].GeneratedCol.Expr, {Expr: &pb.Expr_T{T: &pb.TargetType{}}},
			}}}}
		}, map[string]bool{"g": true, "h": true}},
		{"source type changed", func(_, target *TableDef, _ *AlterTableContext) { target.Cols[0].Typ.Scale++ }, map[string]bool{}},
		{"column reorder", func(_, target *TableDef, _ *AlterTableContext) {
			target.Cols[0], target.Cols[2] = target.Cols[2], target.Cols[0]
		}, map[string]bool{"g": true, "h": true}},
		{"source replaced", func(_, target *TableDef, ctx *AlterTableContext) {
			target.Cols[0].ColId = 99
			delete(ctx.changColDefMap, 1)
		}, map[string]bool{}},
		{"expression changed", func(_, target *TableDef, _ *AlterTableContext) { target.Cols[1].GeneratedCol.OriginString = "a+1" }, map[string]bool{}},
		{"unchanged virtual dependency", func(old, target *TableDef, _ *AlterTableContext) {
			old.Cols[1].GeneratedCol.IsStored = false
			target.Cols[1].GeneratedCol.IsStored = false
		}, map[string]bool{"g": true, "h": true}},
		{"invalid source position", func(old, _ *TableDef, _ *AlterTableContext) { old.Cols[1].GeneratedCol.Expr.GetCol().ColPos = -1 }, map[string]bool{}},
		{"cycle", func(old, _ *TableDef, _ *AlterTableContext) { old.Cols[1].GeneratedCol.Expr.GetCol().ColPos = 2 }, map[string]bool{}},
		{"generated rename", func(_, target *TableDef, ctx *AlterTableContext) {
			target.Cols[1].Name = "renamed"
			ctx.renameColumnSource("g", "renamed")
			ctx.changColDefMap[2].Name = "renamed"
		}, map[string]bool{"renamed": true, "h": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := pb.Type{Id: int32(types.T_int32), Width: 32}
			ref := func(pos int32) *pb.GeneratedCol {
				return &pb.GeneratedCol{IsStored: true, OriginString: "expression", Expr: &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}}
			}
			old := &TableDef{Name: "t", Cols: []*ColDef{
				{Name: "a", ColId: 1, Seqnum: 1, Typ: typ},
				{Name: "g", ColId: 2, Seqnum: 2, Typ: typ, GeneratedCol: ref(0)},
				{Name: "h", ColId: 3, Seqnum: 3, Typ: typ, GeneratedCol: ref(1)},
			}}
			target := DeepCopyTableDef(old, true)
			target.Name = "t_copy"
			ctx := initAlterTableContext(old, target, "db")
			if tc.change != nil {
				tc.change(old, target, ctx)
			}
			require.Equal(t, tc.want, alterCopyPreservedGeneratedColumns(old, target, ctx))
		})
	}
}

func TestCopyGeneratedColumnAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, target string
		purpose      executor.InternalInsertPurpose
		stored, want bool
	}{
		{"ordinary insert", "t_copy", 0, true, false},
		{"ordinary virtual insert", "t_copy", 0, false, false},
		{"index backfill target", "t_copy", executor.InternalInsertIndexBackfill, true, false},
		{"other copy target", "other", executor.InternalInsertCopy, true, false},
		{"copy stored target", "t_copy", executor.InternalInsertCopy, true, true},
		{"copy virtual target", "t_copy", executor.InternalInsertCopy, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(true)
			ctx := context.Background()
			if tc.purpose != 0 {
				ctx = context.WithValue(ctx, defines.InternalInsertOptionsKey{}, &executor.InternalInsertOptions{
					Purpose: tc.purpose, Dedup: &pb.AlterCopyOpt{TargetTableName: tc.target},
				})
			}
			mock.SetContext(ctx)
			builder := NewQueryBuilder(pb.Query_INSERT, mock, false, true)
			col := &ColDef{Name: "g", GeneratedCol: &pb.GeneratedCol{IsStored: tc.stored}}
			table := &TableDef{Name: "t_copy", Cols: []*ColDef{col}}
			cols, err := builder.getInsertColsFromStmt(tree.IdentifierList{"g"}, table)
			if tc.want {
				require.NoError(t, err)
				require.Equal(t, []string{"g"}, cols)
			} else {
				require.Error(t, err)
			}
		})
	}
}
