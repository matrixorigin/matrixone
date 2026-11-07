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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCRC32LegacyGeneratedAttributeValidation(t *testing.T) {
	for _, verb := range []string{"modify column g", "change column g g"} {
		for _, attribute := range []string{"default 0", "on update current_timestamp", "auto_increment"} {
			for _, first := range []bool{false, true} {
				generated := "generated always as (crc32(val)) stored"
				clauses := generated + " " + attribute
				if first {
					clauses = attribute + " " + generated
				}
				sql := "alter table constraint_test.t_on_update_gen " + verb + " bigint unsigned " + clauses
				t.Run(sql, func(t *testing.T) {
					mock := NewMockOptimizer(true, newPlanTestProcess(t))
					base := mock.ctxt.tables["t_on_update_gen"]
					base.Indexes = nil
					pos := mockTableColPos(t, base, "val")
					base.Cols[pos].Typ = planpb.Type{Id: int32(types.T_json)}
					base.Cols[pos].Default = &planpb.Default{NullAbility: true}
					generated := FindColumn(base.Cols, "g")
					shape, err := mysql.ParseOne(context.Background(), "create table shape(g bigint unsigned)", 1)
					require.NoError(t, err)
					defer shape.Free()
					generated.Typ, err = getTypeFromAst(context.Background(), shape.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type)
					require.NoError(t, err)
					expr := legacyCRC32Expr()
					expr.GetF().Args[0].GetCol().ColPos = pos
					expr.GetF().Args[0].GetCol().Name = "val"
					generated.GeneratedCol = &planpb.GeneratedCol{Expr: expr, OriginString: "crc32(val)", IsStored: true}
					FindColumn(base.Cols, "updated_at").OnUpdate = nil
					original := proto.Clone(base).(*planpb.TableDef)
					built, err := runOneStmt(mock, t, sql)
					require.Error(t, err, "legacy preservation must not silently discard illegal attributes")
					// Attribute-first ON UPDATE may fail its existing type contract
					// before the generated attribute is visited. The other orders
					// must reach the generated-column exclusion itself.
					if !first || attribute != "on update current_timestamp" {
						require.ErrorContains(t, err, "generated column 'g' cannot have")
					}
					require.Nil(t, built)
					require.True(t, proto.Equal(original, base), "rejected ALTER must leave the authoritative catalog unchanged")
				})
			}
		}
	}
}

func TestCRC32CopyGeneratedAttributeValidation(t *testing.T) {
	for _, attribute := range []string{"default 0", "on update current_timestamp", "auto_increment"} {
		t.Run(attribute, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			sql := "create table t(j json, g bigint unsigned generated always as (crc32(j)) stored " + attribute + ")"
			stmt, err := mysql.ParseOne(proc.Ctx, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			col := stmt.(*tree.CreateTable).Defs[1].(*tree.ColumnTableDef)
			typ, err := getTypeFromAst(proc.Ctx, col.Type)
			require.NoError(t, err)
			source := &planpb.ColDef{Name: "g", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: legacyCRC32Expr(), OriginString: "crc32(j)", IsStored: true}}
			before := proto.Clone(source).(*planpb.ColDef)
			ctx := context.WithValue(proc.Ctx, defines.CRC32CopyExpressionsKey{}, &planpb.TableDef{Cols: []*planpb.ColDef{source}})
			oldCtx := proc.Ctx
			proc.Ctx = ctx
			defer func() { proc.Ctx = oldCtx }()
			got, err := buildGeneratedExpr(ctx, col, typ, []*ColDef{{Name: "j", Typ: planpb.Type{Id: int32(types.T_json)}}}, proc)
			require.ErrorContains(t, err, "generated column 'g' cannot have")
			require.Nil(t, got)
			require.True(t, proto.Equal(before, source))
		})
	}
}
