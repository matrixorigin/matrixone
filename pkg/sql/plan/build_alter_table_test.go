// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestAlterTable1(t *testing.T) {
	//sql := "ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);"
	//sql := "ALTER TABLE t1 ADD d INT NOT NULL PRIMARY KEY;"
	sql := "ALTER TABLE t1 MODIFY b INT;"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestAlterTableAddColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	// CREATE TABLE t1 (a INTEGER, b CHAR(10));
	sqls := []string{
		`ALTER TABLE t1 ADD d TIMESTAMP;`,
		//`ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY;`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY PRIMARY KEY;`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY PRIMARY KEY PRIMARY KEY;`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func Test_checkChangeTypeCompatible(t *testing.T) {
	type args struct {
		ctx    context.Context
		origin *plan.Type
		to     *plan.Type
	}

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test1",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_binary)},
				to:     &plan.Type{Id: int32(types.T_json)},
			},
			wantErr: assert.Error,
		},
		{
			name: "test2",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_binary)},
				to:     &plan.Type{Id: int32(types.T_json)},
			},
			wantErr: assert.Error,
		},
		{
			name: "test3",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_enum)},
				to:     &plan.Type{Id: int32(types.T_varchar)},
			},
			wantErr: assert.NoError,
		},
		{
			name: "test4",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_varchar)},
				to:     &plan.Type{Id: int32(types.T_enum)},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, checkChangeTypeCompatible(tt.args.ctx, tt.args.origin, tt.args.to), fmt.Sprintf("checkChangeTypeCompatible(%v, %v, %v)", tt.args.ctx, tt.args.origin, tt.args.to))
		})
	}
}

func buildSingleStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	plan, err := BuildPlan(context, statements[0], false)
	if plan != nil {
		testDeepCopy(plan)
	}
	return plan, err
}

// TestAlterTableVarcharLengthBumped tests the isVarcharLengthBumped function.
func TestAlterTableVarcharLengthBumped(t *testing.T) {
	tests := []struct {
		name     string
		clause   *tree.AlterTableModifyColumnClause
		tableDef *TableDef
		wantOk   bool
		wantErr  bool
	}{
		{
			name: "varchar length increased",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},
		{
			name: "varchar length decreased",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  50,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "column not found",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col_not_exist"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: true,
		},
		{
			name: "different type",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "char",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "position changed",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
				Position: &tree.ColumnPosition{
					Typ:            tree.ColumnPositionAfter,
					RelativeColumn: tree.NewUnresolvedColName("col2"),
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
					{
						Name:  "col2",
						ColId: 2,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "with null attribute matching",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: false},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: false,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},
		{
			name: "with null attribute not matching",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: true},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: false,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},

		{
			name: "with more than one null attribute not matching",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: true},
						&tree.AttributeKey{},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: true,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ok, err := isVarcharLengthBumped(ctx, tt.clause, tt.tableDef)
			if (err != nil) != tt.wantErr {
				t.Errorf("isVarcharLengthBumped() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if ok != tt.wantOk {
				t.Errorf("isVarcharLengthBumped() = %v, want %v", ok, tt.wantOk)
			}
		})
	}
}
