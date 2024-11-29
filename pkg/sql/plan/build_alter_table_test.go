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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
