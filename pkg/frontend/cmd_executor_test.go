// Copyright 2021 Matrix Origin
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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUserInput_getSqlSourceType(t *testing.T) {
	type fields struct {
		sql           string
		stmt          tree.Statement
		sqlSourceType []string
	}
	type args struct {
		i int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "t1",
			fields: fields{
				sql:           "select * from t1",
				sqlSourceType: nil,
			},
			args: args{
				i: 0,
			},
			want: "external_sql",
		},
		{
			name: "t2",
			fields: fields{
				sql:           "select * from t1",
				sqlSourceType: nil,
			},
			args: args{
				i: 1,
			},
			want: "external_sql",
		},
		{
			name: "t3",
			fields: fields{
				sql: "select * from t1",
				sqlSourceType: []string{
					"a",
					"b",
					"c",
				},
			},
			args: args{
				i: 2,
			},
			want: "c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ui := &UserInput{
				sql:           tt.fields.sql,
				stmt:          tt.fields.stmt,
				sqlSourceType: tt.fields.sqlSourceType,
			}
			assert.Equalf(t, tt.want, ui.getSqlSourceType(tt.args.i), "getSqlSourceType(%v)", tt.args.i)
		})
	}
}
