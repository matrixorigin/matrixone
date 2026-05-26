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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskFormat(t *testing.T) {
	tests := []struct {
		name string
		stmt NodeFormatter
		want string
	}{
		{
			name: "create full",
			stmt: &CreateSQLTask{
				IfNotExists:   true,
				Name:          Identifier("task_a"),
				CronExpr:      "0 0 * * * *",
				Timezone:      "UTC",
				GateCondition: "select 1",
				RetryLimit:    2,
				Timeout:       "10s",
				SQLBody:       "select 1",
			},
			want: "create task if not exists task_a schedule '0 0 * * * *' timezone 'UTC' when (select 1) retry 2 timeout '10s' as begin select 1; end",
		},
		{
			name: "create empty body",
			stmt: &CreateSQLTask{Name: Identifier("task_empty")},
			want: "create task task_empty as begin end",
		},
		{
			name: "create body keeps semicolon",
			stmt: &CreateSQLTask{Name: Identifier("task_body"), SQLBody: "select 1;"},
			want: "create task task_body as begin select 1; end",
		},
		{
			name: "alter suspend",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskSuspend},
			want: "alter task task_a suspend",
		},
		{
			name: "alter resume",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskResume},
			want: "alter task task_a resume",
		},
		{
			name: "alter schedule",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskSetSchedule, CronExpr: "0 * * * * *", Timezone: "Asia/Shanghai"},
			want: "alter task task_a set schedule '0 * * * * *' timezone 'Asia/Shanghai'",
		},
		{
			name: "alter when",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskSetWhen, GateCondition: "select 1"},
			want: "alter task task_a set when (select 1)",
		},
		{
			name: "alter retry",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskSetRetry, RetryLimit: 3},
			want: "alter task task_a set retry 3",
		},
		{
			name: "alter timeout",
			stmt: &AlterSQLTask{Name: Identifier("task_a"), Action: AlterTaskSetTimeout, Timeout: "1m"},
			want: "alter task task_a set timeout '1m'",
		},
		{
			name: "drop if exists",
			stmt: &DropSQLTask{IfExists: true, Name: Identifier("task_a")},
			want: "drop task if exists task_a",
		},
		{
			name: "execute",
			stmt: &ExecuteSQLTask{Name: Identifier("task_a")},
			want: "execute task task_a",
		},
		{
			name: "show tasks",
			stmt: &ShowSQLTasks{},
			want: "show tasks",
		},
		{
			name: "show task runs",
			stmt: &ShowSQLTaskRuns{TaskName: Identifier("task_a"), HasTask: true, Limit: 5, HasLimit: true},
			want: "show task runs for task_a limit 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, String(tt.stmt, dialect.MYSQL))
		})
	}
}

func TestSQLTaskStatementTypes(t *testing.T) {
	require.Equal(t, "Create Task", (&CreateSQLTask{}).GetStatementType())
	require.Equal(t, "Alter Task", (&AlterSQLTask{}).GetStatementType())
	require.Equal(t, "Drop Task", (&DropSQLTask{}).GetStatementType())
	require.Equal(t, "Execute Task", (&ExecuteSQLTask{}).GetStatementType())
	require.Equal(t, "Show Tasks", (&ShowSQLTasks{}).GetStatementType())
	require.Equal(t, "Show Task Runs", (&ShowSQLTaskRuns{}).GetStatementType())
}
