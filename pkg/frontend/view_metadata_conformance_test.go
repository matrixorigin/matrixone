//go:build integration

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

package frontend_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// 在已有真实目录 fixture 中串行提交 DDL，而不是每个消费者启动一个集群。
// 所有比较均显式接入 S3/S4；默认协议路径不能替代这些证据。
func testNativeViewMetadataDDLTransitions(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string) {
	exec := func(t *testing.T, sql string) {
		t.Helper()
		result, err := svc.GetSQLExecutor().Exec(ctx, sql, executor.Options{}.WithDatabase(database))
		require.NoError(t, err, sql)
		result.Close()
	}
	exec(t, "create table conformance_source (id int not null default 7, label varchar(20) default 'original')")
	defer exec(t, "drop table if exists conformance_source")
	exec(t, "create view conformance_middle as select * from conformance_source")
	defer exec(t, "drop view conformance_middle")
	exec(t, "create view conformance_root as select * from conformance_middle")
	defer exec(t, "drop view conformance_root")

	var sourceID, rootID uint64
	var historical *plan.Snapshot
	withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, _ *process.Process) {
		ts := op.SnapshotTS()
		historical = &plan.Snapshot{TS: &ts}
		_, source, err := compiler.Resolve(database, "conformance_source", nil)
		require.NoError(t, err)
		sourceID = source.TblId
		_, root, err := compiler.Resolve(database, "conformance_root", nil)
		require.NoError(t, err)
		rootID = root.TblId
	})
	// snapshot/restore 属于 frontend 语句，不能交给内部 SQLExecutor 的 planner。
	public := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", database)
	defer public.Close()
	publicDDL := func(t *testing.T, sql string) {
		t.Helper()
		_, err := public.ExecContext(ctx, sql)
		require.NoError(t, err, sql)
	}
	publicDDL(t, "create snapshot view_metadata_conformance for account")
	defer publicDDL(t, "drop snapshot view_metadata_conformance")

	// 先固定读者的 statement，再在独立事务提交 metadata-only DDL。
	// 旧读者仍合法，不应因为外部提交就被迫使用新 default 或返回 Changed。
	if !t.Run("committed DDL does not advance an established statement", func(t *testing.T) {
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			request := compiler.NewViewSchemaRequest(ctx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			defer request.Close()
			before := describeNativeViewSchema(t, request, database, "conformance_root", nil)
			require.Equal(t, "'original'", before.columns[1].Default.OriginString)
			exec(t, "alter table conformance_source modify column label varchar(20) default 'metadata', algorithm=inplace")
			after := describeNativeViewSchema(t, request, database, "conformance_root", nil)
			require.Equal(t, before, after, "同一 statement 的合法旧快照不能拼入 DDL 后的字段")
			request.Close()
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			require.Zero(t, generation.Used())
		})
	}) {
		return
	}

	for _, tc := range []struct {
		name, ddl, defaultValue string
		width                   int32
		replaced                bool
	}{
		{"metadata only", "", "metadata", 20, false},
		{"COPY replacement and frozen star", "alter table conformance_source algorithm=copy, modify column label varchar(40) default 'copied', add column extra int", "copied", 40, true},
		{"same name recreation", "create table conformance_source (id int not null default 7, label varchar(60) default 'recreated')", "recreated", 60, true},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			if tc.name == "same name recreation" {
				exec(t, "drop table conformance_source")
			}
			if tc.ddl != "" {
				exec(t, tc.ddl)
			}
			withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
				_, source, err := compiler.Resolve(database, "conformance_source", nil)
				require.NoError(t, err)
				if tc.replaced {
					require.NotEqual(t, sourceID, source.TblId, "必须真正经过物理对象替换")
				} else {
					require.Equal(t, sourceID, source.TblId, "metadata-only 对照不能变成 COPY")
				}
				sourceID = source.TblId
				_, root, err := compiler.Resolve(database, "conformance_root", nil)
				require.NoError(t, err)
				require.Equal(t, rootID, root.TblId, "源 DDL 不能靠重建根 View 使测试通过")
				testNativeViewMetadataSameSnapshot(t, ctx, compiler, op, proc, database, tc.width, tc.defaultValue)
			})
		}) {
			return // 后续状态依赖本次 DDL 的成功提交，不在失败的 fixture 上继续。
		}
	}

	t.Run("historical source is not the same-name replacement", func(t *testing.T) {
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			compiler.SetSnapshot(historical)
			defer compiler.SetSnapshot(nil)
			testNativeViewMetadataSameSnapshot(t, ctx, compiler, op, proc, database, 20, "original")
		})
	})

	t.Run("restore replaces source without recreating the view", func(t *testing.T) {
		publicDDL(t, "restore table "+database+".conformance_source {snapshot='view_metadata_conformance'}")
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			_, source, err := compiler.Resolve(database, "conformance_source", nil)
			require.NoError(t, err)
			require.NotEqual(t, sourceID, source.TblId)
			_, root, err := compiler.Resolve(database, "conformance_root", nil)
			require.NoError(t, err)
			require.Equal(t, rootID, root.TblId)
			testNativeViewMetadataSameSnapshot(t, ctx, compiler, op, proc, database, 20, "original")
		})
	})
}

func testNativeViewMetadataSameSnapshot(t *testing.T, ctx context.Context, compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process, database string, width int32, defaultValue string) {
	t.Helper()
	beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
	beforeWrites := op.GetWorkspace().WriteOffset()
	authorize := frontend.NativeViewSchemaRoleAuthorizerForTest(compiler)
	// S3 SELECT/CTAS 共用相同的父事务及显式历史域，但各自拥有请求状态。
	for _, sql := range []string{
		"select * from conformance_root limit 0",
		"create table conformance_plan_only as select * from conformance_root",
	} {
		func() {
			statements, err := mysql.Parse(ctx, sql, 1)
			require.NoError(t, err)
			defer statements[0].Free()
			p, err := compiler.BuildPlanWithViewSchema(ctx, statements[0], false, authorize)
			require.NoError(t, err, sql)
			columns := plan.GetResultColumnsFromPlan(p)
			if p.GetDdl() != nil {
				columns = nil
				for _, column := range p.GetDdl().GetCreateTable().TableDef.Cols {
					if !column.Hidden {
						columns = append(columns, column)
					}
				}
			}
			require.Len(t, columns, 2, "冻结的 star 不应加入 COPY 新增的 extra 列")
			require.Equal(t, "label", columns[1].Name)
			require.EqualValues(t, types.T_varchar, columns[1].Typ.Id)
			require.Equal(t, width, columns[1].Typ.Width)
			if p.GetDdl() != nil {
				require.Equal(t, "0", columns[0].Default.OriginString, "CTAS 不复制 View 的 literal default 7")
			}
		}()
	}

	bound := 0
	request := compiler.NewViewMetadataRequest(ctx, func(ctx context.Context, db, name string, snapshot *plan.Snapshot) error {
		bound++
		return authorize(ctx, db, name, snapshot)
	})
	defer request.Close()
	for _, sql := range []string{"describe conformance_root", "show columns from conformance_root", "show full columns from conformance_root"} {
		rows, err := frontend.ReadNativeViewMetadataSQLForTest(request.Context(), compiler, sql)
		require.NoError(t, err, sql)
		require.Len(t, rows, 2)
		require.Equal(t, "label", rows[1][0])
		require.Equal(t, fmt.Sprintf("varchar(%d)", width), rows[1][1])
	}
	query := fmt.Sprintf("select column_name, lower(column_type), column_default from information_schema.columns where table_schema='%s' and table_name='conformance_root' order by ordinal_position", database)
	rows, err := frontend.ReadNativeViewMetadataSQLForTest(request.Context(), compiler, query)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"id", "int", "7"}, {"label", fmt.Sprintf("varchar(%d)", width), "'" + defaultValue + "'"}}, rows)
	require.Positive(t, bound, "必须实际调用 opt-in provider，不能只测已有默认路径")
	request.Close()

	afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
	require.Equal(t, beforeStamp, afterStamp)
	require.Equal(t, beforeRevision, afterRevision)
	require.Equal(t, beforeWrites, op.GetWorkspace().WriteOffset(), "描述及 CTAS 规划不能写目录/刷新状态")
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Zero(t, generation.Used())
	require.False(t, generation.Closed(), "父执行代仍归调用者所有")
}
