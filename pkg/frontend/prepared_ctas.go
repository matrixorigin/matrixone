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

package frontend

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// 从对应的未脱敏语句按词法边界提取源，前置注释和名字里的 FROM 不能影响位置。
func directPreparedCTASSourceSQL(ctx context.Context, sql string) (string, error) {
	scanner := mysql.NewScanner(dialect.MYSQL, sql)
	defer mysql.PutScanner(scanner)
	for _, expected := range []int{mysql.PREPARE, 0, mysql.FROM} {
		token, _ := scanner.Scan()
		if expected != 0 && token != expected {
			return "", moerr.NewParseError(ctx, "invalid CTAS PREPARE wrapper")
		}
	}
	source := strings.TrimSpace(sql[scanner.Pos:])
	if source == "" {
		return "", moerr.NewParseError(ctx, "missing CTAS PREPARE source")
	}
	return source, nil
}

func bindPreparedCTASTargetDatabase(stmt *tree.CreateTable, database string) {
	if stmt.Table.SchemaName == "" && database != "" {
		stmt.Table.SchemaName = tree.Identifier(database)
		stmt.Table.ExplicitSchema = true
	}
}

// 在未注入角色规则的用户原文上附加当前角色，既保留显式 hint，也不能
// 把 prepare 时自动注入的旧角色 hint 当作用户输入跨角色复用。
func applyPreparedCTASRewrite(ctx context.Context, ses *Session, stmt *tree.CreateTable, sql string) error {
	stmt.AsSource.RewriteOption = nil
	if !ses.rewriteEnabled.Load() {
		return nil
	}
	rules, err := rewriteSQL(ctx, ses, sql)
	if err != nil {
		return err
	}
	return parsers.AddRewriteHints(ctx, []tree.Statement{stmt}, rules)
}

// CTAS 没有可复用的预编译执行器；每次 EXECUTE 按当前角色重新推导列和规划，
// 不允许 prepare 时保存的宽策略或旧投影穿过角色切换边界。
// 成功后才发布新 AST/plan，失败保留原 prepared handle 供纠正权限后重试。
func refreshPreparedCTAS(ctx context.Context, ses *Session, prepared *PrepareStmt) error {
	ctas, ok := prepared.PrepareStmt.(*tree.CreateTable)
	if !ok || ctas.AsSource == nil {
		return nil
	}
	v, err := ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		return err
	}
	stmt, err := mysql.ParseOne(ctx, prepared.Sql, v.(int64))
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			stmt.Free()
		}
	}()
	fresh, ok := stmt.(*tree.CreateTable)
	if !ok || fresh.AsSource == nil {
		return moerr.NewInvalidInput(ctx, "prepared CTAS source changed")
	}
	if err = applyPreparedCTASRewrite(ctx, ses, fresh, prepared.Sql); err != nil {
		return err
	}
	bindPreparedCTASTargetDatabase(fresh, prepared.ctasDatabase)
	compileCtx := ses.GetTxnCompileCtx()
	oldDatabase := compileCtx.DefaultDatabase()
	compileCtx.SetDatabase(prepared.ctasDatabase)
	defer compileCtx.SetDatabase(oldDatabase)
	wrapper := &tree.PrepareStmt{Name: tree.Identifier(prepared.Name), Stmt: fresh}
	p, err := buildPlan(ctx, ses, compileCtx, wrapper)
	if err != nil {
		return err
	}
	old := prepared.PrepareStmt
	prepared.PrepareStmt, prepared.PreparePlan = fresh, p
	prepared.Ts = timestamp.Timestamp{PhysicalTime: time.Now().Unix()}
	published = true
	old.Free()
	return nil
}
