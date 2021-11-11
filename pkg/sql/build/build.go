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

package build

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/rewrite"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(db string, sql string, e engine.Engine, proc *process.Process) *build {
	return &build{
		e:    e,
		db:   db,
		sql:  sql,
		proc: proc,
	}
}

func (b *build) Build() ([]op.OP, error) {
	// stmts, err := tree.NewParser().Parse(b.sql)
	stmts, err := parsers.Parse(dialect.MYSQL, b.sql)
	if err != nil {
		return nil, err
	}
	os := make([]op.OP, len(stmts))
	for i, stmt := range stmts {
		o, err := b.BuildStatement(rewrite.Rewrite(stmt))
		if err != nil {
			return nil, err
		}
		os[i] = o
	}
	return os, nil
}

func (b *build) BuildStatement(stmt tree.Statement) (op.OP, error) {
	stmt = rewrite.Rewrite(stmt)
	switch stmt := stmt.(type) {
	case *tree.Select:
		return b.buildSelect(stmt)
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select)
	case *tree.Insert:
		return b.buildInsert(stmt)
	case *tree.DropTable:
		return b.buildDropTable(stmt)
	case *tree.DropDatabase:
		return b.buildDropDatabase(stmt)
	case *tree.CreateTable:
		return b.buildCreateTable(stmt)
	case *tree.CreateDatabase:
		return b.buildCreateDatabase(stmt)
	case *tree.ExplainStmt, *tree.ExplainFor, *tree.ExplainAnalyze:
		return b.buildExplain(stmt)
	case *tree.ShowTables:
		return b.buildShowTables(stmt)
	case *tree.ShowDatabases:
		return b.buildShowDatabases(stmt)
	case *tree.CreateIndex:
		return b.buildCreateIndex(stmt)
	case *tree.DropIndex:
		return b.buildDropIndex(stmt)
	}
	return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", stmt))
}
