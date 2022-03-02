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

package compile

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/ftree"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/rewrite"
	"github.com/matrixorigin/matrixone/pkg/sql/vtree"
)

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	// do ast rewrite work
	e.stmt = rewrite.Rewrite(e.stmt)
	e.stmt = rewrite.AstRewrite(e.stmt)

	// do semantic analysis and build plan for sql
	pn, err := plan.New(e.c.db, e.c.sql, e.c.e).BuildStatement(e.stmt)
	if err != nil {
		return err
	}

	{
		attrs := pn.ResultColumns()
		cols := make([]*Col, len(attrs))
		for i, attr := range attrs {
			cols[i] = &Col{
				Name: attr.Name,
				Typ:  attr.Type.Oid,
			}
		}
		e.resultCols = cols
	}
	e.u = u
	e.e = e.c.e
	e.fill = fill

	// build scope for a single sql
	s, err := e.compileScope(pn)
	if err != nil {
		return err
	}
	e.scope = s
	return nil
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (e *Exec) Run(ts uint64) error {
	if e.scope == nil {
		return nil
	}

	switch e.scope.Magic {
	case Normal:
		return e.scope.Run(e.c.e)
	case Merge:
		return e.scope.MergeRun(e.c.e)
	case Remote:
		return e.scope.RemoteRun(e.c.e)
	case Parallel:
		return e.scope.ParallelRun(e.c.e)
	case Insert:
		affectedRows, err := e.scope.Insert(ts)
		if err != nil {
			return err
		}
		e.setAffectedRows(affectedRows)
		return nil
	case CreateDatabase:
		return e.scope.CreateDatabase(ts)
	case CreateTable:
		return e.scope.CreateTable(ts)
	case CreateIndex:
		return e.scope.CreateIndex(ts)
	case DropDatabase:
		return e.scope.DropDatabase(ts)
	case DropTable:
		return e.scope.DropTable(ts)
	case DropIndex:
		return e.scope.DropIndex(ts)
	case ShowDatabases:
		return e.scope.ShowDatabases(e.u, e.fill)
	case ShowTables:
		return e.scope.ShowTables(e.u, e.fill)
	case ShowColumns:
		return e.scope.ShowColumns(e.u, e.fill)
	case ShowCreateTable:
		return e.scope.ShowCreateTable(e.u, e.fill)
	case ShowCreateDatabase:
		return e.scope.ShowCreateDatabase(e.u, e.fill)
	}
	return nil
}

func (e *Exec) compileScope(pn plan.Plan) (*Scope, error) {
	switch qry := pn.(type) {
	case *plan.Query:
		ft, err := ftree.New().Build(qry)
		if err != nil {
			return nil, err
		}
		return e.compileVTree(vtree.New().Build(ft), qry.VarsMap)
	case *plan.Insert:
		// todo: insert into tbl select a, b from tbl2 should deal next time.
		return &Scope{
			Magic: Insert,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.CreateDatabase:
		return &Scope{
			Magic: CreateDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.CreateTable:
		return &Scope{
			Magic: CreateTable,
			Plan:  pn,
			Proc:  e.c.proc,
			// todo: create table t1 as select a, b, c from t2 ?
			DataSource: nil,
			PreScopes:  nil,
		}, nil
	case *plan.CreateIndex:
		return &Scope{
			Magic: CreateIndex,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropDatabase:
		return &Scope{
			Magic: DropDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropTable:
		return &Scope{
			Magic: DropTable,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropIndex:
		return &Scope{
			Magic: DropIndex,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowDatabases:
		return &Scope{
			Magic: ShowDatabases,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowTables:
		return &Scope{
			Magic: ShowTables,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowColumns:
		return &Scope{
			Magic: ShowColumns,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowCreateTable:
		return &Scope{
			Magic: ShowCreateTable,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowCreateDatabase:
		return &Scope{
			Magic: ShowCreateDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", pn))
}

func (e *Exec) Statement() tree.Statement {
	return e.stmt
}

func (e *Exec) SetSchema(db string) error {
	e.c.db = db
	return nil
}

func (e *Exec) Columns() []*Col {
	return e.resultCols
}

func (e *Exec) increaseAffectedRows(n uint64) {
	e.affectRows += n
}

func (e *Exec) setAffectedRows(n uint64) {
	e.affectRows = n
}

func (e *Exec) GetAffectedRows() uint64 {
	return e.affectRows
}
