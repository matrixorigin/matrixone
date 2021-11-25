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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/ftree"
	"matrixone/pkg/sql/parsers/tree"
	"matrixone/pkg/sql/plan"
	"matrixone/pkg/sql/vtree"
)

// Compile compiles ast tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
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
	s, err := e.compileScope(pn)
	if err != nil {
		return err
	}
	e.scope = s
	return nil
}

func (e *Exec) compileScope(pn plan.Plan) (*Scope, error) {
	switch qry := pn.(type) {
	case *plan.Query:
		ft, err := ftree.New().Build(qry)
		if err != nil {
			return nil, err
		}
		return e.compileVTree(qry.FreeAttrs, vtree.New().Build(ft), qry.VarsMap)
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

func (e *Exec) IncreaseAffectedRows(n uint64) {
	e.affectRows += n
}

func (e *Exec) SetAffectedRows(n uint64) {
	e.affectRows = n
}

func (e *Exec) GetAffectedRows() uint64 {
	return e.affectRows
}

func (e *Exec) Run(ts uint64) error {
	fmt.Printf("+++++++++\n")
	Print(nil, []*Scope{e.scope})
	fmt.Printf("+++++++++\n")
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
		e.SetAffectedRows(affectedRows)
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
	}
	return nil
}
