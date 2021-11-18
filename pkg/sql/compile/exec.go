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
	"matrixone/pkg/sql/rewrite"
	"matrixone/pkg/sql/vtree"
	"sync"
)

// Compile compiles ast tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	e.stmt = rewrite.AstRewrite(e.stmt)
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
		return e.compileVTree(vtree.New().Build(ft), qry.VarsMap)
	case *plan.CreateDatabase:
		return &Scope{
			Magic:        CreateDatabase,
			Plan:         pn,
			Proc:         e.c.proc,
		}, nil
	case *plan.CreateTable:
		return &Scope{
			Magic:        CreateTable,
			Plan:         pn,
			Proc:         e.c.proc,
			// todo: create table t1 as select a, b, c from t2 ?
			DataSource:   nil,
			PreScopes: 	  nil,
		}, nil
	case *plan.CreateIndex:
		return &Scope{
			Magic:        CreateIndex,
			Plan:         pn,
			Proc:         e.c.proc,
		}, nil
	case *plan.DropDatabase:
		return &Scope{
			Magic:        DropDatabase,
			Plan:         pn,
			Proc:         e.c.proc,
		}, nil
	case *plan.DropTable:
		return &Scope{
			Magic:        DropTable,
			Plan:         pn,
			Proc:         e.c.proc,
		}, nil
	case *plan.DropIndex:
		return &Scope{
			Magic:		DropIndex,
			Plan:		pn,
			Proc:		e.c.proc,
		}, nil
	case *plan.ShowDatabases:
		return &Scope{
			Magic:		ShowDatabases,
			Plan:		pn,
			Proc:		e.c.proc,
		}, nil
	case *plan.ShowTables:
		return &Scope{
			Magic:		ShowTables,
			Plan:		pn,
			Proc:		e.c.proc,
		}, nil
	case *plan.ShowColumns:
		return &Scope{
			Magic: 		ShowColumns,
			Plan: 		pn,
			Proc:		e.c.proc,
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
	var wg sync.WaitGroup

	fmt.Printf("+++++++++\n")
	Print(nil, []*Scope{e.scope})
	fmt.Printf("+++++++++\n")
	switch e.scope.Magic {
	case CreateDatabase:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.CreateDatabase(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case CreateTable:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.CreateTable(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case CreateIndex:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.CreateIndex(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case DropDatabase:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.DropDatabase(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case DropTable:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.DropTable(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case DropIndex:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.DropIndex(ts); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case ShowDatabases:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.ShowDatabases(e.u, e.fill); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case ShowTables:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.ShowTables(e.u, e.fill); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	case ShowColumns:
		wg.Add(1)
		go func(s *Scope) {
			if err := s.ShowColumns(e.u, e.fill); err != nil {
				e.err = err
			}
			wg.Done()
		}(e.scope)
	}
	wg.Wait()
	return e.err
}
