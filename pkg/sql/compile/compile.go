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
	"matrixone/pkg/container/types"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/build"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/createDatabase"
	"matrixone/pkg/sql/op/createTable"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/dropDatabase"
	"matrixone/pkg/sql/op/dropTable"
	"matrixone/pkg/sql/op/explain"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/insert"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/sql/op/showDatabases"
	"matrixone/pkg/sql/op/showTables"
	"matrixone/pkg/sql/op/summarize"
	"matrixone/pkg/sql/op/top"
	"matrixone/pkg/sql/opt"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
	"sync"
)

func New(db string, sql string, uid string,
	e engine.Engine, proc *process.Process) *compile {
	return &compile{
		e:    e,
		db:   db,
		uid:  uid,
		sql:  sql,
		proc: proc,
	}
}

func (c *compile) Compile() ([]*Exec, error) {
	stmts, err := tree.NewParser().Parse(c.sql)
	if err != nil {
		return nil, err
	}
	es := make([]*Exec, len(stmts))
	for i, stmt := range stmts {
		es[i] = &Exec{
			c:    c,
			stmt: stmt,
		}
	}
	return es, nil
}

func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, err := build.New(e.c.db, e.c.sql, e.c.e, e.c.proc).BuildStatement(e.stmt)
	if err != nil {
		return err
	}
	o = prune(o)
	o = opt.Optimize(rewrite(o, mergeCount(o, 0)))
	ss, err := e.c.compileAlgebar(o)
	if err != nil {
		return err
	}
	{
		switch o.(type) {
		case *explain.Explain:
			e.cs = append(e.cs, &Col{Typ: types.T_varchar, Name: "Pipeline"})
		case *showTables.ShowTables:
			e.cs = append(e.cs, &Col{Typ: types.T_varchar, Name: "Table"})
		case *showDatabases.ShowDatabases:
			e.cs = append(e.cs, &Col{Typ: types.T_varchar, Name: "Database"})
		}
	}
	mp := o.Attribute()
	attrs := o.Columns()
	cs := make([]*Col, 0, len(mp))
	for _, attr := range attrs {
		cs = append(cs, &Col{mp[attr].Oid, attr})
	}
	{
		switch o.(type) {
		case *explain.Explain:
			cs = append(cs, &Col{Typ: types.T_varchar, Name: "Pipeline"})
		case *showTables.ShowTables:
			cs = append(cs, &Col{Typ: types.T_varchar, Name: "Table"})
		case *showDatabases.ShowDatabases:
			cs = append(cs, &Col{Typ: types.T_varchar, Name: "Database"})
		}
	}
	e.u = u
	e.cs = cs
	e.e = e.c.e
	e.fill = fill
	e.ss = fillOutput(ss, &output.Argument{Data: u, Func: fill, Attrs: attrs}, e.c.proc)
	return nil
}

func (e *Exec) Statement() tree.Statement {
	return e.stmt
}

func (e *Exec) SetSchema(db string) error {
	e.c.db = db
	return nil
}

func (e *Exec) Columns() []*Col {
	return e.cs
}

func (e *Exec) Run(ts uint64) error {
	var wg sync.WaitGroup

	fmt.Printf("+++++++++\n")
	Print(nil, e.ss)
	fmt.Printf("+++++++++\n")
	for i := range e.ss {
		switch e.ss[i].Magic {
		case Normal:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Run(e.e); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case Merge:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.MergeRun(e.e); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case Insert:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Insert(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case Explain:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Explain(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case DropTable:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.DropTable(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case DropDatabase:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.DropDatabase(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case CreateTable:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.CreateTable(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case CreateDatabase:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.CreateDatabase(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case ShowTables:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.ShowTables(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case ShowDatabases:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.ShowDatabases(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		}
	}
	wg.Wait()
	return e.err
}

func (c *compile) compileAlgebar(o op.OP) ([]*Scope, error) {
	switch n := o.(type) {
	case *insert.Insert:
		return []*Scope{&Scope{Magic: Insert, O: o}}, nil
	case *explain.Explain:
		return []*Scope{&Scope{Magic: Explain, O: o}}, nil
	case *dropTable.DropTable:
		return []*Scope{&Scope{Magic: DropTable, O: o}}, nil
	case *dropDatabase.DropDatabase:
		return []*Scope{&Scope{Magic: DropDatabase, O: o}}, nil
	case *createTable.CreateTable:
		return []*Scope{&Scope{Magic: CreateTable, O: o}}, nil
	case *createDatabase.CreateDatabase:
		return []*Scope{&Scope{Magic: CreateDatabase, O: o}}, nil
	case *showTables.ShowTables:
		return []*Scope{&Scope{Magic: ShowTables, O: o}}, nil
	case *showDatabases.ShowDatabases:
		return []*Scope{&Scope{Magic: ShowDatabases, O: o}}, nil
	case *projection.Projection:
		return c.compileOutput(n, make(map[string]uint64))
	case *top.Top:
		return c.compileTopOutput(n, make(map[string]uint64))
	case *order.Order:
		return c.compileOrderOutput(n, make(map[string]uint64))
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%s' unsupprt now", o))

}

func (c *compile) compile(o op.OP, mp map[string]uint64) ([]*Scope, error) {
	switch n := o.(type) {
	case *top.Top:
		return c.compileTop(n, mp)
	case *dedup.Dedup:
		return c.compileDedup(n, mp)
	case *group.Group:
		return c.compileGroup(n, mp)
	case *limit.Limit:
		return c.compileLimit(n, mp)
	case *order.Order:
		return c.compileOrder(n, mp)
	case *offset.Offset:
		return c.compileOffset(n, mp)
	case *product.Product:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%s' unsupprt now", o))
	case *innerJoin.Join:
		return c.compileInnerJoin(n, mp)
	case *naturalJoin.Join:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%s' unsupprt now", o))
	case *relation.Relation:
		return c.compileRelation(n, mp)
	case *restrict.Restrict:
		return c.compileRestrict(n, mp)
	case *summarize.Summarize:
		return c.compileSummarize(n, mp)
	case *projection.Projection:
		return c.compileProjection(n, mp)
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%s' unsupprt now", o))
}
