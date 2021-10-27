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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/build"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createDatabase"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createTable"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropDatabase"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropTable"
	"github.com/matrixorigin/matrixone/pkg/sql/op/explain"
	"github.com/matrixorigin/matrixone/pkg/sql/op/group"
	"github.com/matrixorigin/matrixone/pkg/sql/op/innerJoin"
	"github.com/matrixorigin/matrixone/pkg/sql/op/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/op/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/op/naturalJoin"
	"github.com/matrixorigin/matrixone/pkg/sql/op/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/op/order"
	"github.com/matrixorigin/matrixone/pkg/sql/op/product"
	"github.com/matrixorigin/matrixone/pkg/sql/op/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/op/relation"
	"github.com/matrixorigin/matrixone/pkg/sql/op/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showDatabases"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showTables"
	"github.com/matrixorigin/matrixone/pkg/sql/op/summarize"
	"github.com/matrixorigin/matrixone/pkg/sql/op/top"
	"github.com/matrixorigin/matrixone/pkg/sql/opt"
	rw "github.com/matrixorigin/matrixone/pkg/sql/rewrite"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

// Build generates query execution list based on the result of sql parser.
func (c *compile) Build() ([]*Exec, error) {
	stmts, err := tree.NewParser().Parse(c.sql)
	if err != nil {
		return nil, err
	}
	es := make([]*Exec, len(stmts))
	for i, stmt := range stmts {
		es[i] = &Exec{
			c:    c,
			stmt: stmt,
			affectRows: 0,
		}
	}
	return es, nil
}

// Compile compiles ast tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	// AST rewrite
	e.stmt = rw.AstRewrite(e.stmt)
	// generates relation algebra operator chain.
	o, err := build.New(e.c.db, e.c.sql, e.c.e, e.c.proc).BuildStatement(e.stmt)
	if err != nil {
		return err
	}
	// remove useless operators.
	o = prune(o)
	// optimize is not implemented for now.
	o = opt.Optimize(rewrite(o, mergeCount(o, 0)))
	if o == nil {
		e.u = u
		e.e = e.c.e
		e.fill = fill
		e.resultCols = []*Col{{Typ: types.T_int8, Name: "test"}}
		return nil
	}
	// generates scope list from the relation algebra operator chain.
	ss, err := e.c.compileAlgebra(o)
	if err != nil {
		return err
	}
	mp := o.Attribute()
	attrs := o.ResultColumns()
	cs := make([]*Col, 0, len(mp))
	{
		for i := 0; i < len(attrs); i++ {
			if _, ok := mp[attrs[i]]; !ok {
				attrs = append(attrs[:i], attrs[i+1:]...)
				i--
			}
		}
	}
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
	e.resultCols = cs
	e.e = e.c.e
	e.fill = fill
	e.scopes = fillOutput(ss, &output.Argument{Data: u, Func: fill, Attrs: attrs}, e.c.proc)
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

// Run applies the scopes to the specified data object
// and run through the instruction in each of the scope.
func (e *Exec) Run(ts uint64) error {
	var wg sync.WaitGroup

	if len(e.scopes) == 0 {
		return nil
	}
	fmt.Printf("+++++++++\n")
	Print(nil, e.scopes)
	fmt.Printf("+++++++++\n")
	for i := range e.scopes {
		switch e.scopes[i].Magic {
		case Normal:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Run(e.e); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case Merge:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.MergeRun(e.e); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case Insert:
			wg.Add(1)
			go func(s *Scope) {
				if rows, err := s.Insert(ts); err != nil {
					e.err = err
				} else {
					e.SetAffectedRows(rows)
				}
				wg.Done()
			}(e.scopes[i])
		case Explain:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Explain(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case DropTable:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.DropTable(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case DropDatabase:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.DropDatabase(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case CreateTable:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.CreateTable(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case CreateDatabase:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.CreateDatabase(ts); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case ShowTables:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.ShowTables(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		case ShowDatabases:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.ShowDatabases(e.u, e.fill); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.scopes[i])
		}
	}

	wg.Wait()
	return e.err
}

// compileAlgebra compiles relation algebra operator to execution unit list(scope list).
func (c *compile) compileAlgebra(o op.OP) ([]*Scope, error) {
	switch n := o.(type) {
	case *insert.Insert:
		return []*Scope{{Magic: Insert, Operator: o}}, nil
	case *explain.Explain:
		return []*Scope{{Magic: Explain, Operator: o}}, nil
	case *dropTable.DropTable:
		return []*Scope{{Magic: DropTable, Operator: o}}, nil
	case *dropDatabase.DropDatabase:
		return []*Scope{{Magic: DropDatabase, Operator: o}}, nil
	case *createTable.CreateTable:
		return []*Scope{{Magic: CreateTable, Operator: o}}, nil
	case *createDatabase.CreateDatabase:
		return []*Scope{{Magic: CreateDatabase, Operator: o}}, nil
	case *showTables.ShowTables:
		return []*Scope{{Magic: ShowTables, Operator: o}}, nil
	case *showDatabases.ShowDatabases:
		return []*Scope{{Magic: ShowDatabases, Operator: o}}, nil
	case *projection.Projection:
		return c.compileOutput(n, make(map[string]uint64))
	case *top.Top:
		return c.compileTopOutput(n, make(map[string]uint64))
	case *order.Order:
		return c.compileOrderOutput(n, make(map[string]uint64))
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%s' unsupprt now", o))

}

// compile returns
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
