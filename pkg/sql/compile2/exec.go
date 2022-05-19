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

package compile2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()

	e.u = u
	e.e = e.c.e
	e.fill = fill
	return nil
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (e *Exec) Run(ts uint64) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()

	switch e.scope.Magic {
	case Merge:
		return nil
	case CreateDatabase:
		return e.scope.CreateDatabase(ts, e.c.proc.Snapshot, e.c.e)
	}
	return nil
}

func (e *Exec) compileScope(pn *plan.Plan) (*Scope, error) {
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Ddl:
		switch qry.Ddl.DdlType {
		case plan.DataDefinition_CREATE_DATABASE:
			return &Scope{
				Magic: CreateDatabase,
				Plan:  pn,
			}, nil
		}
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
