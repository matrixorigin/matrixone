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
	"log"
	"matrixone/pkg/sql/parsers"
	"matrixone/pkg/sql/parsers/dialect"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
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
	stmts, err := parsers.Parse(dialect.MYSQL, c.sql)
	if err != nil {
		log.Fatal(err)
	}
	es := make([]*Exec, len(stmts))
	for i := range stmts {
		es[i] = &Exec{
			c:    c,
			stmt: stmts[i],
		}
	}
	return es, nil
}
