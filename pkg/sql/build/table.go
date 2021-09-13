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
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildTable(stmt *tree.TableName) (op.OP, error) {
	if len(stmt.SchemaName) == 0 {
		return b.getTable(true, b.db, string(stmt.ObjectName))
	}
	return b.getTable(false, string(stmt.SchemaName), string(stmt.ObjectName))
}

func (b *build) getTable(s bool, schema string, name string) (op.OP, error) {
	db, err := b.e.Database(schema)
	if err != nil {
		return nil, sqlerror.New(errno.InvalidSchemaName, err.Error())
	}
	r, err := db.Relation(name)
	if err != nil {
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	return relation.New(s, name, schema, r), nil
}
