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

package plan

import (
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/parsers/tree"
	"matrixone/pkg/vm/engine"
)

func (b *build) BuildCreateIndex(stmt *tree.CreateIndex, plan *CreateIndex) error {
	var defs []engine.TableDef
	var typ tree.IndexType

	_, _, r, err := b.tableName(&stmt.Table)
	if err != nil {
		return nil
	}
	if stmt.IndexOption != nil {
		typ = stmt.IndexOption.IType
	} else {
		typ = tree.INDEX_TYPE_BTREE
	}

	def := &engine.IndexTableDef{
		Typ:      int(typ),
		ColNames: stmt.KeyParts[0].ColName.Parts[:1],
		Name:     string(stmt.Name),
	}
	defs = append(defs, def)

	plan.IfNotExistFlag = stmt.IfNotExists
	plan.Defs = defs
	plan.Relation = r
	return nil
}

func (b *build) tableName(tbl *tree.TableName) (string, string, engine.Relation, error) {
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}

	db, err := b.e.Database(string(tbl.SchemaName))
	if err != nil {
		return "", "", nil, errors.New(errno.InvalidSchemaName, err.Error())
	}
	r, err := db.Relation(string(tbl.ObjectName))
	if err != nil {
		return "", "", nil, errors.New(errno.UndefinedTable, err.Error())
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), r, nil
}