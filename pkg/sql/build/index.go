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
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createIndex"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropIndex"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (b *build) buildCreateIndex(stmt *tree.CreateIndex) (op.OP, error) {
	var defs []engine.TableDef

	dbName, tblName, err := b.tableInfo(stmt.Table)
	if err != nil {
		return nil, err
	}
	db, err := b.e.Database(dbName)
	if err != nil {
		return nil, err
	}
	r, err := db.Relation(tblName)
	if err != nil {
		return nil, err
	}
	typ := stmt.IndexOption.IType
	def := engine.IndexTableDef{Typ: int(typ), ColNames: stmt.KeyParts[0].ColName.Parts[:1], Name: string(stmt.Name)}
	defs = append(defs, &def)
	return createIndex.New(stmt.IfNotExists, r, defs), nil
}

func (b *build) buildDropIndex(stmt *tree.DropIndex) (op.OP, error) {
	dbName, tblName, err := b.tableInfo(stmt.TableName)
	if err != nil {
		return nil, err
	}
	db, err := b.e.Database(dbName)
	if err != nil {
		return nil, err
	}
	r, err := db.Relation(tblName)
	if err != nil {
		return nil, err
	}
	return dropIndex.New(stmt.IfExists, r, string(stmt.Name)), nil
}
