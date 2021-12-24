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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var (
	errIndexExists = errors.New(errno.InvalidName, "index already existed")
	errIndexTypeNotSupported = errors.New(errno.FeatureNotSupported, "unsupported index type")

	// err for index support
	errBsiUnsupported = errors.New(errno.FeatureNotSupported, "BSI index not support type char/varchar now")
)

func (b *build) BuildCreateIndex(stmt *tree.CreateIndex, plan *CreateIndex) error {
	var indexName = string(stmt.Name)
	var defs []engine.TableDef
	var engineIndexType engine.IndexT
	var hasExisted = false
	mpColName := make(map[string]types.Type) // map of relation's column names
	mpIndexName := make(map[string]struct{}) // map of relation's index names

	_, _, r, err := b.tableName(&stmt.Table)
	if err != nil {
		return nil
	}
	for _, def := range r.TableDefs() {
		switch t := def.(type) {
		case *engine.AttributeDef:
			mpColName[t.Attr.Name] = t.Attr.Type
		case *engine.IndexTableDef:
			mpIndexName[t.Name] = struct{}{}
		default: // just do nothing
		}
	}

	{ // check index name
		if _, ok := mpIndexName[indexName]; ok {
			if !stmt.IfNotExists {
				return errIndexExists
			}
			hasExisted = true
		}
	}

	if stmt.IndexOption != nil {
		switch stmt.IndexOption.IType{
		case tree.INDEX_TYPE_BSI:
			engineIndexType = engine.BsiIndex
		default:
			engineIndexType = engine.Invalid
		}
	} else {
		engineIndexType = engine.ZoneMap
	}

	def := &engine.IndexTableDef{
		Typ:      engineIndexType,
		Name:     indexName,
	}

	// return error for unsupported type of index
	switch engineIndexType {
	case engine.ZoneMap, engine.BsiIndex:
		switch len(stmt.KeyParts) {
		case 1: // index for one column now
			key := stmt.KeyParts[0]
			if key.ColName == nil || key.Expr != nil { // key.Expr type signs information about index, eg: *funcExpr
				return errIndexTypeNotSupported
			}
			var col string
			if key.ColName.NumParts == 1 {
				col = key.ColName.Parts[0]
				if colType, ok := mpColName[col]; !ok {
					return errors.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s'", col))
				} else {
					if err := bsiSupport(colType); engineIndexType == engine.BsiIndex && err != nil {
						return err
					}
				}
			} else {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("not supported '%s'", key.ColName.Parts))
			}
			def.ColNames = []string{col}
		default: // composite index
			return errIndexTypeNotSupported
		}

	case engine.Invalid:
		return errIndexTypeNotSupported
	}

	defs = append(defs, def)

	plan.IfNotExistFlag = stmt.IfNotExists
	plan.HasExist = hasExisted
	plan.Defs = defs
	plan.Relation = r
	plan.Id = string(stmt.Name)
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

// bsiSupport returns error if bsi index not support this data type
func bsiSupport(t types.Type) error {
	switch t.Oid {
	case types.T_char, types.T_varchar:
		return errBsiUnsupported
	}
	return nil
}