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

package tempengine

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (db *TempDatabase) Relations(context.Context) ([]string, error) {
	return nil, nil
}

func (db *TempDatabase) Relation(ctx context.Context, db_tblNme string) (engine.Relation, error) {
	// now default we can't find
	if val := db.nameToRelation[db_tblNme]; val != nil {
		return val, nil
	}
	return nil, fmt.Errorf("not found table %s", getTblName(db_tblNme))
}

func (db *TempDatabase) Delete(_ context.Context, db_tblNme string) error {
	return nil
}

// remember that the db_tblname is really "databaseName-tblName"
// for update and delete statement, we add a hidekey called "Paddr"
func (db *TempDatabase) Create(ctx context.Context, db_tblName string, tblDefs []engine.TableDef) error {
	// Create Table - (name, table define)
	schema, err := DefsToSchema(db_tblName, tblDefs)
	if err != nil {
		return err
	}
	_, err = db.Relation(ctx, db_tblName)
	if err == nil {
		return fmt.Errorf("table '%s' already exists", getTblName(db_tblName))
	}
	db.nameToRelation[db_tblName] = &TempRelation{
		tblSchema: *schema,
		blockNums: 0,
		bytesData: make(map[string][]byte),
	}
	return nil
}

func DefsToSchema(db_tblName string, tblDefs []engine.TableDef) (*TableSchema, error) {
	tblSchema := &TableSchema{
		tblName:    db_tblName,
		NameToAttr: make(map[string]engine.Attribute),
	}
	for _, def := range tblDefs {
		switch v := def.(type) {
		case *engine.AttributeDef:
			tblSchema.attrs = append(tblSchema.attrs, v.Attr)
			tblSchema.NameToAttr[v.Attr.Name] = v.Attr
		default:
			return nil, fmt.Errorf("only support normal attribute now")
		}
	}
	return tblSchema, nil
}
