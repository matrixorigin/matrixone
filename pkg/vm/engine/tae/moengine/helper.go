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

package moengine

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func SchemaToDefs(schema *catalog.Schema) (defs []engine.TableDef, err error) {
	if schema.Comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = schema.Comment
		defs = append(defs, commentDef)
	}

	if schema.View != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = schema.View
		defs = append(defs, viewDef)
	}
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}

		expr := &plan.Expr{}
		if col.Default.Expr != nil {
			if err := expr.Unmarshal(col.Default.Expr); err != nil {
				return nil, err
			}
		} else {
			expr = nil
		}

		def := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    col.Name,
				Type:    col.Type,
				Primary: col.IsPrimary(),
				Default: &plan.Default{
					NullAbility:  col.Default.NullAbility,
					OriginString: col.Default.OriginString,
					Expr:         expr,
				},
			},
		}
		defs = append(defs, def)
	}
	if schema.SortKey != nil && schema.SortKey.IsPrimary() {
		pk := new(engine.PrimaryIndexDef)
		for _, def := range schema.SortKey.Defs {
			pk.Names = append(pk.Names, def.Name)
		}
		defs = append(defs, pk)
	}
	return
}

func DefsToSchema(name string, defs []engine.TableDef) (schema *catalog.Schema, err error) {
	schema = catalog.NewEmptySchema(name)
	pkMap := make(map[string]int)
	for _, def := range defs {
		if pkDef, ok := def.(*engine.PrimaryIndexDef); ok {
			for i, name := range pkDef.Names {
				pkMap[name] = i
			}
			break
		}
	}
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.AttributeDef:
			if idx, ok := pkMap[defVal.Attr.Name]; ok {
				if err = schema.AppendPKColWithAttribute(defVal.Attr, idx); err != nil {
					return
				}
			} else {
				if err = schema.AppendColWithAttribute(defVal.Attr); err != nil {
					return
				}
			}

		case *engine.PropertiesDef:
			for _, property := range defVal.Properties {
				switch strings.ToLower(property.Key) {
				case catalog.SystemRelAttr_Comment:
					schema.Comment = property.Value
				}
			}

		case *engine.ViewDef:
			schema.View = defVal.View

		default:
			// We will not deal with other cases for the time being
		}
	}
	if err = schema.Finalize(false); err != nil {
		return
	}
	if schema.IsCompoundSortKey() {
		err = fmt.Errorf("%w: compound idx not supported yet", catalog.ErrSchemaValidation)
	}
	return
}
