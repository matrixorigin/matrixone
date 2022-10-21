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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func txnBindAccessInfoFromCtx(txn txnif.AsyncTxn, ctx context.Context) {
	if ctx == nil {
		return
	}
	tid, okt := ctx.Value(defines.TenantIDKey{}).(uint32)
	uid, oku := ctx.Value(defines.UserIDKey{}).(uint32)
	rid, okr := ctx.Value(defines.RoleIDKey{}).(uint32)
	logutil.Debugf("try set %X txn access info to t%d(%v) u%d(%v) r%d(%v), ", txn.GetID(), tid, okt, uid, oku, rid, okr)
	if okt { // TODO: tenantID is required, or all need to be ok?
		txn.BindAccessInfo(tid, uid, rid)
	}
}

func ColDefsToAttrs(colDefs []*catalog.ColDef) (attrs []*engine.Attribute, err error) {
	for _, col := range colDefs {
		expr := &plan.Expr{}
		if col.Default.Expr != nil {
			if err := expr.Unmarshal(col.Default.Expr); err != nil {
				return nil, err
			}
		} else {
			expr = nil
		}

		onUpdate := &plan.Expr{}
		if col.OnUpdate != nil {
			if err := onUpdate.Unmarshal(col.OnUpdate); err != nil {
				return nil, err
			}
		} else {
			onUpdate = nil
		}

		attr := &engine.Attribute{
			Name:    col.Name,
			Type:    col.Type,
			Primary: col.IsPrimary(),
			Comment: col.Comment,
			Default: &plan.Default{
				NullAbility:  col.Default.NullAbility,
				OriginString: col.Default.OriginString,
				Expr:         expr,
			},
			OnUpdate:      onUpdate,
			AutoIncrement: col.IsAutoIncrement(),
		}
		attrs = append(attrs, attr)
	}
	return
}

func SchemaToDefs(schema *catalog.Schema) (defs []engine.TableDef, err error) {
	if schema.Comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = schema.Comment
		defs = append(defs, commentDef)
	}

	if schema.Partition != "" {
		partitionDef := new(engine.PartitionDef)
		partitionDef.Partition = schema.Partition
		defs = append(defs, partitionDef)
	}

	if schema.View != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = schema.View
		defs = append(defs, viewDef)
	}

	if len(schema.IndexInfos) != 0 {
		indexDef := new(engine.ComputeIndexDef)
		for _, indexInfo := range schema.IndexInfos {
			indexDef.Names = append(indexDef.Names, indexInfo.Name)
			indexDef.TableNames = append(indexDef.TableNames, indexInfo.TableName)
			indexDef.Uniques = append(indexDef.Uniques, indexInfo.Unique)
		}
		defs = append(defs, indexDef)
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

		onUpdate := &plan.Expr{}
		if col.OnUpdate != nil {
			if err := onUpdate.Unmarshal(col.OnUpdate); err != nil {
				return nil, err
			}
		} else {
			onUpdate = nil
		}

		def := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    col.Name,
				Type:    col.Type,
				Primary: col.IsPrimary(),
				Comment: col.Comment,
				Default: &plan.Default{
					NullAbility:  col.Default.NullAbility,
					OriginString: col.Default.OriginString,
					Expr:         expr,
				},
				OnUpdate:      onUpdate,
				AutoIncrement: col.IsAutoIncrement(),
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
	pro := new(engine.PropertiesDef)
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   pkgcatalog.SystemRelAttr_Kind,
		Value: string(schema.Relkind),
	})
	if schema.Createsql != "" {
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   pkgcatalog.SystemRelAttr_CreateSQL,
			Value: schema.Createsql,
		})
	}
	defs = append(defs, pro)

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
				case pkgcatalog.SystemRelAttr_Comment:
					schema.Comment = property.Value
				case pkgcatalog.SystemRelAttr_Kind:
					schema.Relkind = property.Value
				case pkgcatalog.SystemRelAttr_CreateSQL:
					schema.Createsql = property.Value
				default:
				}
			}

		case *engine.PartitionDef:
			schema.Partition = defVal.Partition

		case *engine.ViewDef:
			schema.View = defVal.View

		case *engine.ComputeIndexDef:
			for i := range defVal.Names {
				schema.IndexInfos = append(schema.IndexInfos, &catalog.ComputeIndexInfo{
					Name:      defVal.Names[i],
					TableName: defVal.TableNames[i],
					Unique:    defVal.Uniques[i],
				})
			}

		default:
			// We will not deal with other cases for the time being
		}
	}
	if err = schema.Finalize(false); err != nil {
		return
	}
	return
}

// this function used in Precommit. CN won't give PrimaryIndexDef and ComputeIndexDef
// HandleDefsToSchema assume there is at most one AttributeDef with Primary true. TODO:
func HandleDefsToSchema(name string, defs []engine.TableDef) (schema *catalog.Schema, err error) {
	schema = catalog.NewEmptySchema(name)

	have_one := false
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.AttributeDef:
			if defVal.Attr.Primary {
				if have_one {
					panic(moerr.NewInternalError("%s more one pk", name))
				} else {
					have_one = true
				}
				if err = schema.AppendPKColWithAttribute(defVal.Attr, 0); err != nil {
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
				case pkgcatalog.SystemRelAttr_Comment:
					schema.Comment = property.Value
				case pkgcatalog.SystemRelAttr_Kind:
					schema.Relkind = property.Value
				case pkgcatalog.SystemRelAttr_CreateSQL:
					schema.Createsql = property.Value
				default:
				}
			}

		case *engine.PartitionDef:
			schema.Partition = defVal.Partition

		case *engine.ViewDef:
			schema.View = defVal.View

		case *engine.CommentDef:
			schema.Comment = defVal.Comment

		default:
			// We will not deal with other cases for the time being
		}
	}
	if err = schema.Finalize(false); err != nil {
		return
	}
	return
}
