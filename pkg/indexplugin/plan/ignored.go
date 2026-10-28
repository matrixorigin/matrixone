// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// BuildIgnoredTableDefs returns the IndexDef and TableDef of the ignored table of an index:
// the source rows the index cannot store, keyed by the source primary key pk.
func BuildIgnoredTableDefs(
	ctx CompilerContext,
	indexInfo *tree.Index,
	indexParts []string,
	pk *plan.ColDef,
) (*plan.IndexDef, *plan.TableDef, error) {
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	indexDef, err := CreateIndexDef(ctx, indexInfo, indexTableName, catalog.IndexIgnored_TblType, indexParts, false)
	if err != nil {
		return nil, nil, err
	}
	pkTyp := pk.Typ
	pkTyp.AutoIncr = false
	pkTyp.NotNullable = true
	tableDef := &plan.TableDef{
		Name:      indexTableName,
		TableType: catalog.IndexIgnored_TblType,
		Cols: []*plan.ColDef{
			{
				Name:    catalog.IndexIgnored_TblCol_Pk,
				Alg:     plan.CompressType_Lz4,
				Typ:     pkTyp,
				Primary: true,
				Default: &plan.Default{NullAbility: false},
			},
			{
				Name:    catalog.IndexIgnored_TblCol_Reason,
				Alg:     plan.CompressType_Lz4,
				Typ:     plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				Default: &plan.Default{NullAbility: false},
			},
			{
				Name:    catalog.IndexIgnored_TblCol_Ts,
				Alg:     plan.CompressType_Lz4,
				Typ:     plan.Type{Id: int32(types.T_int64)},
				Default: &plan.Default{NullAbility: false},
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{catalog.IndexIgnored_TblCol_Pk},
			PkeyColName: catalog.IndexIgnored_TblCol_Pk,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{Properties: []*plan.Property{
				{Key: catalog.SystemRelAttr_Kind, Value: catalog.IndexIgnored_TblType},
			}},
		},
	})
	return indexDef, tableDef, nil
}
