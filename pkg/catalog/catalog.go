// Copyright 2022 Matrix Origin
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

package catalog

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	CatalogVersion_V1 uint32 = 1

	CatalogVersion_Curr uint32 = CatalogVersion_V1
)

type Defines struct {
	// used by memengine or tae
	MoDatabaseTableDefs []engine.TableDef
	// used by memengine or tae
	MoTablesTableDefs []engine.TableDef
	// used by memengine or tae
	MoColumnsTableDefs []engine.TableDef
	// used by memengine or tae or cn
	MoTableMetaDefs      []engine.TableDef
	MoDatabaseConstraint []byte
	MoTableConstraint    []byte
	MoColumnConstraint   []byte
}

func SetupDefines(sid string) {
	runtime.ServiceRuntime(sid).SetGlobalVariables("catalog_defines", NewDefines())
}

func GetDefines(sid string) *Defines {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables("catalog_defines")
	if !ok {
		panic("catalog_defines is not set: " + sid)
	}
	return v.(*Defines)
}

func NewDefines() *Defines {
	d := &Defines{}

	d.MoDatabaseTableDefs = make([]engine.TableDef, len(MoDatabaseSchema))
	for i, name := range MoDatabaseSchema {
		d.MoDatabaseTableDefs[i] = newAttributeDef(name, MoDatabaseTypes[i], i == 0)
	}
	d.MoTablesTableDefs = make([]engine.TableDef, len(MoTablesSchema))
	for i, name := range MoTablesSchema {
		d.MoTablesTableDefs[i] = newAttributeDef(name, MoTablesTypes[i], i == 0)
	}
	d.MoColumnsTableDefs = make([]engine.TableDef, len(MoColumnsSchema))
	for i, name := range MoColumnsSchema {
		d.MoColumnsTableDefs[i] = newAttributeDef(name, MoColumnsTypes[i], i == 0)
	}
	d.MoTableMetaDefs = make([]engine.TableDef, len(MoTableMetaSchema))

	{
		// mo_database
		dbCpkPos := len(MoDatabaseSchema) - 1 // cpk is the last column
		d.MoDatabaseTableDefs = make([]engine.TableDef, len(MoDatabaseSchema))
		for i, name := range MoDatabaseSchema {
			d.MoDatabaseTableDefs[i] = newAttributeDef(name, MoDatabaseTypes[i], i == dbCpkPos)
		}
		def := &engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						Cols:        []uint64{uint64(dbCpkPos)},
						PkeyColId:   0,
						PkeyColName: SystemDBAttr_CPKey,
						Names:       []string{SystemDBAttr_AccID, SystemDBAttr_Name},
						CompPkeyCol: &plan.ColDef{
							ColId:   uint64(dbCpkPos),
							Name:    SystemDBAttr_CPKey,
							Hidden:  true,
							Typ:     plan.Type{Id: int32(types.T_varchar), Scale: 65536},
							Default: &plan.Default{},
							Seqnum:  uint32(dbCpkPos),
						},
					},
				},
			},
		}
		d.MoDatabaseConstraint, _ = def.MarshalBinary()
		d.MoDatabaseTableDefs = append(d.MoDatabaseTableDefs, def)
	}

	{
		// mo_tables
		tblCpkPos := len(MoTablesSchema) - 1
		d.MoTablesTableDefs = make([]engine.TableDef, len(MoTablesSchema))
		for i, name := range MoTablesSchema {
			d.MoTablesTableDefs[i] = newAttributeDef(name, MoTablesTypes[i], i == tblCpkPos)
		}
		def := &engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						Cols:        []uint64{uint64(tblCpkPos)},
						PkeyColId:   0,
						PkeyColName: SystemRelAttr_CPKey,
						Names:       []string{SystemRelAttr_AccID, SystemRelAttr_DBName, SystemRelAttr_Name},
						CompPkeyCol: &plan.ColDef{
							ColId:   uint64(tblCpkPos),
							Name:    SystemRelAttr_CPKey,
							Hidden:  true,
							Typ:     plan.Type{Id: int32(types.T_varchar), Scale: 65536},
							Default: &plan.Default{},
							Seqnum:  uint32(tblCpkPos),
						},
					},
				},
			},
		}
		d.MoTableConstraint, _ = def.MarshalBinary()
		d.MoTablesTableDefs = append(d.MoTablesTableDefs, def)
	}

	{
		// mo_columns
		colCpkPos := len(MoColumnsSchema) - 1
		d.MoColumnsTableDefs = make([]engine.TableDef, len(MoColumnsSchema))
		for i, name := range MoColumnsSchema {
			d.MoColumnsTableDefs[i] = newAttributeDef(name, MoColumnsTypes[i], i == colCpkPos)
		}
		def := &engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						Cols:        []uint64{uint64(colCpkPos)},
						PkeyColId:   0,
						PkeyColName: SystemColAttr_CPKey,
						Names:       []string{SystemColAttr_AccID, SystemColAttr_DBName, SystemColAttr_RelName, SystemColAttr_Name},
						CompPkeyCol: &plan.ColDef{
							ColId:   uint64(colCpkPos),
							Name:    SystemColAttr_CPKey,
							Hidden:  true,
							Typ:     plan.Type{Id: int32(types.T_varchar), Scale: 65536},
							Default: &plan.Default{},
							Seqnum:  uint32(colCpkPos),
						},
					},
				},
			},
		}
		d.MoColumnConstraint, _ = def.MarshalBinary()
		d.MoColumnsTableDefs = append(d.MoColumnsTableDefs, def)
	}

	d.MoTableMetaDefs = make([]engine.TableDef, len(MoTableMetaSchema))
	for i, name := range MoTableMetaSchema {
		d.MoTableMetaDefs[i] = newAttributeDef(name, MoTableMetaTypes[i], i == 0)
	}
	return d
}

func BuildQueryResultPath(accountName, statementId string, blockIdx int) string {
	return fmt.Sprintf(QueryResultPath, accountName, statementId, blockIdx)
}

func BuildQueryResultMetaPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultMetaPath, accountName, statementId)
}

func BuildProfilePath(serviceTyp string, nodeId string, typ, name string) string {
	return fmt.Sprintf("%s/%s_%s_%s_%s", ProfileDir, serviceTyp, nodeId, typ, name)
}
