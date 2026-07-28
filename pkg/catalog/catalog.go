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
	"github.com/matrixorigin/matrixone/pkg/compress"
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
	MoTableMetaDefs []engine.TableDef
	// used by tae for mo_tables logical_id index table
	MoTablesLogicalIdIndexTableDefs  []engine.TableDef
	MoTablesLogicalIdIndexConstraint []byte
	MoDatabaseConstraint             []byte
	MoTableConstraint                []byte
	MoColumnConstraint               []byte
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

	{
		// mo_database
		dbCpkPos := MO_DATABASE_CPKEY_IDX
		d.MoDatabaseTableDefs = make([]engine.TableDef, len(MoDatabaseSchema))
		for i, name := range MoDatabaseSchema {
			d.MoDatabaseTableDefs[i] = newAttributeDef(name, MoDatabaseTypes[i], i == dbCpkPos, i == dbCpkPos)
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
		tblCpkPos := MO_TABLES_CPKEY_IDX
		d.MoTablesTableDefs = make([]engine.TableDef, len(MoTablesSchema))
		for i, name := range MoTablesSchema {
			d.MoTablesTableDefs[i] = newAttributeDef(name, MoTablesTypes[i], i == tblCpkPos, i == tblCpkPos || i == MO_TABLES_EXTRA_INFO_IDX)
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
				&engine.IndexDef{
					Indexes: []*plan.IndexDef{
						{
							IndexName:      "idx_rel_logical_id",
							Parts:          []string{SystemRelAttr_LogicalID},
							Unique:         true,
							Visible:        true,
							IndexTableName: MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
							TableExist:     true, // Index table will be created during system startup
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
		colCpkPos := MO_COLUMNS_ATT_CPKEY_IDX
		d.MoColumnsTableDefs = make([]engine.TableDef, len(MoColumnsSchema))
		for i, name := range MoColumnsSchema {
			d.MoColumnsTableDefs[i] = newAttributeDef(name, MoColumnsTypes[i], i == colCpkPos, i == colCpkPos)
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
	{
		// block meta
		d.MoTableMetaDefs = make([]engine.TableDef, len(MoTableMetaSchema))
		for i, name := range MoTableMetaSchema {
			d.MoTableMetaDefs[i] = newAttributeDef(name, MoTableMetaTypes[i], i == 0, i == 0)
		}
	}

	{
		// mo_tables logical_id index table
		d.MoTablesLogicalIdIndexTableDefs = GenMoTablesLogicalIdIndexTableDefs()

		// Pre-calculate index table constraints (consistent with mo_tables)
		for _, def := range d.MoTablesLogicalIdIndexTableDefs {
			if constraintDef, ok := def.(*engine.ConstraintDef); ok {
				d.MoTablesLogicalIdIndexConstraint, _ = constraintDef.MarshalBinary()
				break
			}
		}
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

func IsFakePkName(name string) bool {
	return name == FakePrimaryKeyColName
}

// GenMoTablesLogicalIdIndexTableDefs generates table definitions for mo_tables logical_id index table
func GenMoTablesLogicalIdIndexTableDefs() []engine.TableDef {
	// Standard column definitions for unique index table
	// __mo_index_idx_col: index column (rel_logical_id)
	// __mo_index_pri_col: primary key of main table (for table lookup)

	idxColDef := &engine.AttributeDef{
		Attr: engine.Attribute{
			Name:     IndexTableIndexColName,
			Type:     types.New(types.T_uint64, 0, 0), // rel_logical_id is uint64
			Primary:  false,
			IsHidden: false,
			Alg:      compress.Lz4,
			Default:  &plan.Default{NullAbility: true},
		},
	}

	priColDef := &engine.AttributeDef{
		Attr: engine.Attribute{
			Name:     IndexTablePrimaryColName,
			Type:     types.New(types.T_varchar, 0, 65536), // composite primary key of main table
			Primary:  false,
			IsHidden: false,
			Alg:      compress.Lz4,
			Default:  &plan.Default{NullAbility: true},
		},
	}

	// Index table's primary key is the index column
	pkDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &plan.PrimaryKeyDef{
					Cols:        []uint64{0}, // first column (index column) as primary key
					PkeyColId:   0,
					PkeyColName: IndexTableIndexColName,
					Names:       []string{IndexTableIndexColName},
				},
			},
		},
	}

	return []engine.TableDef{idxColDef, priColDef, pkDef}
}
