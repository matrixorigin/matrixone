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
	"regexp"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	CatalogVersion_V1 uint32 = 1

	CatalogVersion_Curr uint32 = CatalogVersion_V1
)

func init() {
	MoDatabaseTableDefs = make([]engine.TableDef, len(MoDatabaseSchema))
	for i, name := range MoDatabaseSchema {
		MoDatabaseTableDefs[i] = newAttributeDef(name, MoDatabaseTypes[i], i == 0)
	}
	MoTablesTableDefs = make([]engine.TableDef, len(MoTablesSchema))
	for i, name := range MoTablesSchema {
		MoTablesTableDefs[i] = newAttributeDef(name, MoTablesTypes[i], i == 0)
	}
	MoColumnsTableDefs = make([]engine.TableDef, len(MoColumnsSchema))
	for i, name := range MoColumnsSchema {
		MoColumnsTableDefs[i] = newAttributeDef(name, MoColumnsTypes[i], i == 0)
	}
	MoTableMetaDefs = make([]engine.TableDef, len(MoTableMetaSchema))
	for i, name := range MoTableMetaSchema {
		MoTableMetaDefs[i] = newAttributeDef(name, MoTableMetaTypes[i], i == 0)
	}
}

func newAttributeDef(name string, typ types.Type, isPrimary bool) engine.TableDef {
	return &engine.AttributeDef{
		Attr: engine.Attribute{
			Type:    typ,
			Name:    name,
			Primary: isPrimary,
			Alg:     compress.Lz4,
			Default: &plan.Default{NullAbility: true},
		},
	}
}

// consume a set of entries and return a command and the remaining entries
func ParseEntryList(es []*api.Entry) (any, []*api.Entry, error) {
	if len(es) == 0 {
		return nil, nil, nil
	}
	e := es[0]
	if e.DatabaseId != MO_CATALOG_ID {
		return e, es[1:], nil
	}
	switch e.TableId {
	case MO_DATABASE_ID:
		bat, err := batch.ProtoBatchToBatch(e.Bat)
		if err != nil {
			return nil, nil, err
		}
		if e.EntryType == api.Entry_Insert {
			return genCreateDatabases(GenRows(bat)), es[1:], nil
		}
		return genDropDatabases(GenRows(bat)), es[1:], nil
	case MO_TABLES_ID:
		bat, err := batch.ProtoBatchToBatch(e.Bat)
		if err != nil {
			return nil, nil, err
		}
		if e.EntryType == api.Entry_Delete {
			return genDropOrTruncateTables(GenRows(bat)), es[1:], nil
		} else if e.EntryType == api.Entry_Update {
			return genUpdateConstraint(GenRows(bat)), es[1:], nil
		} else if e.EntryType == api.Entry_Alter {
			e, err := genUpdateAltertable(GenRows(bat))
			if err != nil {
				return nil, nil, err
			}
			return e, es[1:], nil
		}
		cmds := genCreateTables(GenRows(bat))
		idx := 0
		for i := range cmds {
			// tae's logic
			if len(cmds[i].Comment) > 0 {
				cmds[i].Defs = append(cmds[i].Defs, &engine.CommentDef{
					Comment: cmds[i].Comment,
				})
			}
			if len(cmds[i].Viewdef) > 0 {
				cmds[i].Defs = append(cmds[i].Defs, &engine.ViewDef{
					View: cmds[i].Viewdef,
				})
			}
			if len(cmds[i].Constraint) > 0 {
				c := new(engine.ConstraintDef)
				if err = c.UnmarshalBinary(cmds[i].Constraint); err != nil {
					return nil, nil, err
				}
				cmds[i].Defs = append(cmds[i].Defs, c)
			}
			if cmds[i].Partitioned > 0 || len(cmds[i].Partition) > 0 {
				cmds[i].Defs = append(cmds[i].Defs, &engine.PartitionDef{
					Partitioned: cmds[i].Partitioned,
					Partition:   cmds[i].Partition,
				})
			}
			pro := new(engine.PropertiesDef)
			pro.Properties = append(pro.Properties, engine.Property{
				Key:   SystemRelAttr_Kind,
				Value: string(cmds[i].RelKind),
			})
			pro.Properties = append(pro.Properties, engine.Property{
				Key:   SystemRelAttr_CreateSQL,
				Value: cmds[i].CreateSql,
			})
			cmds[i].Defs = append(cmds[i].Defs, pro)
			if err = fillCreateTable(&idx, &cmds[i], es); err != nil {
				return nil, nil, err
			}
		}
		return cmds, es[idx+1:], nil
	default:
		return e, es[1:], nil
	}
}

func GenBlockInfo(rows [][]any) []BlockInfo {
	infos := make([]BlockInfo, len(rows))
	for i, row := range rows {
		infos[i].BlockID = row[BLOCKMETA_ID_IDX].(types.Blockid)
		infos[i].EntryState = row[BLOCKMETA_ENTRYSTATE_IDX].(bool)
		infos[i].Sorted = row[BLOCKMETA_SORTED_IDX].(bool)
		infos[i].SetMetaLocation(row[BLOCKMETA_METALOC_IDX].([]byte))
		infos[i].SetDeltaLocation(row[BLOCKMETA_DELTALOC_IDX].([]byte))
		infos[i].CommitTs = row[BLOCKMETA_COMMITTS_IDX].(types.TS)
		infos[i].SegmentID = row[BLOCKMETA_SEGID_IDX].(types.Uuid)
	}
	return infos
}

func genCreateDatabases(rows [][]any) []CreateDatabase {
	cmds := make([]CreateDatabase, len(rows))
	for i, row := range rows {
		cmds[i].DatabaseId = row[MO_DATABASE_DAT_ID_IDX].(uint64)
		cmds[i].Name = string(row[MO_DATABASE_DAT_NAME_IDX].([]byte))
		cmds[i].Owner = row[MO_DATABASE_OWNER_IDX].(uint32)
		cmds[i].Creator = row[MO_DATABASE_CREATOR_IDX].(uint32)
		cmds[i].AccountId = row[MO_DATABASE_ACCOUNT_ID_IDX].(uint32)
		cmds[i].CreatedTime = row[MO_DATABASE_CREATED_TIME_IDX].(types.Timestamp)
		cmds[i].CreateSql = string(row[MO_DATABASE_CREATESQL_IDX].([]byte))
		cmds[i].DatTyp = string(row[MO_DATABASE_DAT_TYPE_IDX].([]byte))
	}
	return cmds
}

func genDropDatabases(rows [][]any) []DropDatabase {
	cmds := make([]DropDatabase, len(rows))
	for i, row := range rows {
		cmds[i].Id = row[MO_DATABASE_DAT_ID_IDX].(uint64)
		cmds[i].Name = string(row[MO_DATABASE_DAT_NAME_IDX].([]byte))
	}
	return cmds
}

func genCreateTables(rows [][]any) []CreateTable {
	cmds := make([]CreateTable, len(rows))
	for i, row := range rows {
		cmds[i].TableId = row[MO_TABLES_REL_ID_IDX].(uint64)
		cmds[i].Name = string(row[MO_TABLES_REL_NAME_IDX].([]byte))
		cmds[i].CreateSql = string(row[MO_TABLES_REL_CREATESQL_IDX].([]byte))
		cmds[i].Owner = row[MO_TABLES_OWNER_IDX].(uint32)
		cmds[i].Creator = row[MO_TABLES_CREATOR_IDX].(uint32)
		cmds[i].AccountId = row[MO_TABLES_ACCOUNT_ID_IDX].(uint32)
		cmds[i].DatabaseId = row[MO_TABLES_RELDATABASE_ID_IDX].(uint64)
		cmds[i].DatabaseName = string(row[MO_TABLES_RELDATABASE_IDX].([]byte))
		cmds[i].Comment = string(row[MO_TABLES_REL_COMMENT_IDX].([]byte))
		cmds[i].Partitioned = row[MO_TABLES_PARTITIONED_IDX].(int8)
		cmds[i].Partition = string(row[MO_TABLES_PARTITION_INFO_IDX].([]byte))
		cmds[i].Viewdef = string(row[MO_TABLES_VIEWDEF_IDX].([]byte))
		cmds[i].Constraint = row[MO_TABLES_CONSTRAINT_IDX].([]byte)
		cmds[i].RelKind = string(row[MO_TABLES_RELKIND_IDX].([]byte))
	}
	return cmds
}

func genUpdateConstraint(rows [][]any) []UpdateConstraint {
	cmds := make([]UpdateConstraint, len(rows))
	for i, row := range rows {
		cmds[i].TableId = row[MO_TABLES_REL_ID_IDX].(uint64)
		cmds[i].DatabaseId = row[MO_TABLES_RELDATABASE_ID_IDX].(uint64)
		cmds[i].TableName = string(row[MO_TABLES_REL_NAME_IDX].([]byte))
		cmds[i].DatabaseName = string(row[MO_TABLES_RELDATABASE_IDX].([]byte))
		cmds[i].Constraint = row[MO_TABLES_UPDATE_CONSTRAINT].([]byte)
	}
	return cmds
}

func genUpdateAltertable(rows [][]any) ([]*api.AlterTableReq, error) {
	cmds := make([]*api.AlterTableReq, len(rows))
	for i, row := range rows {
		req := &api.AlterTableReq{}
		err := req.Unmarshal(row[MO_TABLES_ALTER_TABLE].([]byte))
		if err != nil {
			return nil, err
		}
		cmds[i] = req
	}
	return cmds, nil
}

func genDropOrTruncateTables(rows [][]any) []DropOrTruncateTable {
	cmds := make([]DropOrTruncateTable, len(rows))
	for i, row := range rows {
		name := string(row[SKIP_ROWID_OFFSET+MO_TABLES_REL_NAME_IDX].([]byte))
		if id, tblName, ok := isTruncate(name); ok {
			cmds[i].Id = id
			cmds[i].Name = tblName
			cmds[i].NewId = row[SKIP_ROWID_OFFSET+MO_TABLES_REL_ID_IDX].(uint64)
			cmds[i].DatabaseId = row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_ID_IDX].(uint64)
			cmds[i].DatabaseName = string(row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_IDX].([]byte))
		} else {
			cmds[i].IsDrop = true
			cmds[i].Id = row[SKIP_ROWID_OFFSET+MO_TABLES_REL_ID_IDX].(uint64)
			cmds[i].Name = name
			cmds[i].DatabaseId = row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_ID_IDX].(uint64)
			cmds[i].DatabaseName = string(row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_IDX].([]byte))
		}
	}
	return cmds
}

func fillCreateTable(idx *int, cmd *CreateTable, es []*api.Entry) error {
	for i, e := range es {
		// to find tabledef, only need to detect the insertion of mo_columns
		if e.TableId != MO_COLUMNS_ID || e.EntryType != api.Entry_Insert {
			continue
		}
		bat, err := batch.ProtoBatchToBatch(e.Bat)
		if err != nil {
			return err
		}
		rows := GenRows(bat)
		for _, row := range rows {
			if row[MO_COLUMNS_ATT_RELNAME_ID_IDX].(uint64) == cmd.TableId {
				def, err := genTableDefs(row)
				if err != nil {
					return err
				}
				cmd.Defs = append(cmd.Defs, def)
				if i > *idx {
					*idx = i
				}
			}
		}
	}
	return nil
}

func genTableDefs(row []any) (engine.TableDef, error) {
	var attr engine.Attribute

	attr.Name = string(row[MO_COLUMNS_ATTNAME_IDX].([]byte))
	attr.Alg = compress.Lz4
	if err := types.Decode(row[MO_COLUMNS_ATTTYP_IDX].([]byte), &attr.Type); err != nil {
		return nil, err
	}
	if row[MO_COLUMNS_ATTHASDEF_IDX].(int8) == 1 {
		attr.Default = new(plan.Default)
		if err := types.Decode(row[MO_COLUMNS_ATT_DEFAULT_IDX].([]byte), attr.Default); err != nil {
			return nil, err
		}
	}
	if row[MO_COLUMNS_ATT_HAS_UPDATE_IDX].(int8) == 1 {
		attr.OnUpdate = new(plan.OnUpdate)
		if err := types.Decode(row[MO_COLUMNS_ATT_UPDATE_IDX].([]byte), attr.OnUpdate); err != nil {
			return nil, err
		}
	}
	attr.Comment = string(row[MO_COLUMNS_ATT_COMMENT_IDX].([]byte))
	attr.IsHidden = row[MO_COLUMNS_ATT_IS_HIDDEN_IDX].(int8) == 1
	attr.AutoIncrement = row[MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX].(int8) == 1
	attr.Primary = string(row[MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX].([]byte)) == "p"
	attr.ClusterBy = row[MO_COLUMNS_ATT_IS_CLUSTERBY].(int8) == 1
	return &engine.AttributeDef{Attr: attr}, nil
}

func GenRows(bat *batch.Batch) [][]any {
	rows := make([][]any, bat.Length())
	for i := 0; i < bat.Length(); i++ {
		rows[i] = make([]any, bat.VectorCount())
	}
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		switch vec.GetType().Oid {
		case types.T_bool:
			col := vector.MustFixedCol[bool](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int8:
			col := vector.MustFixedCol[int8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int16:
			col := vector.MustFixedCol[int16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int32:
			col := vector.MustFixedCol[int32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int64:
			col := vector.MustFixedCol[int64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint8:
			col := vector.MustFixedCol[uint8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint16:
			col := vector.MustFixedCol[uint16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint32:
			col := vector.MustFixedCol[uint32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint64:
			col := vector.MustFixedCol[uint64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_float32:
			col := vector.MustFixedCol[float32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_float64:
			col := vector.MustFixedCol[float64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_date:
			col := vector.MustFixedCol[types.Date](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_time:
			col := vector.MustFixedCol[types.Time](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_datetime:
			col := vector.MustFixedCol[types.Datetime](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_timestamp:
			col := vector.MustFixedCol[types.Timestamp](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_decimal64:
			col := vector.MustFixedCol[types.Decimal64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_decimal128:
			col := vector.MustFixedCol[types.Decimal128](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uuid:
			col := vector.MustFixedCol[types.Uuid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_TS:
			col := vector.MustFixedCol[types.TS](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_Rowid:
			col := vector.MustFixedCol[types.Rowid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_Blockid:
			col := vector.MustFixedCol[types.Blockid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_json, types.T_text:
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = vec.GetBytesAt(j)
			}
		default:
			panic(fmt.Sprintf("unspported type: %v", vec.GetType()))
		}
	}
	return rows
}

func isTruncate(name string) (uint64, string, bool) {
	ok, _ := regexp.MatchString(`\_\d+\_meta`, name)
	if !ok {
		return 0, "", false
	}
	reg, _ := regexp.Compile(`\d+`)
	str := reg.FindString(name)
	id, _ := strconv.ParseUint(str, 10, 64)
	return id, name[len(str)+Meta_Length:], true
}

func BuildQueryResultPath(accountName, statementId string, blockIdx int) string {
	return fmt.Sprintf(QueryResultPath, accountName, statementId, blockIdx)
}

func BuildQueryResultMetaPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultMetaPath, accountName, statementId)
}
