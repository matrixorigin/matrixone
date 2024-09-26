// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newAttributeDef(name string, typ types.Type, isPrimary, isHidden bool) engine.TableDef {
	return &engine.AttributeDef{
		Attr: engine.Attribute{
			Type:     typ,
			Name:     name,
			Primary:  isPrimary,
			Alg:      compress.Lz4,
			Default:  &plan.Default{NullAbility: true},
			IsHidden: isHidden,
		},
	}
}

// consume a set of entries and return a command and the remaining entries
func ParseEntryList(es []*api.Entry) (any, []*api.Entry, error) {
	if len(es) == 0 {
		return nil, nil, nil
	}
	e := es[0]
	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_DATABASE_ID {
		bat, err := batch.ProtoBatchToBatch(e.Bat)
		if err != nil {
			return nil, nil, err
		}
		if e.EntryType == api.Entry_Insert {
			return &CreateDatabaseReq{
				Bat:  bat,
				Cmds: genCreateDatabases(GenRows(bat)),
			}, es[1:], nil
		} else {
			return &DropDatabaseReq{
				Bat:  bat,
				Cmds: genDropDatabases(GenRows(bat)),
			}, es[1:], nil
		}
	}

	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_TABLES_ID {
		if e.TableName == "alter" {
			bat, err := batch.ProtoBatchToBatch(e.Bat)
			if err != nil {
				return nil, nil, err
			}
			if bat.RowCount() != 1 {
				panic("bad alter table batch size")
			}
			colBat, err := getColumnBatch(es[1])

			if err != nil {
				return nil, nil, err
			}
			if e.EntryType == api.Entry_Insert {
				return &CreateTableReq{
					TableBat:  bat,
					ColumnBat: []*batch.Batch{colBat},
				}, es[2:], nil
			} else {
				return &DropTableReq{
					TableBat:  bat,
					ColumnBat: []*batch.Batch{colBat},
				}, es[2:], nil
			}
		}
		if e.EntryType == api.Entry_Insert {
			return parseCreateTable(es)
		} else if e.EntryType == api.Entry_Delete {
			return parseDeleteTable(es)
		} else {
			panic(fmt.Sprintf("bad mo_tables entry type %d", e.EntryType))
		}
	}

	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_COLUMNS_ID {
		panic("bad write format")
	}
	if e.EntryType == api.Entry_Alter {
		bat, err := batch.ProtoBatchToBatch(e.Bat)
		if err != nil {
			return nil, nil, err
		}
		return genUpdateAltertable(GenRows(bat)), es[1:], nil
	}
	return e, es[1:], nil
}

func parseDeleteTable(es []*api.Entry) (any, []*api.Entry, error) {
	bat, err := batch.ProtoBatchToBatch(es[0].Bat)
	if err != nil {
		return nil, nil, err
	}
	cmds := genDropTables(GenRows(bat))
	colBatch := make([]*batch.Batch, 0, len(cmds))
	for i := 0; i < len(cmds); i++ {
		cbat, err := getColumnBatch(es[i+1])
		if err != nil {
			return nil, nil, err
		}
		colBatch = append(colBatch, cbat)
	}
	req := &DropTableReq{
		Cmds:      cmds,
		TableBat:  bat,
		ColumnBat: colBatch,
	}

	return req, es[len(cmds)+1:], nil
}

func parseCreateTable(es []*api.Entry) (any, []*api.Entry, error) {
	bat, err := batch.ProtoBatchToBatch(es[0].Bat)
	if err != nil {
		return nil, nil, err
	}
	cmds := genCreateTables(GenRows(bat))

	colBatch := make([]*batch.Batch, 0, len(cmds))
	for i := 0; i < len(cmds); i++ {
		cbat, err := collectInsertColBatch(es[i+1], &cmds[i])
		if err != nil {
			return nil, nil, err
		}
		colBatch = append(colBatch, cbat)
	}
	req := &CreateTableReq{
		Cmds:      cmds,
		TableBat:  bat,
		ColumnBat: colBatch,
	}

	return req, es[len(cmds)+1:], nil
}

func getColumnBatch(e *api.Entry) (*batch.Batch, error) {
	if e.DatabaseId != MO_CATALOG_ID || e.TableId != MO_COLUMNS_ID {
		// return nil, moerr.NewInternalErrorNoCtx("mismatched column batch")
		panic("mismatched column batch")
	}
	bat, err := batch.ProtoBatchToBatch(e.Bat)
	if err != nil {
		return nil, err
	}
	return bat, nil
}

func collectInsertColBatch(e *api.Entry, cmd *CreateTable) (*batch.Batch, error) {
	bat, err := getColumnBatch(e)
	if err != nil {
		return nil, err
	}
	rows := GenRows(bat)
	for _, row := range rows {
		if ctid := row[MO_COLUMNS_ATT_RELNAME_ID_IDX].(uint64); ctid != cmd.TableId {
			// return nil, moerr.NewInternalErrorNoCtx("mismatched column id %d -- %d", ctid, cmd.TableId)
			panic(fmt.Sprintf("mismatched column id %d -- %d", ctid, cmd.TableId))
		}
		def, err := genTableDefs(row)
		if def.(*engine.AttributeDef).Attr.Name == Row_ID {
			// skip rowid for TN, who generate rowid column automatically
			continue
		}
		if err != nil {
			return nil, err
		}
		cmd.Defs = append(cmd.Defs, def)
	}
	return bat, nil
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
		cmds[i].Id = row[SKIP_ROWID_OFFSET+MO_DATABASE_DAT_ID_IDX].(uint64)
		cmds[i].Name = string(row[SKIP_ROWID_OFFSET+MO_DATABASE_DAT_NAME_IDX].([]byte))
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
		cmds[i].ExtraInfo = row[MO_TABLES_EXTRA_INFO_IDX].([]byte)
	}

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
			if err := c.UnmarshalBinary(cmds[i].Constraint); err != nil {
				panic(err)
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
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   PropSchemaExtra,
			Value: string(cmds[i].ExtraInfo),
		})
		cmds[i].Defs = append(cmds[i].Defs, pro)
	}
	return cmds
}

func genUpdateAltertable(rows [][]any) []*api.AlterTableReq {
	cmds := make([]*api.AlterTableReq, len(rows))
	for i, row := range rows {
		req := &api.AlterTableReq{}
		err := req.Unmarshal(row[MO_TABLES_ALTER_TABLE].([]byte))
		if err != nil {
			panic("alter table unmarshal failed")
		}
		cmds[i] = req
	}
	return cmds
}

func genDropTables(rows [][]any) []DropTable {
	cmds := make([]DropTable, len(rows))
	for i, row := range rows {
		cmds[i].IsDrop = true
		cmds[i].Id = row[SKIP_ROWID_OFFSET+MO_TABLES_REL_ID_IDX].(uint64)
		cmds[i].Name = string(row[SKIP_ROWID_OFFSET+MO_TABLES_REL_NAME_IDX].([]byte))
		cmds[i].DatabaseId = row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_ID_IDX].(uint64)
		cmds[i].DatabaseName = string(row[SKIP_ROWID_OFFSET+MO_TABLES_RELDATABASE_IDX].([]byte))
	}
	return cmds
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
	attr.EnumVlaues = string(row[MO_COLUMNS_ATT_ENUM_IDX].([]byte))
	attr.Seqnum = row[MO_COLUMNS_ATT_SEQNUM_IDX].(uint16)
	return &engine.AttributeDef{Attr: attr}, nil
}

func GenRows(bat *batch.Batch) [][]any {
	rows := make([][]any, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		rows[i] = make([]any, bat.VectorCount())
	}
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		switch vec.GetType().Oid {
		case types.T_bool:
			col := vector.MustFixedColNoTypeCheck[bool](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_bit:
			col := vector.MustFixedColNoTypeCheck[uint64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int8:
			col := vector.MustFixedColNoTypeCheck[int8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int16:
			col := vector.MustFixedColNoTypeCheck[int16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int32:
			col := vector.MustFixedColNoTypeCheck[int32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_int64:
			col := vector.MustFixedColNoTypeCheck[int64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint8:
			col := vector.MustFixedColNoTypeCheck[uint8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint16:
			col := vector.MustFixedColNoTypeCheck[uint16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint32:
			col := vector.MustFixedColNoTypeCheck[uint32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uint64:
			col := vector.MustFixedColNoTypeCheck[uint64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_float32:
			col := vector.MustFixedColNoTypeCheck[float32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_float64:
			col := vector.MustFixedColNoTypeCheck[float64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_date:
			col := vector.MustFixedColNoTypeCheck[types.Date](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_time:
			col := vector.MustFixedColNoTypeCheck[types.Time](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_datetime:
			col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_timestamp:
			col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_enum:
			col := vector.MustFixedColNoTypeCheck[types.Enum](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_decimal64:
			col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_decimal128:
			col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_uuid:
			col := vector.MustFixedColNoTypeCheck[types.Uuid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_TS:
			col := vector.MustFixedColNoTypeCheck[types.TS](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_Rowid:
			col := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_Blockid:
			col := vector.MustFixedColNoTypeCheck[types.Blockid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = col[j]
			}
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_json, types.T_text, types.T_datalink:
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = vec.GetBytesAt(j)
			}
		case types.T_array_float32:
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = vector.GetArrayAt[float32](vec, j)
			}
		case types.T_array_float64:
			for j := 0; j < vec.Length(); j++ {
				rows[j][i] = vector.GetArrayAt[float64](vec, j)
			}
		default:
			panic(fmt.Sprintf("unspported type: %v", vec.GetType()))
		}
	}
	return rows
}
