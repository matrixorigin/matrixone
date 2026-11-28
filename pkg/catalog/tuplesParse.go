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
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	if e == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx("nil catalog entry")
	}
	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_DATABASE_ID {
		bat, err := parseDatabaseEntryBatch(e)
		if err != nil {
			return nil, nil, err
		}
		if e.EntryType == api.Entry_Insert {
			return &CreateDatabaseReq{Bat: bat, Attrs: e.Attrs, Cmds: genCreateDatabases(GenRows(bat))}, es[1:], nil
		}
		return &DropDatabaseReq{Bat: bat, Attrs: e.Attrs, Cmds: genDropDatabases(GenRows(bat))}, es[1:], nil
	}

	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_TABLES_ID {
		if e.TableName == "alter" {
			bat, err := batch.ViewBinary(e.Bat.Bytes())
			if err != nil {
				return nil, nil, err
			}
			if bat.RowCount() != 1 {
				return nil, nil, moerr.NewInternalErrorNoCtxf("bad alter table batch size %s", common.MoBatchToString(bat, 10))
			}
			colBat, err := getColumnBatch(es[1])

			if err != nil {
				return nil, nil, err
			}
			if e.EntryType == api.Entry_Insert {
				return &CreateTableReq{
					TableBat:    bat,
					ColumnBat:   []*batch.Batch{colBat},
					TableAttrs:  e.Attrs,
					ColumnAttrs: [][]string{es[1].Attrs},
				}, es[2:], nil
			} else {
				return &DropTableReq{
					TableBat:    bat,
					ColumnBat:   []*batch.Batch{colBat},
					TableAttrs:  e.Attrs,
					ColumnAttrs: [][]string{es[1].Attrs},
				}, es[2:], nil
			}
		}
		if e.EntryType == api.Entry_Insert {
			return parseCreateTable(es)
		} else if e.EntryType == api.Entry_Delete {
			return parseDeleteTable(es)
		} else {
			return nil, nil, moerr.NewInternalErrorNoCtxf("bad mo_tables entry type '%d'", e.EntryType)
		}
	}

	if e.DatabaseId == MO_CATALOG_ID && e.TableId == MO_COLUMNS_ID {
		if e.TableName == MO_COLUMNS_UPDATE {
			return e, es[1:], nil
		}
		bat, _ := batch.ViewBinary(e.Bat.Bytes())
		batstr := ""
		if bat != nil {
			batstr = common.MoBatchToString(bat, 5)
		}
		return nil, nil, moerr.NewInternalErrorNoCtxf("bad write format %q, %v, batch %s", e.EntryType, len(es), batstr)
	}
	if e.EntryType == api.Entry_Alter {
		bat, err := batch.ViewBinary(e.Bat.Bytes())
		if err != nil {
			return nil, nil, err
		}
		return genUpdateAltertable(GenRows(bat)), es[1:], nil
	}
	return e, es[1:], nil
}

// parseDatabaseEntryBatch verifies the specialized CREATE/DROP DATABASE tuple
// before GenRows and its typed callers index into it. Generic SQL DML sends a
// different batch layout and must never be interpreted as a database DDL.
func parseDatabaseEntryBatch(e *api.Entry) (*batch.Batch, error) {
	var attrs []string
	var oids []types.T
	switch e.EntryType {
	case api.Entry_Insert:
		attrs = MoDatabaseSchema
		oids = make([]types.T, len(MoDatabaseTypes))
		for i := range oids {
			oids[i] = MoDatabaseTypes[i].Oid
		}
	case api.Entry_Delete:
		attrs = []string{Row_ID, CPrimaryKeyColName, MoDatabaseSchema[MO_DATABASE_DAT_ID_IDX], MoDatabaseSchema[MO_DATABASE_DAT_NAME_IDX]}
		oids = []types.T{types.T_Rowid, types.T_varchar, MoDatabaseTypes[MO_DATABASE_DAT_ID_IDX].Oid, MoDatabaseTypes[MO_DATABASE_DAT_NAME_IDX].Oid}
	default:
		return nil, moerr.NewInvalidInputNoCtxf("invalid mo_database entry type %d", e.EntryType)
	}
	if len(e.Attrs) != len(attrs) {
		return nil, moerr.NewInvalidInputNoCtxf("invalid mo_database entry width: expected %d columns", len(attrs))
	}
	for i, attr := range attrs {
		if e.Attrs[i] != attr {
			return nil, moerr.NewInvalidInputNoCtxf("invalid mo_database entry column %d", i)
		}
	}
	// ViewBinary checks the encoding itself: data sizes, varlen offsets and
	// that every vector holds the row count.
	bat, err := batch.ViewBinary(e.Bat.Bytes())
	if err != nil {
		return nil, err
	}
	if len(bat.Vecs) != len(attrs) {
		return nil, moerr.NewInvalidInputNoCtxf("invalid mo_database entry width: expected %d columns", len(attrs))
	}
	for i, oid := range oids {
		vec := bat.Vecs[i]
		if vec.GetType().Oid != oid || vec.IsConst() || vec.GetNulls().Any() {
			return nil, moerr.NewInvalidInputNoCtxf("invalid mo_database entry vector %d", i)
		}
	}
	return bat, nil
}

func parseDeleteTable(es []*api.Entry) (any, []*api.Entry, error) {
	bat, err := batch.ViewBinary(es[0].Bat.Bytes())
	if err != nil {
		return nil, nil, err
	}
	cmds := genDropTables(GenRows(bat))
	colBatch := make([]*batch.Batch, 0, len(cmds))
	colAttrs := make([][]string, 0, len(cmds))
	for i := 0; i < len(cmds); i++ {
		cbat, err := getColumnBatch(es[i+1])
		if err != nil {
			return nil, nil, err
		}
		colBatch = append(colBatch, cbat)
		colAttrs = append(colAttrs, es[i+1].Attrs)
	}
	req := &DropTableReq{
		Cmds:        cmds,
		TableBat:    bat,
		ColumnBat:   colBatch,
		TableAttrs:  es[0].Attrs,
		ColumnAttrs: colAttrs,
	}

	return req, es[len(cmds)+1:], nil
}

func parseCreateTable(es []*api.Entry) (any, []*api.Entry, error) {
	bat, err := batch.ViewBinary(es[0].Bat.Bytes())
	if err != nil {
		return nil, nil, err
	}
	cmds := genCreateTables(GenRows(bat))

	colBatch := make([]*batch.Batch, 0, len(cmds))
	colAttrs := make([][]string, 0, len(cmds))
	for i := 0; i < len(cmds); i++ {
		cbat, err := collectInsertColBatch(es[i+1], &cmds[i])
		if err != nil {
			return nil, nil, err
		}
		colBatch = append(colBatch, cbat)
		colAttrs = append(colAttrs, es[i+1].Attrs)
	}
	req := &CreateTableReq{
		Cmds:        cmds,
		TableBat:    bat,
		ColumnBat:   colBatch,
		TableAttrs:  es[0].Attrs,
		ColumnAttrs: colAttrs,
	}

	return req, es[len(cmds)+1:], nil
}

func getColumnBatch(e *api.Entry) (*batch.Batch, error) {
	bat, err := batch.ViewBinary(e.Bat.Bytes())
	if err != nil {
		return nil, err
	}
	if e.DatabaseId != MO_CATALOG_ID || e.TableId != MO_COLUMNS_ID {
		return nil, moerr.NewInternalErrorNoCtxf("mismatched column batch %v,%v, %s", e.DatabaseId, e.TableId, common.MoBatchToString(bat, 10))
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
			return nil, moerr.NewInternalErrorNoCtxf("mismatched column id %d -- %d", ctid, cmd.TableId)
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
	// Call the concrete Unmarshal method rather than types.Decode: passing
	// &attr.Type through the encoding.BinaryUnmarshaler interface forces the
	// whole attr local to escape to the heap. The direct method call avoids
	// the extra standalone allocation per column.
	if err := attr.Type.Unmarshal(row[MO_COLUMNS_ATTTYP_IDX].([]byte)); err != nil {
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
	if row[MO_COLUMNS_ATT_HAS_GENERATED_IDX].(int8) == 1 {
		attr.GeneratedCol = new(plan.GeneratedCol)
		if err := types.Decode(row[MO_COLUMNS_ATT_GENERATED_IDX].([]byte), attr.GeneratedCol); err != nil {
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
		if !vector.SupportsAny(vec.GetType().Oid) {
			panic(fmt.Sprintf("unspported type: %v", vec.GetType()))
		}
		for j := 0; j < vec.Length(); j++ {
			rows[j][i] = vector.GetAny(vec, j, false)
		}
	}
	return rows
}

func ShowReqs(reqs []any) string {
	b := new(bytes.Buffer)
	for _, req := range reqs {
		switch r := req.(type) {
		case *CreateDatabaseReq:
			cmd := r.Cmds[0]
			b.WriteString(fmt.Sprintf("CreateDatabaseReq{%v,%v,%v}/%v | ", cmd.AccountId, cmd.DatabaseId, cmd.Name, len(r.Cmds)))
		case *DropDatabaseReq:
			cmd := r.Cmds[0]
			b.WriteString(fmt.Sprintf("DropDatabaseReq{%v,%v}/%v | ", cmd.Id, cmd.Name, len(r.Cmds)))
		case *CreateTableReq:
			cmd := r.Cmds[0]
			b.WriteString(fmt.Sprintf("CreateTableReq{%v,%v,%v,%v}/%v | ", cmd.AccountId, cmd.DatabaseId, cmd.TableId, cmd.Name, len(r.Cmds)))
		case *DropTableReq:
			cmd := r.Cmds[0]
			b.WriteString(fmt.Sprintf("DropTableReq{%v,%v}/%v | ", cmd.Id, cmd.Name, len(r.Cmds)))
		case *api.AlterTableReq:
			b.WriteString(fmt.Sprintf("AlterTableReq{%v,%v,%v} | ", r.DbId, r.TableId, r.Kind))
		default:
			b.WriteString("WriteBatch | ")
		}
	}
	return b.String()
}
