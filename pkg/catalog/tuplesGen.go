// Copyright 2024 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Column struct {
	AccountId  uint32
	TableId    uint64
	DatabaseId uint64

	// column name, letter case: origin
	Name            string
	TableName       string
	DatabaseName    string
	Typ             []byte
	TypLen          int32
	Num             int32
	Comment         string
	NotNull         int8
	HasDef          int8
	DefaultExpr     []byte
	ConstraintType  string
	IsClusterBy     int8
	IsHidden        int8
	IsAutoIncrement int8
	HasUpdate       int8
	UpdateExpr      []byte
	Seqnum          uint16
	EnumValues      string
}

type Table struct {
	AccountId    uint32
	TableId      uint64
	DatabaseId   uint64
	TableName    string
	DatabaseName string

	Kind          string
	Comment       string
	CreateSql     string
	UserId        uint32
	RoleId        uint32
	Partitioned   int8
	PartitionInfo string
	Viewdef       string
	Constraint    []byte
	Version       uint32
	ExtraInfo     []byte
}

// genColumnsFromDefs generates column struct from TableDef.
//
// NOTE: 1. it will modify the input TableDef.
// 2. it is usually used in creating new table.
// 3. It will append rowid column as the last column, which is **incorrect** if we want impl alter column gracefully.
func GenColumnsFromDefs(accountId uint32, tableName, databaseName string,
	tableId, databaseId uint64, defs []engine.TableDef) ([]Column, error) {
	{
		mp := make(map[string]int)
		for i, def := range defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				mp[strings.ToLower(attr.Attr.Name)] = i
			}
		}
		for _, def := range defs {
			if constraintDef, ok := def.(*engine.ConstraintDef); ok {
				for _, ct := range constraintDef.Cts {
					if pkdef, ok2 := ct.(*engine.PrimaryKeyDef); ok2 {
						pos := mp[pkdef.Pkey.PkeyColName]
						attr, _ := defs[pos].(*engine.AttributeDef)
						attr.Attr.Primary = true
					}
				}
			}

			if clusterByDef, ok := def.(*engine.ClusterByDef); ok {
				attr, _ := defs[mp[clusterByDef.Name]].(*engine.AttributeDef)
				attr.Attr.ClusterBy = true
			}
		}
	}
	var num int32 = 1
	cols := make([]Column, 0, len(defs))
	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if !ok || attrDef.Attr.Name == Row_ID {
			continue
		}
		typ, err := types.Encode(&attrDef.Attr.Type)
		if err != nil {
			return nil, err
		}
		col := Column{
			Typ:          typ,
			TypLen:       int32(len(typ)),
			AccountId:    accountId,
			TableId:      tableId,
			DatabaseId:   databaseId,
			Name:         attrDef.Attr.Name,
			TableName:    tableName,
			DatabaseName: databaseName,
			Num:          num,
			Comment:      attrDef.Attr.Comment,
			Seqnum:       uint16(num - 1),
			EnumValues:   attrDef.Attr.EnumVlaues,
		}
		attrDef.Attr.ID = uint64(num)
		attrDef.Attr.Seqnum = uint16(num - 1)
		if attrDef.Attr.Default != nil {
			if !attrDef.Attr.Default.NullAbility {
				col.NotNull = 1
			}
			defaultExpr, err := types.Encode(attrDef.Attr.Default)
			if err != nil {
				return nil, err
			}
			if len(defaultExpr) > 0 {
				col.HasDef = 1
				col.DefaultExpr = defaultExpr
			}
		}
		if attrDef.Attr.OnUpdate != nil {
			expr, err := types.Encode(attrDef.Attr.OnUpdate)
			if err != nil {
				return nil, err
			}
			if len(expr) > 0 {
				col.HasUpdate = 1
				col.UpdateExpr = expr
			}
		}
		if attrDef.Attr.IsHidden {
			col.IsHidden = 1
		}
		if attrDef.Attr.AutoIncrement {
			col.IsAutoIncrement = 1
		}
		if attrDef.Attr.Primary {
			col.ConstraintType = SystemColPKConstraint
		} else {
			col.ConstraintType = SystemColNoConstraint
		}
		if attrDef.Attr.ClusterBy {
			col.IsClusterBy = 1
		}

		cols = append(cols, col)
		num++
	}

	// add rowid column
	rowidTyp := types.T_Rowid.ToType()
	typ, _ := types.Encode(&rowidTyp)
	cols = append(cols, Column{
		Typ:          typ,
		TypLen:       int32(len(typ)),
		AccountId:    accountId,
		TableId:      tableId,
		DatabaseId:   databaseId,
		Name:         Row_ID,
		TableName:    tableName,
		DatabaseName: databaseName,
		Num:          int32(len(cols) + 1),
		Seqnum:       uint16(len(cols)),
		IsHidden:     1,
		NotNull:      1,
	})

	return cols, nil
}

func GenCreateDatabaseTuple(sql string, accountId, userId, roleId uint32,
	name string, databaseId uint64, typ string,
	m *mpool.MPool, packer *types.Packer,
) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, MoDatabaseSchema...)
	bat.SetRowCount(1)

	packer.Reset()
	var err error
	defer func() {
		packer.Reset()
		if err != nil {
			bat.Clean(m)
		}
	}()

	{
		idx := MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // dat_id
		if err = vector.AppendFixed(bat.Vecs[idx], databaseId, false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // datname
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_DAT_CATALOG_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // dat_catalog_name
		val := []byte(SystemCatalogName)
		if name == MO_CATALOG { // historic debt
			val = []byte(MO_CATALOG)
		}
		if err = vector.AppendBytes(bat.Vecs[idx], val, false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_CREATESQL_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx])                             // dat_createsql
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(sql), false, m); err != nil { // TODO
			return nil, err
		}
		idx = MO_DATABASE_OWNER_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // owner
		if err = vector.AppendFixed(bat.Vecs[idx], roleId, false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_CREATOR_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // creator
		if err = vector.AppendFixed(bat.Vecs[idx], userId, false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // created_time
		if err = vector.AppendFixed(bat.Vecs[idx], types.CurrentTimestamp(), false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // account_id
		if err = vector.AppendFixed(bat.Vecs[idx], accountId, false, m); err != nil {
			return nil, err
		}
		idx = MO_DATABASE_DAT_TYPE_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx])                             // dat_type
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(typ), false, m); err != nil { // TODO
			return nil, err
		}

		idx = MO_DATABASE_CPKEY_IDX
		bat.Vecs[idx] = vector.NewVec(MoDatabaseTypes[idx]) // cpkey
		packer.EncodeUint32(accountId)
		packer.EncodeStringType([]byte(name))
		if err = vector.AppendBytes(bat.Vecs[idx], packer.Bytes(), false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func GenDropDatabaseTuple(
	rowid types.Rowid, accid uint32, datid uint64, name string,
	m *mpool.MPool, packer *types.Packer,
) (*batch.Batch, error) {
	bat := batch.NewWithSize(4)
	bat.Attrs = append([]string{Row_ID, CPrimaryKeyColName}, MoDatabaseSchema[:2]...)
	bat.SetRowCount(1)
	var err error
	packer.Reset()
	defer func() {
		packer.Reset()
		if err != nil {
			bat.Clean(m)
		}
	}()

	//add the rowid vector as the first one in the batch
	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	if err = vector.AppendFixed(rowidVec, rowid, false, m); err != nil {
		return nil, err
	}
	bat.Vecs[0] = rowidVec

	// add the cpkey vector as the second one in the batch
	cpkVec := vector.NewVec(types.T_varchar.ToType()) // cpkey of accid+dbname
	packer.EncodeUint32(accid)
	packer.EncodeStringType([]byte(name))
	if err = vector.AppendBytes(cpkVec, packer.Bytes(), false, m); err != nil {
		return nil, err
	}
	bat.Vecs[1] = cpkVec

	// add supplementary info to generate ddl cmd for TN handler
	{
		bat.Vecs[2] = vector.NewVec(MoDatabaseTypes[MO_DATABASE_DAT_ID_IDX]) // dat_id
		if err = vector.AppendFixed(bat.Vecs[2], datid, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[3] = vector.NewVec(MoDatabaseTypes[MO_DATABASE_DAT_NAME_IDX]) // datname
		if err = vector.AppendBytes(bat.Vecs[3], []byte(name), false, m); err != nil {
			return nil, err
		}
	}

	return bat, nil
}

func GenTableAlterTuple(constraint [][]byte, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(1)
	bat.Attrs = append(bat.Attrs, SystemRelAttr_Constraint)
	bat.SetRowCount(1)

	var err error
	defer func() {
		if err != nil {
			bat.Clean(m)
		}
	}()

	idx := MO_TABLES_ALTER_TABLE
	bat.Vecs[idx] = vector.NewVec(MoTablesTypes[MO_TABLES_CONSTRAINT_IDX]) // constraint
	for i := 0; i < len(constraint); i++ {
		if err = vector.AppendBytes(bat.Vecs[idx], constraint[i], false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

// genCreateTableTuple yields a batch for insertion into mo_tables.
func GenCreateTableTuple(tbl Table, m *mpool.MPool, packer *types.Packer) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(MoTablesSchema))
	bat.Attrs = append(bat.Attrs, MoTablesSchema...)
	bat.SetRowCount(1)

	var err error
	packer.Reset()
	defer func() {
		packer.Reset()
		if err != nil {
			bat.Clean(m)
		}
	}()

	{
		idx := MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // rel_id
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.TableId, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // relname
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.TableName), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // reldatabase
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.DatabaseName), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // reldatabase_id
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.DatabaseId, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELPERSISTENCE_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // relpersistence
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(""), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELKIND_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // relkind
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.Kind), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_REL_COMMENT_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // rel_comment
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.Comment), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_REL_CREATESQL_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // rel_createsql
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.CreateSql), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // created_time
		if err = vector.AppendFixed(bat.Vecs[idx], types.CurrentTimestamp(), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_CREATOR_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // creator
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.UserId, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_OWNER_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // owner
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.RoleId, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // account_id
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.AccountId, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_PARTITIONED_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // partitioned
		if err = vector.AppendFixed(bat.Vecs[idx], tbl.Partitioned, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_PARTITION_INFO_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // partition_info
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(tbl.PartitionInfo), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_VIEWDEF_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // viewdef
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.Viewdef), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_CONSTRAINT_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // constraint
		if err = vector.AppendBytes(bat.Vecs[idx], tbl.Constraint, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_VERSION_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // schema_version
		if err = vector.AppendFixed(bat.Vecs[idx], uint32(tbl.Version), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_CATALOG_VERSION_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // catalog version
		if err = vector.AppendFixed(bat.Vecs[idx], CatalogVersion_Curr, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_CPKEY_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // cpkey
		packer.EncodeUint32(tbl.AccountId)
		packer.EncodeStringType([]byte(tbl.DatabaseName))
		packer.EncodeStringType([]byte(tbl.TableName))
		if err = vector.AppendBytes(bat.Vecs[idx], packer.Bytes(), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_EXTRA_INFO_IDX
		bat.Vecs[idx] = vector.NewVec(MoTablesTypes[idx]) // extra_info
		if err = vector.AppendBytes(bat.Vecs[idx], tbl.ExtraInfo, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

// genCreateColumnTuples yields a batch for insertion into mo_columns.
func GenCreateColumnTuples(cols []Column, m *mpool.MPool, packer *types.Packer) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, MoColumnsSchema...)
	bat.SetRowCount(len(cols))

	var err error
	packer.Reset()
	defer func() {
		packer.Reset()
		if err != nil {
			bat.Clean(m)
		}
	}()
	for i := 0; i <= MO_COLUMNS_MAXIDX; i++ {
		bat.Vecs[i] = vector.NewVec(MoColumnsTypes[i])
		bat.Vecs[i].PreExtend(len(cols), m)
	}
	for _, col := range cols {
		idx := MO_COLUMNS_ATT_UNIQ_NAME_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(fmt.Sprintf("%v-%v", col.TableId, col.Name)),
			false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ACCOUNT_ID_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.AccountId, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_DATABASE_ID_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.DatabaseId, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_DATABASE_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.DatabaseName), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_RELNAME_ID_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.TableId, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_RELNAME_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.TableName), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTNAME_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.Name), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTTYP_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], col.Typ, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTNUM_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.Num, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_LENGTH_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.TypLen, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTNOTNULL_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.NotNull, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTHASDEF_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.HasDef, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_DEFAULT_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], col.DefaultExpr, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATTISDROPPED_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], int8(0), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.ConstraintType), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_IS_UNSIGNED_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], int8(0), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.IsAutoIncrement, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_COMMENT_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.Comment), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_IS_HIDDEN_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.IsHidden, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_HAS_UPDATE_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.HasUpdate, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_UPDATE_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], col.UpdateExpr, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_IS_CLUSTERBY
		if err = vector.AppendFixed(bat.Vecs[idx], col.IsClusterBy, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_SEQNUM_IDX
		if err = vector.AppendFixed(bat.Vecs[idx], col.Seqnum, false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_ENUM_IDX
		if err = vector.AppendBytes(bat.Vecs[idx], []byte(col.EnumValues), false, m); err != nil {
			return nil, err
		}
		idx = MO_COLUMNS_ATT_CPKEY_IDX
		packer.Reset()
		packer.EncodeUint32(col.AccountId)
		packer.EncodeStringType([]byte(col.DatabaseName))
		packer.EncodeStringType([]byte(col.TableName))
		packer.EncodeStringType([]byte(col.Name))
		if err = vector.AppendBytes(bat.Vecs[idx], packer.Bytes(), false, m); err != nil {
			return nil, err
		}

	}
	return bat, nil
}

// genDropColumnTuple generates the batch for deletion on mo_columns.
// the batch has rowid vector.
func GenDropColumnTuples(rowids []types.Rowid, pks [][]byte, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{Row_ID, CPrimaryKeyColName}
	bat.SetRowCount(len(rowids))

	var err error
	defer func() {
		if err != nil {
			bat.Clean(m)
		}
	}()

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	for _, rowid := range rowids {
		if err = vector.AppendFixed(rowidVec, rowid, false, m); err != nil {
			return nil, err
		}
	}
	bat.Vecs[0] = rowidVec

	pkVec := vector.NewVec(types.T_varchar.ToType())
	for _, s := range pks {
		if err = vector.AppendBytes(pkVec, s, false, m); err != nil {
			return nil, err
		}
	}
	bat.Vecs[1] = pkVec
	return bat, nil
}

// genDropTableTuple generates the batch for deletion on mo_tables.
// the batch has rowid vector.
func GenDropTableTuple(rowid types.Rowid, accid uint32, id, databaseId uint64, name, databaseName string,
	m *mpool.MPool, packer *types.Packer) (*batch.Batch, error) {
	bat := batch.NewWithSize(6)
	bat.Attrs = append([]string{Row_ID, CPrimaryKeyColName}, MoTablesSchema[:4]...)
	bat.SetRowCount(1)

	var err error
	packer.Reset()
	defer func() {
		packer.Reset()
		if err != nil {
			bat.Clean(m)
		}
	}()

	//add the rowid vector as the first one in the batch
	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	if err = vector.AppendFixed(rowidVec, rowid, false, m); err != nil {
		return nil, err
	}
	bat.Vecs[0] = rowidVec

	cpkVec := vector.NewVec(types.T_varchar.ToType()) // cpkey of acc_id + db_name + tbl_name
	packer.EncodeUint32(accid)
	packer.EncodeStringType([]byte(databaseName))
	packer.EncodeStringType([]byte(name))
	if err = vector.AppendBytes(cpkVec, packer.Bytes(), false, m); err != nil {
		return nil, err
	}
	bat.Vecs[1] = cpkVec

	{
		off := 2
		idx := MO_TABLES_REL_ID_IDX
		bat.Vecs[idx+off] = vector.NewVec(MoTablesTypes[idx]) // rel_id
		if err = vector.AppendFixed(bat.Vecs[idx+off], id, false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx+off] = vector.NewVec(MoTablesTypes[idx]) // relname
		if err = vector.AppendBytes(bat.Vecs[idx+off], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx+off] = vector.NewVec(MoTablesTypes[idx]) // reldatabase
		if err = vector.AppendBytes(bat.Vecs[idx+off], []byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx+off] = vector.NewVec(MoTablesTypes[idx]) // reldatabase_id
		if err = vector.AppendFixed(bat.Vecs[idx+off], databaseId, false, m); err != nil {
			return nil, err
		}
	}

	return bat, nil
}
