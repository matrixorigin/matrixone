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

package frontend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// MoCatalogSchemaAttribute defines the attribute of the schema
type MoCatalogSchemaAttribute struct {
	AttributeName string
	AttributeType types.Type
	IsPrimaryKey  bool
	Comment       string
}

func (mcsa *MoCatalogSchemaAttribute) GetName() string {
	return mcsa.AttributeName
}

func (mcsa *MoCatalogSchemaAttribute) GetType() types.Type {
	return mcsa.AttributeType
}

func (mcsa *MoCatalogSchemaAttribute) GetIsPrimaryKey() bool {
	return mcsa.IsPrimaryKey
}

func (mcsa *MoCatalogSchemaAttribute) GetComment() string {
	return mcsa.Comment
}

// MoCatalogSchema defines the schema for the mo catalog
type MoCatalogSchema struct {
	Name       string
	Attributes []*MoCatalogSchemaAttribute
}

func (mcs *MoCatalogSchema) GetName() string {
	return mcs.Name
}

func (mcs *MoCatalogSchema) Length() int {
	return len(mcs.Attributes)
}

func (mcs *MoCatalogSchema) GetAttributes() []*MoCatalogSchemaAttribute {
	return mcs.Attributes
}

func (mcs *MoCatalogSchema) GetAttribute(i int) *MoCatalogSchemaAttribute {
	return mcs.Attributes[i]
}

// DefineSchemaForMoDatabase decides the schema of the mo_database
func DefineSchemaForMoDatabase() *MoCatalogSchema {
	/*
		mo_database schema

		| Attribute         | Type         | Primary Key | Note   |
		| ---------------- | ------------- | ---- | ------------- |
		| datname          | varchar(256)  | PK   | database name |
		| dat_catalog_name | varchar(256)  |      | catalog name  |
		| dat_createsql    | varchar(4096) |      | create sql    |
	*/
	datNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "datname",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "database name",
	}
	datNameAttr.AttributeType.Width = 256

	datCatalogNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "dat_catalog_name",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "catalog name",
	}
	datCatalogNameAttr.AttributeType.Width = 256

	datCreatesqlAttr := &MoCatalogSchemaAttribute{
		AttributeName: "dat_createsql",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "create sql",
	}
	datCreatesqlAttr.AttributeType.Width = 4096

	attrs := []*MoCatalogSchemaAttribute{
		datNameAttr,
		datCatalogNameAttr,
		datCreatesqlAttr,
	}
	return &MoCatalogSchema{Name: "mo_database", Attributes: attrs}
}

func PrepareInitialDataForMoDatabase() [][]string {
	/*
		hard code database:
		mo_catalog
	*/
	data := [][]string{
		{"mo_catalog", "def", "hardcode"},
	}
	return data
}

func FillInitialDataForMoDatabase() *batch.Batch {
	schema := DefineSchemaForMoDatabase()
	data := PrepareInitialDataForMoDatabase()
	return PrepareInitialDataForSchema(schema, data)
}

func PrepareInitialDataForSchema(schema *MoCatalogSchema, data [][]string) *batch.Batch {
	engineAttributeDefs := ConvertMoCatalogSchemaToEngineFormat(schema)
	batch := AllocateBatchBasedOnEngineAttributeDefinition(engineAttributeDefs, len(data))
	//fill batch with prepared data
	FillBatchWithData(data, batch)
	return batch
}

// DefineSchemaForMoTables decides the schema of the mo_tables
func DefineSchemaForMoTables() *MoCatalogSchema {
	/*
		mo_tables schema

		| Attribute      | Type           | Primary Key  | Note                                                                 |
		| -------------- | ------------- | ----- | ---------------------------------------------------------------------------- |
		| relname        | varchar(256)  | PK    | Name of the table, index, view, etc.                                         |
		| reldatabase    | varchar(256)  | PK,FK | The database that contains this relation. reference mo_database.datname      |
		| relpersistence | char(1)       |       | p = permanent table, t = temporary table                                     |
		| relkind        | char(1)       |       | r = ordinary table, i = index, S = sequence, v = view, m = materialized view |
		| rel_comment    | varchar(1024) |       | comment                                                                      |
		| rel_createsql  | varchar(4096) |       | create sql                                                                   |
	*/
	relNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "relname",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "Name of the table, index, view, etc.",
	}
	relNameAttr.AttributeType.Width = 256

	relDatabaseAttr := &MoCatalogSchemaAttribute{
		AttributeName: "reldatabase",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "The database that contains this relation. reference mo_database.datname",
	}
	relDatabaseAttr.AttributeType.Width = 256

	relPersistenceAttr := &MoCatalogSchemaAttribute{
		AttributeName: "relpersistence",
		AttributeType: types.T_char.ToType(),
		IsPrimaryKey:  false,
		Comment:       "p = permanent table, t = temporary table",
	}
	relPersistenceAttr.AttributeType.Width = 1

	relKindAttr := &MoCatalogSchemaAttribute{
		AttributeName: "relkind",
		AttributeType: types.T_char.ToType(),
		IsPrimaryKey:  false,
		Comment:       "r = ordinary table, i = index, S = sequence, v = view, m = materialized view",
	}
	relKindAttr.AttributeType.Width = 1

	relCommentAttr := &MoCatalogSchemaAttribute{
		AttributeName: "rel_comment",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "comment",
	}
	relCommentAttr.AttributeType.Width = 1024

	relCreatesqlAttr := &MoCatalogSchemaAttribute{
		AttributeName: "rel_createsql",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "create sql",
	}
	relCreatesqlAttr.AttributeType.Width = 4096

	attrs := []*MoCatalogSchemaAttribute{
		relNameAttr,
		relDatabaseAttr,
		relPersistenceAttr,
		relKindAttr,
		relCommentAttr,
		relCreatesqlAttr,
	}
	return &MoCatalogSchema{Name: "mo_tables", Attributes: attrs}
}

func PrepareInitialDataForMoTables() [][]string {
	/*
		hard code tables:
		mo_database,mo_tables,mo_columns

		tabled created in the initdb step:
		mo_global_variables,mo_user
	*/
	data := [][]string{
		{"mo_database", "mo_catalog", "p", "r", "tae hardcode", "databases"},
		{"mo_tables", "mo_catalog", "p", "r", "tae hardcode", "tables"},
		{"mo_columns", "mo_catalog", "p", "r", "tae hardcode", "columns"},
	}
	return data
}

func FillInitialDataForMoTables() *batch.Batch {
	schema := DefineSchemaForMoTables()
	data := PrepareInitialDataForMoTables()
	return PrepareInitialDataForSchema(schema, data)
}

// DefineSchemaForMoColumns decides the schema of the mo_columns
func DefineSchemaForMoColumns() *MoCatalogSchema {
	/*
		mo_columns schema

		| Attribute             | Type          | Primary Key  | Note                                                                                                                                                                     |
		| --------------------- | ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
		| att_database          | varchar(256)  | PK    | database                                                                                                                                                                        |
		| att_relname           | varchar(256)  | PK,UK | The table this column belongs to.(references mo_tables.relname)                                                                                                                 |
		| attname               | varchar(256)  | PK    | The column name                                                                                                                                                                 |
		| atttyp                | int           |       | The data type of this column (zero for a dropped column).                                                                                                                       |
		| attnum                | int           | UK    | The number of the column. Ordinary columns are numbered from 1 up.                                                                                                              |
		| att_length            | int           |       | bytes count for the type.                                                                                                                                                       |
		| attnotnull            | tinyint(1)    |       | This represents a not-null constraint.                                                                                                                                          |
		| atthasdef             | tinyint(1)    |       | This column has a default expression or generation expression.                                                                                                                  |
		| att_default           | varchar(1024) |       | default expression                                                                                                                                                              |
		| attisdropped          | tinyint(1)    |       | This column has been dropped and is no longer valid. A dropped column is still physically present in the table, but is ignored by the parser and so cannot be accessed via SQL. |
		| att_constraint_type   | char(1)       |       | p = primary key constraint, n=no constraint                                                                                                                                     |
		| att_is_unsigned       | tinyint(1)    |       | unsigned or not                                                                                                                                                                 |
		| att_is_auto_increment | tinyint       |       | auto increment or not                                                                                                                                                           |
		| att_comment           | varchar(1024) |       | comment                                                                                                                                                                         |
		| att_is_hidden         | tinyint(1)    |       | hidden or not                                                                                                                                                                   |
	*/
	attDatabaseAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_database",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "database",
	}
	attDatabaseAttr.AttributeType.Width = 256

	attRelNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_relname",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "The table this column belongs to.(references mo_tables.relname)",
	}
	attRelNameAttr.AttributeType.Width = 256

	attNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "attname",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "The column name ",
	}
	attNameAttr.AttributeType.Width = 256

	attTypAttr := &MoCatalogSchemaAttribute{
		AttributeName: "atttyp",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "The data type of this column (zero for a dropped column). ",
	}

	attNumAttr := &MoCatalogSchemaAttribute{
		AttributeName: "attnum",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "The number of the column. Ordinary columns are numbered from 1 up.",
	}

	attLengthAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_length",
		AttributeType: types.T_int32.ToType(),
		IsPrimaryKey:  false,
		Comment:       "bytes count for the type.",
	}

	attNotNullAttr := &MoCatalogSchemaAttribute{
		AttributeName: "attnotnull",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "This represents a not-null constraint.",
	}

	attHasDefAttr := &MoCatalogSchemaAttribute{
		AttributeName: "atthasdef",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "This column has a default expression or generation expression.",
	}

	attDefaultAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_default",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "default expression",
	}
	attDefaultAttr.AttributeType.Width = 1024

	attIsDroppedAttr := &MoCatalogSchemaAttribute{
		AttributeName: "attisdropped",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "This column has been dropped and is no longer valid. A dropped column is still physically present in the table, but is ignored by the parser and so cannot be accessed via SQL.",
	}

	attConstraintTypeAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_constraint_type",
		AttributeType: types.T_char.ToType(),
		IsPrimaryKey:  false,
		Comment:       "p = primary key constraint, n=no constraint",
	}
	attConstraintTypeAttr.AttributeType.Width = 1

	attIsUnsignedAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_is_unsigned",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "unsigned or not",
	}

	attIsAutoIncrementAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_is_auto_increment",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "auto increment or not ",
	}

	attCommentAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_comment",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  false,
		Comment:       "comment",
	}
	attCommentAttr.AttributeType.Width = 1024

	attIsHiddenAttr := &MoCatalogSchemaAttribute{
		AttributeName: "att_is_hidden",
		AttributeType: types.T_int8.ToType(),
		IsPrimaryKey:  false,
		Comment:       "hidden or not",
	}

	attrs := []*MoCatalogSchemaAttribute{
		attDatabaseAttr,
		attRelNameAttr,
		attNameAttr,
		attTypAttr,
		attNumAttr,
		attLengthAttr,
		attNotNullAttr,
		attHasDefAttr,
		attDefaultAttr,
		attIsDroppedAttr,
		attConstraintTypeAttr,
		attIsUnsignedAttr,
		attIsAutoIncrementAttr,
		attCommentAttr,
		attIsHiddenAttr,
	}

	return &MoCatalogSchema{Name: "mo_columns", Attributes: attrs}
}

func extractColumnsInfoFromAttribute(schema *MoCatalogSchema, i int) []string {
	attr := schema.GetAttribute(i)
	moColumnsSchema := DefineSchemaForMoColumns()
	ret := make([]string, moColumnsSchema.Length())

	//| Attribute             | Type          | Primary Key  | Note                                                                                                                                                                     |
	//| --------------------- | ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
	//| att_database          | varchar(256)  | PK    | database                                                                                                                                                                        |
	ret[0] = "mo_catalog"
	//| att_relname           | varchar(256)  | PK,UK | The table this column belongs to.(references mo_tables.relname)                                                                                                                 |
	ret[1] = schema.GetName()
	//| attname               | varchar(256)  | PK    | The column name                                                                                                                                                                 |
	ret[2] = attr.GetName()
	//| atttyp                | int           |       | The data type of this column (zero for a dropped column).                                                                                                                       |
	ret[3] = fmt.Sprintf("%d", attr.GetType().Oid)
	//| attnum                | int           | UK    | The number of the column. Ordinary columns are numbered from 1 up.                                                                                                              |
	ret[4] = fmt.Sprintf("%d", i)
	//| att_length            | int           |       | bytes count for the type.                                                                                                                                                       |
	if attr.GetType().Oid == types.T_varchar || attr.GetType().Oid == types.T_char {
		ret[5] = fmt.Sprintf("%d", attr.GetType().Width)
	} else {
		ret[5] = fmt.Sprintf("%d", attr.GetType().Size)
	}
	//| attnotnull            | tinyint(1)    |       | This represents a not-null constraint.                                                                                                                                          |
	if attr.GetIsPrimaryKey() {
		ret[6] = "1"
	} else {
		ret[6] = "0"
	}
	//| atthasdef             | tinyint(1)    |       | This column has a default expression or generation expression.                                                                                                                  |
	ret[7] = "0"
	//| att_default           | varchar(1024) |       | default expression                                                                                                                                                              |
	ret[8] = "''"
	//| attisdropped          | tinyint(1)    |       | This column has been dropped and is no longer valid. A dropped column is still physically present in the table, but is ignored by the parser and so cannot be accessed via SQL. |
	ret[9] = "0"
	//| att_constraint_type   | char(1)       |       | p = primary key constraint, n=no constraint                                                                                                                                     |
	if attr.GetIsPrimaryKey() {
		ret[10] = "p"
	} else {
		ret[10] = "n"
	}
	//| att_is_unsigned       | tinyint(1)    |       | unsigned or not                                                                                                                                                                 |
	switch attr.GetType().Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		ret[11] = "1"
	default:
		ret[11] = "0"
	}
	//| att_is_auto_increment | tinyint       |       | auto increment or not                                                                                                                                                           |
	ret[12] = "0"
	//| att_comment           | varchar(1024) |       | comment                                                                                                                                                                         |
	ret[13] = attr.GetComment()
	//| att_is_hidden         | tinyint(1)    |       | hidden or not                                                                                                                                                                   |
	ret[14] = "0"
	return ret
}

func PrepareInitialDataForMoColumns() [][]string {
	moDatabaseSchema := DefineSchemaForMoDatabase()
	moDatabaseColumns := make([][]string, moDatabaseSchema.Length())
	for i := 0; i < moDatabaseSchema.Length(); i++ {
		moDatabaseColumns[i] = extractColumnsInfoFromAttribute(moDatabaseSchema, i)
	}

	moTablesSchema := DefineSchemaForMoTables()
	moTablesColumns := make([][]string, moTablesSchema.Length())
	for i := 0; i < moTablesSchema.Length(); i++ {
		moTablesColumns[i] = extractColumnsInfoFromAttribute(moTablesSchema, i)
	}

	moColumnsSchema := DefineSchemaForMoColumns()
	moColumnsColumns := make([][]string, moColumnsSchema.Length())
	for i := 0; i < moColumnsSchema.Length(); i++ {
		moColumnsColumns[i] = extractColumnsInfoFromAttribute(moColumnsSchema, i)
	}

	var data [][]string
	data = append(data, moDatabaseColumns...)
	data = append(data, moTablesColumns...)
	data = append(data, moColumnsColumns...)
	return data
}

func FillInitialDataForMoColumns() *batch.Batch {
	schema := DefineSchemaForMoColumns()
	data := PrepareInitialDataForMoColumns()
	return PrepareInitialDataForSchema(schema, data)
}

// DefineSchemaForMoColumns decides the schema of the mo_global_variables
func DefineSchemaForMoGlobalVariables() *MoCatalogSchema {
	/*
		mo_global_variables schema
		   	  | Attribute       | Type            | Primary Key | Note  |
		      | ----------------- | ------------- | ---- | --- |
		      | gv_variable_name  | varchar(256)  | PK   |  |
		      | gv_variable_value | varchar(1024) |      |  |
	*/
	gvVariableNameAttr := &MoCatalogSchemaAttribute{
		AttributeName: "gv_variable_name",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "",
	}
	gvVariableNameAttr.AttributeType.Width = 256

	gvVariableValueAttr := &MoCatalogSchemaAttribute{
		AttributeName: "gv_variable_value",
		AttributeType: types.T_varchar.ToType(),
		IsPrimaryKey:  true,
		Comment:       "",
	}
	gvVariableNameAttr.AttributeType.Width = 1024

	attrs := []*MoCatalogSchemaAttribute{
		gvVariableNameAttr,
		gvVariableValueAttr,
	}

	return &MoCatalogSchema{Name: "mo_global_variables", Attributes: attrs}
}

func PrepareInitialDataForMoGlobalVariables() [][]string {
	data := [][]string{
		{"max_allowed_packet", "67108864"},
		{"version_comment", "MatrixOne"},
		{"port", "6001"},
		{"host", "0.0.0.0"},
		{"storePath", "./store"},
		{"batchSizeInLoadData", "40000"},
	}
	return data
}

func FillInitialDataForMoGlobalVariables() *batch.Batch {
	schema := DefineSchemaForMoGlobalVariables()
	data := PrepareInitialDataForMoGlobalVariables()
	return PrepareInitialDataForSchema(schema, data)
}
