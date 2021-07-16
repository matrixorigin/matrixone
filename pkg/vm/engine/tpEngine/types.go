package tpEngine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/process"
	"sync"
)

const(
	tpEngineName string= "tp-atomic"

	//internal database
	tpEngineDatabase0 string ="0xFEDCBAABCDEFDB0"
	tpEngineDatabase0Id uint64 = 0

	tpEngineSlash byte = '/' //ascii 47
	tpEngineConcat byte = '-' //ascii 45
	//the max length of string fields in the system table
	tpEngineMaxLengthOfStringFieldInSystemTable int = 20
)

const (
	/*
		step 1 :
		Databases
		------------------------
		0          1        2           3
		database   id       createInfo  schema
		(primary)
		------------------------
		db0        0        "create database info"
	*/
	TABLE_DATABASES_ID uint64 = 0
	TABLE_DATABASES_NAME string = "databases"

	/*
		step 2 : default tables in the database
		Tables
		-----------------------
		0         1        2            3
		table     id       tableInfo    schema
		(primary)
		-----------------
		Databases 0        "create table info"
		Tables    1        "create table info"
		Indexes   2        "create table info"
		Meta1     3        "create table info"
		Meta2     4        "create table info"
	*/
	TABLE_TABLES_ID uint64 = 1
	TABLE_TABLES_NAME string = "tables"

	/*
		step 3 : default indexes on the table
		Indexes
		-------------------------
		0          1           2       3              4
		tableId    indexName   id      indexInfo      schema
		(  primary  key    )
		--------------------------
		0          primary     0       "create index info"
		1          primary     0       "create index info"
		2          primary     0       "create index info"
		3          primary     0       "create index info"
		4          primary     0       "create index info"

	*/
	TABLE_INDEXES_ID uint64 = 2
	TABLE_INDEXES_NAME string = "indexes"
	TABLE_INDEXES_PRIMARY_KEY_PART2 string = "primary"

	/*
		step 4 : table meta1
		Meta1
		------------------------------------------
		0                1                2
		rowid            nextDatabaseId   nextTableId
		------------------------------------------
		pk                 1                LAST_TABLE_ID + 1
	*/
	TABLE_META1_ID uint64 = 3
	TABLE_META1_NAME string = "meta1"
	TABLE_META1_PRIMARY_KEY string = "pk"

	/*
		step 5 : table meta2 (merge with "tables")
		Meta1
		------------------------------------------
		0                1
		tableid          nextIndexId
		(primary key)
		------------------------------------------
		0                1
		1                1
		2                1
		3                1
		4                1
	*/
	TABLE_META2_ID uint64 = 4
	TABLE_META2_NAME string = "meta2"

	/*
		step 6 : table views
		Views
		-------------------------------------
		0             1         2           3
		viewname      viewId    viewInfo    schema
		(Primary key)
	*/
	TABLE_VIEWS_ID uint64 = 5
	TABLE_VIEWS_NAME string = "views"

	LAST_TABLE_ID uint64 = TABLE_VIEWS_ID
)

var (
	//prefix key
	TP_ENGINE_PREFIX_KEY *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_UINT64)

	//databases table
	TABLE_DATABASES_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)
	TABLE_DATABASES_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_STRING)
	TABLE_DATABASES_REST_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)

	//tables table
	TABLE_TABLES_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)
	TABLE_TABLES_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_STRING)
	TABLE_TABLES_REST_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)

	//indexes table
	TABLE_INDEXES_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)
	TABLE_INDEXES_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING)
	TABLE_INDEXES_REST_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)

	//META1 table
	TABLE_META1_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_UINT64)
	TABLE_META1_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_STRING)
	TABLE_META1_REST_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_UINT64)

	//meta2 table
	TABLE_META2_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_UINT64)
	TABLE_META2_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_UINT64)
	TABLE_META2_REST_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_UINT64)

	//views table
	TABLE_VIEWS_TUPLE_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)
	TABLE_VIEWS_PRIMARY_KEY_SCHEMA *tpSchema = NewTpSchema(TP_ENCODE_TYPE_STRING)
	TABLE_VIEWS_REST_SCHEMA *tpSchema = NewTpSchema(
		TP_ENCODE_TYPE_UINT64,
		TP_ENCODE_TYPE_STRING,
		TP_ENCODE_TYPE_STRING,
		)
)

/**
Table encoding:

Version 1:
	Tow requirements (for key collation and prefix rightness):
	(a). the length of the each field in primary columns should be fixed.
	(b). the length of the each field in secondary indexes should be fixed.

Cluster Index:

	Key encoding:
		engine/dbId/tableId/indexId/[primary columns]

	Value encoding:
		[primary columns] rest fields

Secondary Index:
	Key encoding:
		engine/dbId/tableId/indexId/[indexed columns]/[primary columns]

	Value encoding:
		row - (primary columns or indexed columns)

 */

/**
the simple schema definition of the table (system table, user table).
Format: |column1 type | column2 type | ... | columnN type
 */
type tpSchema struct {
	//column types
	colTypes []byte
	/*
	if used[i]
		== 1, then, the column i will be encoded
		== 0, then, the column i will be be encoded
	 */
	used []byte
}

/**
decide the schema from the data
 */
func NewTpSchemaHelper(args ...interface{}) *tpSchema {
	var tps []byte
	for _,arg := range args {
		switch a := arg.(type) {
		case uint64:
			tps = append(tps,TP_ENCODE_TYPE_UINT64)
		case string:
			tps = append(tps,TP_ENCODE_TYPE_STRING)
		default:
			panic(fmt.Errorf("unsupported data type %v",a))
		}
	}
	return NewTpSchema(tps...)
}

func NewTpSchema(c ...byte)*tpSchema {
	return &tpSchema{
		colTypes: c,
		used: makeByteSlice(len(c),1),
	}
}

func (ts *tpSchema) ColumnCount() int {
	return len(ts.colTypes)
}

func (ts *tpSchema) ColumnType(i int)byte {
	return ts.colTypes[i]
}

/*
column may be encoded
 */
func (ts *tpSchema) IsUsedInEncoding(i int) bool {
	return ts.used[i] == 1
}

func (ts *tpSchema) UsedInEncoding(i int){
	ts.colTypes[i] = 1
}

func (ts *tpSchema) UnUsedInEncoding(i int){
	ts.colTypes[i] = 0
}

/**
table key
Function: table model primary key -> the key of the kv storage
Format: /Fixed Prefix/Primary keys/Suffix/
Components:
1.Fixed Prefix: /engine/database id/table id/index id/
2.Primary keys: /key1 key2 ... keyN/
3.Suffix:/suf1 suf2 ... sufM/
 */
type tpTableKey struct {
	/*
	schema for prefix:
	string,uint64,uint64,uint64
	 */
	prefixSchema *tpSchema
	//prefix
	engine string
	dbId uint64
	tableId uint64
	indexId uint64
	//schema for primary keys
	primarySchema *tpSchema
	//count >= 1
	primaries []interface{}
	//schema for suffix
	suffixSchema *tpSchema
	//end part
	suffix []interface{}
}

func NewTpTableKey(e string, db, table, index uint64, primSch *tpSchema, prims []interface{}, sufSch *tpSchema, sufs []interface{}) *tpTableKey {
	return &tpTableKey{
		prefixSchema: TP_ENGINE_PREFIX_KEY,
		engine:    e,
		dbId:      db,
		tableId:   table,
		indexId:   index,
		primarySchema: primSch,
		primaries: prims,
		suffixSchema: sufSch,
		suffix:    sufs,
	}
}

func NewTpTableKeyWithSchema(primSch,suffSch *tpSchema) *tpTableKey{
	return &tpTableKey{
		prefixSchema: NewTpSchema(TP_ENCODE_TYPE_STRING,TP_ENCODE_TYPE_UINT64,TP_ENCODE_TYPE_UINT64,TP_ENCODE_TYPE_UINT64),
		primarySchema: primSch,
		suffixSchema: suffSch,
	}
}

/**
table tuple
 */
type tpTuple interface {
	fmt.Stringer
	schema()*tpSchema
	encode(data []byte) []byte
	decode(data []byte) ([]byte,error)
}

type tpTupleImpl struct {
	tpTuple
	schema *tpSchema
	fields []interface{}
}

func NewTpTupleImpl(sch *tpSchema, f ...interface{}) *tpTupleImpl {
	return &tpTupleImpl{
		schema: sch,
		fields:      f,
	}
}

/**
TABLE_DATABASES_NAME row data
 */
type tableDatabasesRow struct {
	tpTupleImpl
	/*
	field 0: dbname
	field 1: dbid
	field 2: dbschema
	 */
}

func NewTableDatabasesRow(n string, id uint64, sch string) *tableDatabasesRow {
	return &tableDatabasesRow{
		tpTupleImpl:tpTupleImpl{
			fields:      []interface{}{n,id,sch},
		},
	}
}

/*
tuple engine for metadata management

string -> string

key: tp-engineName1-db-no1 value: x1
key: tp-engineName1-db-no2 value: x2
key: tp-engineName1-db-no3 value: x3

key: x1 value: db1
key: x2 value: db2
key: x3 value: db3

......
 */
type tpEngine struct {
	engine.Engine
	rwlock sync.RWMutex
	engName string
	kv dist.Storage
	proc *process.Process

	//db0 table meta1
	nextDbNo    uint64
	nextTableNo uint64

	//for fast check
	dbs map[string]*tpTupleImpl

	//for async recycle
	recyclingDb map[string]*tpTupleImpl

	//TODO: async recycle routine
}

/*
the Database in the tp engine

string -> relation info(name,column defs)

key: tp-db1-rel1 value: y1
key: tp-db1-rel2 value: y2
key: tp-db1-rel3 value: y3

key: y1 value: rel1
key: y2 value: rel2
key: y3 value: rel3

......
 */
type tpDatabase struct {
	dbName string
	proc *process.Process
	kv dist.Storage
}

/*
the Relation in the schema

string -> column data

key: tp-rel1-primarykey1 value: z1
key: tp-rel1-primarykey2 value: z2
key: tp-rel1-primarykey3 value: z3

key: z1 value: tuple1
key: z2 value: tuple2
key: z3 value: tuple3

......
 */
type tpRelation struct {
	relName string
	kv dist.Storage
}