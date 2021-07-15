package tpEngine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/process"
	"sync"
)

const(
	tpEngineName string= "atomic"
	tpEngineSlash byte = '/'
	tpEngineConcat byte = '-'
	//the max length of string fields in the system table
	tpEngineMaxLengthOfStringFieldInSystemTable uint64 = 512
)

const (
	/*
		step 1 :
		Databases
		------------------------
		0          1        2
		database   id       dbschema
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
		0         1        2
		table     id       tableschema
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
		0          1           2       3
		tableId    indexName   id      indexschema
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
		0             1         2
		viewname      viewId    viewschema
	*/
	TABLE_VIEWS_ID uint64 = 5
	TABLE_VIEWS_NAME string = "views"

	LAST_TABLE_ID uint64 = TABLE_VIEWS_ID
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
primare key for comparable encoding.
 */
type tpPrimaryKey interface {
	encode([]byte)[]byte
	fmt.Stringer
}

/**
value in kv pair
 */
type tpValue interface {
	encodeValue([]byte)[]byte
	decodeValue([]byte)
	fmt.Stringer
}

/**
table key
map: table model primary key -> the key of the kv storage
 */
type tpTableKey struct {
	//prefix
	engine tpPrimaryKey
	dbId tpPrimaryKey
	tableId tpPrimaryKey
	indexId tpPrimaryKey
	//count >= 1
	primaries []tpPrimaryKey
	//end part
	suffix []tpPrimaryKey
}

/**
table tuple
 */
type tpTuple interface {
	fmt.Stringer
	encode(data []byte) []byte
	decode(data []byte) ([]byte,error)
}

type tpTupleImpl struct {
	tpTuple
	columnTypes []byte
	fields []interface{}
}

func NewTpTupleImpl(f ...interface{})*tpTupleImpl{
	return &tpTupleImpl{
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

/**
TABLE_TABLES_NAME row data
 */
type tableTablesRow struct {
	tpTuple
	tabname string
	tabid uint64
	tabschema string
}

func NewTableTablesRow(n string,id uint64,sch string) *tableTablesRow{
	return &tableTablesRow{
		tabname:   n,
		tabid:     id,
		tabschema: sch,
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
	dbs map[string]*tableDatabasesRow

	//for async recycle
	recyclingDb map[string]*tableDatabasesRow

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