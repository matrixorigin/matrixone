package tpEngine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/process"
	"sync"
)

var tpEngineName string= "atomic"
var tpEngineSlash byte = '/'

/**
primare key for comparable encoding
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
	dbs []string

	//for async recycle
	recyclingDb []string
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

primarykey(有多个列怎么办？)

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