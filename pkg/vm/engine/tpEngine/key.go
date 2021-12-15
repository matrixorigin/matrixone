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

package tpEngine

import "fmt"

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
encode prefix
*/
func (ttk *tpTableKey) encodePrefix(data []byte) []byte {
	return encodeKeysWithSchema(data,ttk.prefixSchema,
		ttk.engine,
		ttk.dbId,
		ttk.tableId,
		ttk.indexId,
	)
}

func (ttk *tpTableKey) decodePrefix(data []byte) ([]byte,error) {
	data1,prefixs,err := decodeKeys(data,ttk.prefixSchema)
	if err != nil {
		return nil, err
	}

	ttk.engine = prefixs[0].(string)
	ttk.dbId = prefixs[1].(uint64)
	ttk.tableId = prefixs[2].(uint64)
	ttk.indexId = prefixs[3].(uint64)
	return data1,nil
}

/**
encode primary keys
*/
func (ttk *tpTableKey) encodePrimaryKeys(data []byte)[]byte{
	if ttk.primarySchema == nil {
		return data
	}
	if ttk.primaries == nil  || len(ttk.primaries) != ttk.primarySchema.ColumnCount(){
		panic("missing primary keys or schema")
	}

	return encodeKeysWithSchema(data,ttk.primarySchema,ttk.primaries...)
}

func (ttk *tpTableKey) decodePrimaryKeys(data []byte)([]byte,error){
	if ttk.primarySchema == nil {
		return data,nil
	}
	data1,prims,err := decodeKeys(data,ttk.primarySchema)
	if err != nil {
		return nil, err
	}

	ttk.primaries = prims

	return data1, nil
}

/**
encode prefix and primary keys
*/
func (ttk *tpTableKey) encodePrefixAndPrimaryKeys(data []byte)[]byte{
	data = ttk.encodePrefix(data)

	data = ttk.encodePrimaryKeys(data)
	return  data
}

func (ttk *tpTableKey) decodePrefixAndPrimaryKeys(data []byte) ([]byte,error){
	data1,err := ttk.decodePrefix(data)
	if err != nil {
		return nil,err
	}

	data2,err := ttk.decodePrimaryKeys(data1)
	if err != nil {
		return nil, err
	}
	return data2,nil
}

/**
encode suffix keys
*/
func (ttk *tpTableKey) encodeSuffix(data []byte)[]byte{
	if ttk.suffixSchema == nil {
		return data
	}

	if ttk.suffix == nil || len(ttk.suffix) != ttk.suffixSchema.ColumnCount() {
		panic("missing suffix keys or schema")
	}

	return encodeKeysWithSchema(data,ttk.suffixSchema,ttk.suffix...)
}

func (ttk *tpTableKey) decodeSuffix(data []byte)([]byte,error){
	if ttk.suffixSchema == nil {
		return data, nil
	}
	data1,sufs,err := decodeKeys(data,ttk.suffixSchema)
	if err != nil {
		return nil,err
	}

	ttk.suffix = sufs

	return data1,nil
}

/**
encode table key into the comparable bytes
*/
func (ttk *tpTableKey) encode(data []byte)[]byte{
	data = ttk.encodePrefix(data)

	data = ttk.encodePrimaryKeys(data)

	data = ttk.encodeSuffix(data)
	return data
}

/**
decode bytes into tptablekey
*/
func (ttk *tpTableKey) decode(data []byte)([]byte,error){
	data1,err := ttk.decodePrefix(data)
	if err != nil {
		return nil,err
	}

	data2,err := ttk.decodePrimaryKeys(data1)
	if err != nil {
		return nil, err
	}

	data3,err := ttk.decodeSuffix(data2)
	if err != nil {
		return nil, err
	}
	return data3,nil
}

func (ttk *tpTableKey) SetPrimarySchema(sch *tpSchema){
	ttk.primarySchema = sch
}

func (ttk *tpTableKey) SetSuffixSchema(sch *tpSchema){
	ttk.suffixSchema = sch
}

func (ttk *tpTableKey) isPrefixEqualTo(o *tpTableKey) bool {
	if o == nil {
		return false
	}

	return ttk.engine == ttk.engine &&
		ttk.dbId == ttk.dbId &&
		ttk.tableId == ttk.tableId &&
		ttk.indexId == ttk.indexId
}

func (ttk *tpTableKey) String() string {
	return fmt.Sprintf("%s-%d-%d-%d-[%v]-[%v]",
		ttk.engine,ttk.dbId,ttk.tableId,ttk.indexId,
		ttk.primaries,
		ttk.suffix)
}
