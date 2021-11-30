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

import (
	"encoding/gob"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func init() {
	gob.Register(tpMetadata{})
}
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
merge multiple schemas by order into the one
*/
func mergeTpSchema(schs ...*tpSchema)*tpSchema{
	t := NewTpSchema()
	for _,sch := range schs {
		t.colTypes = append(t.colTypes,sch.colTypes...)
		t.used = append(t.used,sch.used...)
	}
	return t
}

type tpMetadata struct {
	Segs  int64
	Rows  int64
	Name  string
	Attrs []engine.Attribute
}