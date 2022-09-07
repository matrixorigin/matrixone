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

package tempengine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// note that,we don't need to think about Concurrency,
// as we know, in one Session, we hava one TempEngine,
// different sessions won't bother each other,
// and we can't execute multi-sqls one time

// TempEngine is used to store temporary tables
// For TempEngine, it just has only one database, itdoesn't need more
// when we find a tempRelation, just use "databaseName-TableName"
type TempEngine struct {
	// node engine.Node
	tempDatabase *TempDatabase
}

// once TempRelation starts to write a batch
// we will regard this batch as a block, and then we will
type TempRelation struct {
	tblSchema TableSchema
	blockNums uint64
	rows      uint64
	// string will be like dbName-tblName-blockId-attrName, the []byte is the data of the col in
	// this block (a block is a batch)
	bytesData  map[string][]byte
	currDbName string
}

type TempDatabase struct {
	nameToRelation map[string]*TempRelation // "string shoule be databaseName-tblName"
	currDbName     string
}

type TempReader struct {
	blockIdxs   []uint64 // indicate which blocks this reader should read
	ownRelation *TempRelation
	waterMark   uint64
	helperBufs  []*bytes.Buffer
	decodeBufs  []*bytes.Buffer
}

type TableSchema struct {
	tblName    string             //tblName is truely dbName+tblName
	attrs      []engine.Attribute // record the
	NameToAttr map[string]engine.Attribute
	// TODO: we need to support primaryKey, partition key and auto_increment_key so on
	// this needs to refer to the TAE engine
}
