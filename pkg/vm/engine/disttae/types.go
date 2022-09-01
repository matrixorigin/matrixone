// Copyright 2022 Matrix Origin
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

package disttae

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

const (
	INSERT = iota
	DELETE
)

const (
	// default database name for catalog
	MO_CATALOG  = "mo_catalog"
	MO_DATABASE = "mo_database"
	MO_TABLES   = "mo_tables"
	MO_COLUMNS  = "mo_columns"
)

type Reader interface {
}

type Engine struct {
	sync.RWMutex
	getClusterDetails GetClusterDetailsFunc
	txns              map[string]*Transaction
}

// ReadOnly DB cache for tae
type DB struct {
	readTs timestamp.Timestamp
}

// Transaction represents a transaction
type Transaction struct {
	// readOnly default value is true, once a write happen, then set to false
	readOnly bool
	//	db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	blockId uint64
	// use for solving halloween problem
	statementId uint64
	meta        txn.TxnMeta
	// fileMaps used to store the mapping relationship between s3 filenames and blockId
	fileMap map[string]uint64
	// writes cache stores any writes done by txn
	// every statement is an element
	writes   [][]Entry
	dnStores []logservice.DNStore
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableName    string
	databaseName string
	fileName     string       // blockName for s3 file
	blockId      uint64       // blockId for s3 file
	bat          *batch.Batch // update or delete
}
