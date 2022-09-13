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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

const (
	// default database id for catalog
	MO_CATALOG_ID  = 1
	MO_DATABASE_ID = 1
	MO_TABLES_ID   = 2
	MO_COLUMNS_ID  = 3
)

// column's index in catalog table
const (
	MO_DATABASE_DAT_ID_IDX   = 0
	MO_DATABASE_DAT_NAME_IDX = 1
	MO_TABLES_REL_ID_IDX     = 0
	MO_TABLES_REL_NAME_IDX   = 1
)

var (
	MoDatabaseSchema = []string{
		"dat_id",
		"datname",
		"dat_catalog_name",
		"dat_createsql",
		"owner",
		"creator",
		"created_time",
		"account_id",
	}
	MoTablesSchema = []string{
		"rel_id",
		"relname",
		"reldatabase",
		"reldatabase_id",
		"relpersistence",
		"relkind",
		"rel_comment",
		"rel_createsql",
		"created_time",
		"creator",
		"owner",
		"account_id",
	}
	MoColumnsSchema = []string{}
)

type DNStore = logservice.DNStore

// tae's block metadata, which is currently just an empty one,
// does not serve any purpose When tae submits a concrete structure,
// it will replace this structure with tae's code
type BlockMeta struct {
}

// Cache is a multi-version cache for maintaining some table data.
// The cache is concurrently secure,  with multiple transactions accessing
// the cache at the same time.
// For different dn,  the cache is handled independently, the format
// for our example is k-v, k being the dn number and v the timestamp,
// suppose there are 2 dn, for table A exist dn0 - 100, dn1 - 200.
type Cache interface {
	// update table's cache to the specified timestamp
	Update(ctx context.Context, dnList []DNStore, databaseId uint64,
		tableId uint64, ts timestamp.Timestamp) error
	// BlockList return a list of unmodified blocks that do not require
	// a merge read and can be very simply distributed to other nodes
	// to perform queries
	BlockList(ctx context.Context, dnList []DNStore, databaseId uint64,
		tableId uint64, ts timestamp.Timestamp, entries [][]Entry) []BlockMeta
	// NewReader create some readers to read the data of the modified blocks,
	// including workspace data
	NewReader(ctx context.Context, readerNumber int, expr *plan.Expr,
		dnList []DNStore, databaseId uint64, tableId uint64,
		ts timestamp.Timestamp, entries [][]Entry) ([]engine.Reader, error)
}

type Engine struct {
	sync.RWMutex
	getClusterDetails GetClusterDetailsFunc
	db                *DB
	txns              map[string]*Transaction
}

// DB is implementataion of cache
type DB struct {
	readTs timestamp.Timestamp
}

// Transaction represents a transaction
type Transaction struct {
	db *DB
	// readOnly default value is true, once a write happen, then set to false
	readOnly bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	blockId uint64
	// use for solving halloween problem
	statementId uint64
	meta        txn.TxnMeta
	// fileMaps used to store the mapping relationship between s3 filenames
	// and blockId
	fileMap map[string]uint64
	// writes cache stores any writes done by txn
	// every statement is an element
	writes   [][]Entry
	dnStores []DNStore
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	// blockName for s3 file
	fileName string
	// blockId for s3 file
	blockId uint64
	// update or delete tuples
	bat *batch.Batch
}

type database struct {
	databaseId   uint64
	databaseName string
	txn          *Transaction
}

type table struct {
	tableId   uint64
	tableName string
	db        *database
}
