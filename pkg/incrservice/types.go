// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// AutoIncrementService provides data service for the columns of auto-incremenet.
// Each CN contains a service instance. Whenever a table containing an auto-increment
// column is created, the service internally creates a data cache for the auto-increment
// column to avoid updating the sequence values of these auto-increment columns each
// time data is inserted.
type AutoIncrementService interface {
	// Create a separate transaction is used to create the cache service and insert records
	// into catalog.AutoIncrTableName before the transaction that created the table is committed.
	// When the transaction that created the table is rolled back, the corresponding records in
	// catalog.AutoIncrTableName are deleted.
	Create(ctx context.Context, table *plan.TableDef, txn client.TxnOperator) error
	// Delete until the delete table transaction is committed, no operation is performed, only the
	// records to be deleted are recorded. When the delete table transaction is committed, the
	// delete operation is triggered.
	Delete(ctx context.Context, table *plan.TableDef, txn client.TxnOperator) error
	// InsertValues insert auto columns values into bat.
	InsertValues(ctx context.Context, tabelDef *plan.TableDef, bat *batch.Batch) error
	// Close close the auto increment service
	Close()
}

// incrTableCache a cache containing auto-incremented columns of a table, an incrCache may
// contain multiple cacheItem, each cacheItem corresponds to a auto-incremented column.
//
// The cache of each column's auto-incrementing column is updated independently, without
// interfering with each other.
//
// The incrCache is created at table creation time and, by design, will be initialized by
// inserting multiple rows to catalog.AutoIncrTableName in the same transaction.
//
// A cacheItem will appear on any CN whenever a CN needs to insert a auto-incrementing
// value. The cacheItem on each CN uses its own independent transaction to modify the
// record corresponding to catalog.AutoIncrTableName to pre-apply a Range cache locally.
// Most of the values for each request for a auto-incrementing column on CN are allocated
// on the local cache until the local cache is allocated.
//
// Once the local cache has been allocated, a transaction needs to be started to go to
// the corresponding record of catalog.AutoIncrTableName and continue to allocate the next
// Range. we certainly don't want this to happen when the value actually needs to be allocated,
// as this would affect write performance.
//
// Each CacheItem will have a low-water alert, and when the margin of the locally cached Range
// falls below this realm, an asynchronous task will be started to advance the allocation of
// the next Range.
//
// In addition to passively assigning the next Range in advance, we are going to need to have
// the ability to actively assign it in advance. Each allocated Range has a size, if the
// allocated Range is not enough to meet the demand of one write, it will cause a delayed
// wait for a write process that needs to go to allocate multiple Ranges. So when the amount
// of data written at one time is particularly large, such as load, you need to actively tell
// the cacheItem the approximate amount of data to be written, to avoid the scenario of multiple
// allocations for one write.
type incrTableCache interface {
	table() uint64
	keys() []string
	insertAutoValues(ctx context.Context, tabelDef *plan.TableDef, bat *batch.Batch) error
}

type valueAllocator interface {
	alloc(ctx context.Context, key string, count int) (uint64, uint64, error)
	asyncAlloc(ctx context.Context, key string, count int, cb func(uint64, uint64, error))
	close()
}

// IncrValueStore is used to add and delete metadata records for auto-increment columns.
type IncrValueStore interface {
	// Create add metadata records into catalog.AutoIncrTableName.
	Create(ctx context.Context, key string, value uint64, step int) error
	// Alloc alloc new range for auto-increment column.
	Alloc(ctx context.Context, key string, count int) (uint64, uint64, error)
	// Delete remove metadata records from catalog.AutoIncrTableName.
	Delete(ctx context.Context, keys []string) error
	// Close the store
	Close()
}
