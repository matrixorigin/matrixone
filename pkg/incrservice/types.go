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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// GetAutoIncrementService get increment service from process level runtime
func GetAutoIncrementService(ctx context.Context) AutoIncrementService {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.AutoIncrementService)
	if !ok {
		return nil
	}
	s := v.(AutoIncrementService)
	uuid, ok := ctx.Value(defines.NodeIDKey{}).(string)
	if !ok || uuid == "" {
		return s
	}
	if s.UUID() != uuid {
		v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.AutoIncrementService + "_" + uuid)
		if !ok {
			panic("cannot get the appropriate AutoIncrementService")
		}
		s = v.(AutoIncrementService)
	}
	return s
}

// SetAutoIncrementServiceByID set auto increment service instance into process level runtime.
func SetAutoIncrementServiceByID(id string, v AutoIncrementService) {
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.AutoIncrementService+"_"+id, v)
}

// AutoIncrementService provides data service for the columns of auto-increment.
// Each CN contains a service instance. Whenever a table containing an auto-increment
// column is created, the service internally creates a data cache for the auto-increment
// column to avoid updating the sequence values of these auto-increment columns each
// time data is inserted.
type AutoIncrementService interface {
	// UUID returns the uuid of this increment service, which comes from CN service.
	UUID() string
	// Create a separate transaction is used to create the cache service and insert records
	// into catalog.AutoIncrTableName before the transaction that created the table is committed.
	// When the transaction that created the table is rolled back, the corresponding records in
	// catalog.AutoIncrTableName are deleted.
	Create(ctx context.Context, tableID uint64, caches []AutoColumn, txn client.TxnOperator) error
	// Reset consists of delete+create, if keep is true, then the new self-incrementing column cache
	// will retain the value of the old cache
	Reset(ctx context.Context, oldTableID, newTableID uint64, keep bool, txn client.TxnOperator) error
	// Delete until the delete table transaction is committed, no operation is performed, only the
	// records to be deleted are recorded. When the delete table transaction is committed, the
	// delete operation is triggered.
	Delete(ctx context.Context, tableID uint64, txn client.TxnOperator) error
	// InsertValues insert auto columns values into bat.
	InsertValues(ctx context.Context, tableID uint64, bat *batch.Batch, estimate int64) (uint64, error)
	// CurrentValue return current incr column value.
	CurrentValue(ctx context.Context, tableID uint64, col string) (uint64, error)
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
	commit()
	columns() []AutoColumn
	insertAutoValues(ctx context.Context, tableID uint64, bat *batch.Batch, estimate int64) (uint64, error)
	currentValue(ctx context.Context, tableID uint64, col string) (uint64, error)
	close() error
	adjust(ctx context.Context, cols []AutoColumn) error
}

type valueAllocator interface {
	allocate(ctx context.Context, tableID uint64, col string, count int, txnOp client.TxnOperator) (uint64, uint64, error)
	asyncAllocate(ctx context.Context, tableID uint64, col string, count int, txnOp client.TxnOperator, cb func(uint64, uint64, error)) error
	updateMinValue(ctx context.Context, tableID uint64, col string, minValue uint64, txnOp client.TxnOperator) error
	close()
}

// IncrValueStore is used to add and delete metadata records for auto-increment columns.
type IncrValueStore interface {
	// Exec new a txn operator, used for debug.
	NewTxnOperator(ctx context.Context) client.TxnOperator
	// SelectAll return all auto increment metadata records from catalog.AutoIncrTableName.
	SelectAll(ctx context.Context, tableID uint64, txnOp client.TxnOperator) (string, error)
	// GetColumns return auto columns of table.
	GetColumns(ctx context.Context, tableID uint64, txnOp client.TxnOperator) ([]AutoColumn, error)
	// Create add metadata records into catalog.AutoIncrTableName.
	Create(ctx context.Context, tableID uint64, cols []AutoColumn, txnOp client.TxnOperator) error
	// Allocate allocate new range for auto-increment column.
	Allocate(ctx context.Context, tableID uint64, col string, count int, txnOp client.TxnOperator) (uint64, uint64, error)
	// UpdateMinValue update auto column min value to specified value.
	UpdateMinValue(ctx context.Context, tableID uint64, col string, minValue uint64, txnOp client.TxnOperator) error
	// Delete remove metadata records from catalog.AutoIncrTableName.
	Delete(ctx context.Context, tableID uint64) error
	// Close the store
	Close()
}

// AutoColumn model
type AutoColumn struct {
	TableID  uint64
	ColName  string
	ColIndex int
	Offset   uint64
	Step     uint64
}

// GetAutoColumnFromDef get auto columns from table def
func GetAutoColumnFromDef(def *plan.TableDef) []AutoColumn {
	var cols []AutoColumn
	for i, col := range def.Cols {
		if col.Typ.AutoIncr {
			cols = append(cols, AutoColumn{
				ColName:  col.Name,
				TableID:  def.TblId,
				Step:     1,
				Offset:   def.AutoIncrOffset,
				ColIndex: i,
			})
		}
	}
	return cols
}
