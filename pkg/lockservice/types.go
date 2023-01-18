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

package lockservice

import (
	"context"
)

// Granularity row granularity, single row or row range
type Granularity int

const (
	// Row single row
	Row Granularity = iota
	// Range row range mode
	Range
)

// LockMode exclusive or shared lock
type LockMode int

const (
	// Exclusive mode
	Exclusive LockMode = iota
	// Shared mode
	Shared
)

// WaitPolicy waiting strategy if lock conflicts are encountered when locking.
type WaitPolicy int

const (
	// Wait waiting for conflicting locks to be released
	Wait WaitPolicy = iota
	// FastFail return fail if lock conflicts are encountered
	FastFail
)

// LockStorage the store that holds the locks, a storage instance is corresponding to
// all the locks of a table. The LockStorage no need to be thread-safe.
//
// All locks are stored in an orderly in the LockStorage, so lock conflicts
// can be easily detected.
type LockStorage interface {
	// Add we use kv to store the lock. Key is a locked row or a row range. Value is the
	// TxnID.
	Add(key []byte, value Lock)
	// Get returns the value of the given key
	Get(key []byte) (Lock, bool)
	// Len returns number of the locks in the storage
	Len() int
	// Delete delete lock from the storage
	Delete(key []byte)
	// Seek returns the first KV Pair that is >= the given key
	Seek(key []byte) ([]byte, Lock, bool)
}

// LockService lock service is running at the CN node. The lockservice maintains a set
// of LockStorage internally (one table corresponds to one LockStorage instance).
// All Lock and Unlock operations on each Table are concurrent.
//
// Lock waiting is implemented as fair, internally there is a waiting queue for each
// Lock and when a Lock is released, a new Lock is executed in a FIFO fashion. And the
// element in the wait queue is the transaction ID.
//
// The current lock waiting mechanism will trigger deadlock, so lockservice has implemented
// a deadlock detection mechanism internally. In order to ensure the performance of Lock
// operations, we cannot synchronise deadlock detection with each Lock operation.
// The current implementation is that when a new waiter is added to the wait queue of any
// Lock, a set of background goroutines are notified to start a deadlock detection for all
// transactions in the Lock's wait queue.
type LockService interface {
	// Lock locks rows(row or row range determined by the Granularity in options) a table. Lockservice
	// has no requirement for the format of rows, but requires all rows of a table on a lockservice
	// to be sortable.
	//
	// If a conflict is encountered, the method will block until the conflicting lock is
	// released and held by the current operation, or until it times out.
	//
	// Returns false if conflicts are encountered in FastFail wait policy and ErrDeadLockDetected
	// returns if current operation was aborted by deadlock detection.
	Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (bool, error)
	// Unlock release all locks associated with the transaction.
	Unlock(txnID []byte) error
	// Close close the lock service.
	Close() error
}

// LockOptions options for lock
type LockOptions struct {
	granularity Granularity
	mode        LockMode
	policy      WaitPolicy
}

// Lock stores specific lock information. Since there are a large number of lock objects
// in the LockStorage at runtime, this object has been specially designed to save memory
// usage.
type Lock struct {
	txnID []byte
	// all lock info will encode into this field to save memory overhead
	value  byte
	waiter *waiter
}
