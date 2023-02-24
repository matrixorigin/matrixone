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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
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
	Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) error
	// Unlock release all locks associated with the transaction.
	Unlock(txnID []byte) error
	// Close close the lock service.
	Close() error
}

// lockTable is used to manage all locks of a Table. LockTable can be local or remote, as determined
// by LockTableAllocator.
//
// A LockService instance contains multiple LockTables (either local or remote), and forLockService holder
// does not know which LockTable is local and which is remote, these details are fully implemented inside
// the LockService.
//
// TODO(fagongzi): If a Table has multiple partitions, then these Partitions are inside the LockTable and
// all operations can be parallel.
type lockTable interface {
	// lock attempts to add a lock to some data in a Table, either as a range or as a single lock set. Return
	// nil means that the locking was successful. Transaction need to be abort if any error returned.
	//
	// Possible errors returned:
	// 1. ErrDeadlockDetectorClosed, indicates that the current transaction has triggered a deadlock.
	// 2. ErrLockTableNotMatch, indicates that the LockTable binding relationship has changed.
	// 3. Other known errors.
	lock(ctx context.Context, txn *activeTxn, rows [][]byte, options LockOptions) error
	// Unlock release a set of locks, it will keep retrying until the context times out when it encounters an
	// error.
	unlock(ctx context.Context, ls *cowSlice) error
	// getLock get a lock, it will keep retrying until the context times out when it encounters an error.
	getLock(ctx context.Context, key []byte) (Lock, bool)
}

// LockTableAllocator is used to managing the binding relationship between
// LockTable and LockService, and check the validity of the binding held by
// the transaction when the transaction is committed.
//
// A LockTable will only be bound by a LockService, and once the binding
// relationship between a LockTable and a LockService changes, the binding
// version will also change. Once a LockService goes offline (crash or network
// partition), the LockTable is bound to another LockService.
//
// During the [Txn-Lock, Txn-Commit] time period, if the binding between LockTable
// and LockService changes, we need to be able to detect it and get the transaction
// rolled back, because the Lock acquired by this transaction is not valid and cannot
// resolve W-W conflicts.
type LockTableAllocator interface {
	// Get get the original LockTable data corresponding to a Table. If there is no
	// corresponding binding, then the CN binding of the current request will be used.
	Get(serviceID string, tableID uint64) pb.LockTable
	// Keepalive once a cn is bound to a Table, a heartbeat needs to be sent periodically
	// to keep the binding in place. If no heartbeat is sent for a long period of time
	// to maintain the binding, the binding will become invalid.
	Keepalive(serviceID string) bool
	// Valid check for changes in the binding relationship of a specific locktable.
	Valid(binds []pb.LockTable) bool
	// Close close the lock table allocator
	Close() error
}

// LockTableKeeper is used to keep a heartbeat with the LockTableAllocator to keep the
// LockTable bind. And get the changed info of LockTable and LockService bind.
type LockTableKeeper interface {
	// Add add a new LockTable to keepalive.
	Add(pb.LockTable)
	// Changed lock table bind changed notify, if a lock table bind changed, all related
	// transactions must be abort
	Changed() chan pb.LockTable
	// Close close the keeper
	Close() error
}

// KeepaliveSender is used to send keepalive message to LockTableAllocator.
type KeepaliveSender interface {
	// Keep send locktables keepalive messages, if lockTable version changed, the return
	// []pb.LockTable will include these.
	Keep(context.Context, []pb.LockTable) ([]pb.LockTable, error)
	// Close close the sender
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
