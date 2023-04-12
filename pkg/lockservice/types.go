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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

var (
	// ErrDeadlockDetectorClosed deadlock detector is closed
	ErrDeadlockDetectorClosed = moerr.NewInvalidStateNoCtx("deadlock detector is closed")
	// ErrDeadLockDetected dead lock detected
	ErrDeadLockDetected = moerr.NewDeadLockDetectedNoCtx()
	// ErrLockTableBindChanged lock table and lock service bind changed
	ErrLockTableBindChanged = moerr.NewLockTableBindChangedNoCtx()
	// ErrLockTableNotFound lock table not found on remote lock service
	ErrLockTableNotFound = moerr.NewLockTableNotFoundNoCtx()
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
	Delete(key []byte) (Lock, bool)
	// Seek returns the first KV Pair that is >= the given key
	Seek(key []byte) ([]byte, Lock, bool)
	// Prev returns the first KV Pair that is < the given key
	Prev(key []byte) ([]byte, Lock, bool)
	// Range range in [start, end)
	Range(start []byte, end []byte, fn func([]byte, Lock) bool)
	// Iter iter all values
	Iter(func([]byte, Lock) bool)
	// Clear clear the lock
	Clear()
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
	// GetConfig returns the lockservice config
	GetConfig() Config
	// Lock locks rows(row or row range determined by the Granularity in options) a table. Lockservice
	// has no requirement for the format of rows, but requires all rows of a table on a lockservice
	// to be sortable.
	//
	// If a conflict is encountered, the method will block until the conflicting lock is
	// released and held by the current operation, or until it times out.
	//
	// Returns false if conflicts are encountered in FastFail wait policy and ErrDeadLockDetected
	// returns if current operation was aborted by deadlock detection.
	Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (pb.Result, error)
	// Unlock release all locks associated with the transaction. If commitTS is not empty, means
	// the txn was committed.
	Unlock(ctx context.Context, txnID []byte, commitTS timestamp.Timestamp) error

	// Close close the lock service.
	Close() error

	// Observability methods

	// GetWaitingList get specical txnID's waiting list
	GetWaitingList(ctx context.Context, txnID []byte) (bool, []pb.WaitTxn, error)
	// ForceRefreshLockTableBinds force refresh all lock tables binds
	ForceRefreshLockTableBinds()
	// GetLockTableBind returns lock table bind
	GetLockTableBind(tableID uint64) (pb.LockTable, error)
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
	lock(ctx context.Context, txn *activeTxn, rows [][]byte, options LockOptions) (pb.Result, error)
	// Unlock release a set of locks, if txn was committed, commitTS is not empty
	unlock(txn *activeTxn, ls *cowSlice, commitTS timestamp.Timestamp)
	// getLock get a lock
	getLock(txnID, key []byte, fn func(Lock))
	// getBind returns lock table binding
	getBind() pb.LockTable
	// close close the locktable
	close()
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
	// KeepLockTableBind once a cn is bound to a Table, a heartbeat needs to be sent
	// periodically to keep the binding in place. If no heartbeat is sent for a long
	// period of time to maintain the binding, the binding will become invalid.
	KeepLockTableBind(serviceID string) bool
	// Valid check for changes in the binding relationship of a specific locktable.
	Valid(binds []pb.LockTable) bool
	// Close close the lock table allocator
	Close() error
}

// LockTableKeeper is used to keep a heartbeat with the LockTableAllocator to keep the
// LockTable bind. And get the changed info of LockTable and LockService bind.
type LockTableKeeper interface {
	// Close close the keeper
	Close() error
}

// Client is used to send lock table operations to other service.
// 1. lock service <-> lock service
// 2. lock service <-> lock table allocator
type Client interface {
	// Send send request to other lock service, and wait for a response synchronously.
	Send(context.Context, *pb.Request) (*pb.Response, error)
	// AsyncSend async send request to other lock service.
	AsyncSend(context.Context, *pb.Request) (*morpc.Future, error)
	// Close close the client
	Close() error
}

// RequestHandleFunc request handle func
type RequestHandleFunc func(context.Context, *pb.Request, *pb.Response) error

// ServerOption server option
type ServerOption func(*server)

// Server receives and processes requests from Client.
type Server interface {
	// Start start the txn server
	Start() error
	// Close the txn server
	Close() error
	// RegisterMethodHandler register txn request handler func
	RegisterMethodHandler(pb.Method, RequestHandleFunc)
}

// LockOptions options for lock
type LockOptions = pb.LockOptions

// Lock stores specific lock information. Since there are a large number of lock objects
// in the LockStorage at runtime, this object has been specially designed to save memory
// usage.
type Lock struct {
	txnID []byte
	// all lock info will encode into this field to save memory overhead
	value  byte
	waiter *waiter
}
