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

package lockservice

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func newRemoteLockTable(
	binding pb.LockTable,
	detector *detector) *remoteLockTable {
	l := &remoteLockTable{binding: binding, detector: detector}
	return l
}

type remoteLockTable struct {
	binding  pb.LockTable
	detector *detector
}

// // lock attempts to add a lock to some data in a Table, either as a range or as a single lock set. Return
// 	// nil means that the locking was successful. Transaction need to be abort if any error returned.
// 	//
// 	// Possible errors returned:
// 	// 1. ErrDeadlockDetectorClosed, indicates that the current transaction has triggered a deadlock.
// 	// 2. ErrLockTableNotMatch, indicates that the LockTable binding relationship has changed.
// 	// 3. Other known errors.
// 	lock(ctx context.Context, txn *activeTxn, rows [][]byte, options LockOptions) error
// 	// Unlock release a set of locks, it will keep retrying until the context times out when it encounters an
// 	// error.
// 	unlock(ctx context.Context, ls *cowSlice) error
// 	// getLock get a lock, it will keep retrying until the context times out when it encounters an error.
// 	getLock(ctx context.Context, key []byte) (Lock, bool)
