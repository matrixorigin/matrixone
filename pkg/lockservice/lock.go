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
	"fmt"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

const (
	flagLockRow byte = 1 << iota
	flagLockRangeStart
	flagLockRangeEnd
	flagLockExclusiveMode
	flagLockSharedMode
	flagLockTableDefChanged
)

func newRangeLock(txnID []byte, opts LockOptions) (Lock, Lock) {
	l := newLock(txnID, opts)
	return l.toRangeStartLock(), l.toRangeEndLock()
}

func newRowLock(txnID []byte, opts LockOptions) Lock {
	l := newLock(txnID, opts)
	return l.toRowLock()
}

func newLock(txnID []byte, opts LockOptions) Lock {
	l := Lock{txnID: txnID}
	if opts.Mode == pb.LockMode_Exclusive {
		l.value |= flagLockExclusiveMode
	} else {
		l.value |= flagLockSharedMode
	}
	if opts.TableDefChanged {
		l.value |= flagLockTableDefChanged
	}
	return l
}

func (l Lock) toRowLock() Lock {
	l.value |= flagLockRow
	return l
}

func (l Lock) toRangeStartLock() Lock {
	l.value |= flagLockRangeStart
	return l
}

func (l Lock) toRangeEndLock() Lock {
	l.value |= flagLockRangeEnd
	return l
}

func (l Lock) isLockRow() bool {
	return l.value&flagLockRow != 0
}

func (l Lock) isLockRangeEnd() bool {
	return l.value&flagLockRangeEnd != 0
}

func (l Lock) isLockRangeStart() bool {
	return l.value&flagLockRangeStart != 0
}

func (l Lock) isLockTableDefChanged() bool {
	return l.value&flagLockTableDefChanged != 0
}

func (l Lock) getLockMode() pb.LockMode {
	if l.value&flagLockExclusiveMode != 0 {
		return pb.LockMode_Exclusive
	}
	return pb.LockMode_Shared
}

// String implement Stringer
func (l Lock) String() string {
	g := "row"
	if !l.isLockRow() {
		g = "range(start)"
		if l.isLockRangeEnd() {
			g = "range(end)"
		}
	}

	// hold txn: mode-[row|range]
	return fmt.Sprintf("%s: %s-%s",
		l.waiter.String(),
		l.getLockMode().String(),
		g)
}
