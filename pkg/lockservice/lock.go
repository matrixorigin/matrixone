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

var (
	flagLockRow           byte = 1
	flagLockRangeStart    byte = 2
	flagLockRangeEnd      byte = 4
	flagLockExclusiveMode byte = 8
	flagLockSharedMode    byte = 16
)

func newRangeLock(txnID []byte, mode LockMode) (Lock, Lock) {
	l := newLock(txnID, mode)
	return l.toRangeStartLock(), l.toRangeEndLock()
}

func newRowLock(txnID []byte, mode LockMode) Lock {
	l := newLock(txnID, mode)
	return l.toRowLock()
}

func newLock(txnID []byte, mode LockMode) Lock {
	l := Lock{txnID: txnID}
	if mode == Exclusive {
		l.value |= flagLockExclusiveMode
	} else {
		l.value |= flagLockSharedMode
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

func (l Lock) isLockRange() bool {
	return !l.isLockRow()
}

func (l Lock) isLockRangeStart() bool {
	return l.value&flagLockRangeStart != 0
}

func (l Lock) isLockRangeEnd() bool {
	return l.value&flagLockRangeEnd != 0
}

func (l Lock) getLockMode() LockMode {
	if l.value&flagLockExclusiveMode != 0 {
		return Exclusive
	}
	return Shared
}
