// Copyright 2021 Matrix Origin
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

package db

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

const (
	LockName string = "TAE"
)

// POSIX record locks are process-scoped. Reserve the path before opening
// another descriptor, since closing any descriptor can release those locks.
var dbWriterLocks sync.Map

type dbWriterLock struct {
	path string
	file *os.File
}

func (lock *dbWriterLock) Close() error {
	err := lock.file.Close()
	dbWriterLocks.CompareAndDelete(lock.path, lock)
	return err
}

// createDBLock serializes writers both within and across processes.
func createDBLock(dir string) (_ io.Closer, err error) {
	if err = os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	fname := dbutils.MakeLockFileName(dir, LockName)
	lock := &dbWriterLock{path: fname}
	if _, loaded := dbWriterLocks.LoadOrStore(fname, lock); loaded {
		return nil, moerr.NewInternalErrorNoCtxf("TAE writer still owns %q", dir)
	}
	defer func() {
		if err != nil {
			dbWriterLocks.CompareAndDelete(fname, lock)
		}
	}()
	f, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
		Pid:    int32(os.Getpid()),
	}
	if err = syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &flockT); err != nil {
		logutil.Errorf("error locking file: %s", err)
		f.Close()
		return nil, err
	}
	lock.file = f
	return lock, nil
}

// closeDBLock keeps the writer fenced while a timed-out WAL shutdown drains.
func closeDBLock(lock io.Closer, store wal.Store) error {
	if lock == nil {
		return nil
	}
	if async, ok := store.(interface{ ShutdownDone() <-chan struct{} }); ok {
		if done := async.ShutdownDone(); done != nil {
			select {
			case <-done:
			default:
				go func() {
					<-done
					if err := lock.Close(); err != nil {
						logutil.Errorf("release TAE writer lock after WAL shutdown: %v", err)
					}
				}()
				return nil
			}
		}
	}
	return lock.Close()
}
