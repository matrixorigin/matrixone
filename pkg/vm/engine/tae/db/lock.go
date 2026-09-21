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
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
)

const (
	LockName string = "TAE"
)

type dbDirectoryLock struct {
	legacy *os.File
	owner  *os.File
	once   sync.Once
	err    error
}

func (l *dbDirectoryLock) Close() error {
	l.once.Do(func() {
		// Keep flock ownership until the legacy process-scoped lock is gone.
		l.err = errors.Join(l.legacy.Close(), l.owner.Close())
	})
	return l.err
}

// createDBLock creates a file lock on TAE's working directory.
func createDBLock(dir string) (io.Closer, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	fname := dbutils.MakeLockFileName(dir, LockName)
	// POSIX record locks are process-owned: another open in this process can
	// reacquire them, and closing either descriptor releases the first lock.
	// Lock a separate inode before even opening the legacy inode. flock is
	// descriptor-owned and therefore rejects same-process duplicate opens too.
	// Never unlink either lock file: that would permit owners of two inodes.
	owner, err := os.OpenFile(fname+".owner", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	f, err := os.Create(fname)
	if err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
		Pid:    int32(os.Getpid()),
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &flockT); err != nil {
		logutil.Errorf("error locking file: %s", err)
		return nil, errors.Join(err, f.Close(), owner.Close())
	}
	return &dbDirectoryLock{legacy: f, owner: owner}, nil
}
