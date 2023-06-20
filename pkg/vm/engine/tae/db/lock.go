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
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
)

const (
	LockName string = "TAE"
)

// createDBLock creates a file lock on TAE's working directory.
func createDBLock(dir string) (io.Closer, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	fname := dbutils.MakeLockFileName(dir, LockName)
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
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &flockT); err != nil {
		logutil.Errorf("error locking file: %s", err)
		f.Close()
		return nil, err
	}
	return f, nil
}
