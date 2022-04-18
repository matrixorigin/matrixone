package db

import (
	"io"
	"os"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	fname := common.MakeLockFileName(dir, LockName)
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
