// Copyright 2026 Matrix Origin
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
	"os/exec"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/stretchr/testify/require"
)

func TestDBDirectoryLockExcludesSameProcessAndLegacyWriter(t *testing.T) {
	dir := t.TempDir()
	lock, err := createDBLock(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	duplicate, err := createDBLock(dir)
	if duplicate != nil {
		t.Cleanup(func() { _ = duplicate.Close() })
	}
	require.Error(t, err)
	require.Nil(t, duplicate)
	run := func(mode string) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDBDirectoryLockHelper$")
		cmd.Env = append(os.Environ(), "MO_TEST_TAE_LOCK_MODE="+mode, "MO_TEST_TAE_LOCK_DIR="+dir)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s: %s", mode, output)
	}
	// The failed same-process attempt must not close a descriptor for the
	// original fcntl inode and accidentally release compatibility exclusion.
	run("legacy-blocked")
	run("blocked")
	require.NoError(t, lock.Close())
	require.NoError(t, lock.Close())
	run("acquired")
}

func TestDBDirectoryLockHelper(t *testing.T) {
	mode := os.Getenv("MO_TEST_TAE_LOCK_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("MO_TEST_TAE_LOCK_DIR")
	if mode == "legacy-blocked" {
		f, err := os.OpenFile(dbutils.MakeLockFileName(dir, LockName), os.O_RDWR, 0)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		err = syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &syscall.Flock_t{
			Type: syscall.F_WRLCK, Whence: io.SeekStart,
		})
		require.Error(t, err, "legacy writer must still be fenced")
		return
	}
	lock, err := createDBLock(dir)
	if lock != nil {
		t.Cleanup(func() { require.NoError(t, lock.Close()) })
	}
	switch mode {
	case "blocked":
		require.Error(t, err)
	case "acquired":
		require.NoError(t, err)
	default:
		t.Fatalf("unexpected lock mode %q", mode)
	}
}
