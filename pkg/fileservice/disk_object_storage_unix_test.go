//go:build darwin || linux

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDiskObjectStorageReadInvalidRangeClosesFile(t *testing.T) {
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	storage := &diskObjectStorage{path: t.TempDir()}
	const key = "object"
	require.NoError(t, os.WriteFile(filepath.Join(storage.path, key), []byte("data"), 0644))

	minusOne := int64(-1)
	zero := int64(0)
	one := int64(1)
	two := int64(2)
	tests := []struct {
		name        string
		min         *int64
		max         *int64
		expectEmpty bool
	}{
		{name: "equal bounds", min: &one, max: &one, expectEmpty: true},
		{name: "inverted bounds", min: &two, max: &one, expectEmpty: true},
		{name: "non-positive upper bound", max: &zero, expectEmpty: true},
		{name: "seek failure", min: &minusOne},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := openFileDescriptorCount(t)
			for range 16 {
				reader, err := storage.Read(context.Background(), key, tc.min, tc.max)
				require.Nil(t, reader)
				require.Error(t, err)
				if tc.expectEmpty {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))
				}
			}
			require.Equal(t, before, openFileDescriptorCount(t))
		})
	}
}

func openFileDescriptorCount(t *testing.T) int {
	t.Helper()

	var limit unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &limit))
	const maxDescriptorsToInspect = uint64(1 << 16)
	if limit.Cur > maxDescriptorsToInspect {
		limit.Cur = maxDescriptorsToInspect
	}

	count := 0
	for fd := uint64(0); fd < limit.Cur; fd++ {
		for {
			_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err == nil {
				count++
			} else if !errors.Is(err, unix.EBADF) {
				t.Fatalf("inspect file descriptor %d: %v", fd, err)
			}
			break
		}
	}
	return count
}
