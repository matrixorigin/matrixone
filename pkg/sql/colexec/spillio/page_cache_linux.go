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

//go:build linux

package spillio

import (
	"os"

	"golang.org/x/sys/unix"
)

func startWriteback(file *os.File, offset, length int64) {
	_ = unix.SyncFileRange(
		int(file.Fd()), offset, length, unix.SYNC_FILE_RANGE_WRITE,
	)
}

func finishWritebackAndDrop(file *os.File, offset, length int64) {
	if length <= 0 {
		return
	}
	if err := unix.SyncFileRange(
		int(file.Fd()),
		offset,
		length,
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|
			unix.SYNC_FILE_RANGE_WRITE|
			unix.SYNC_FILE_RANGE_WAIT_AFTER,
	); err != nil {
		return
	}
	dropCleanPages(file, offset, length)
}

func dropCleanPages(file *os.File, offset, length int64) {
	_ = unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_DONTNEED)
}
