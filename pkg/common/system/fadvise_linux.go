// Copyright 2024 Matrix Origin
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

package system

import (
	"golang.org/x/sys/unix"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// FadviseDontNeed advises the kernel that the file data is no longer needed
// and can be evicted from page cache. This is important in cgroup-limited
// containers where page cache counts toward the memory limit.
func FadviseDontNeed(fd int) {
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
		logutil.Debugf("fadvise DONTNEED fd=%d: %v", fd, err)
	}
}
