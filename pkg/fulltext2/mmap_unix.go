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

//go:build darwin || linux

package fulltext2

import (
	"os"
	"syscall"
)

var munmapFn = syscall.Munmap

// mmapReadOnly maps the whole file into a shared, read-only region. Normal
// loading creates one mapping per Segment; the experimental file pool creates
// one independent mapping per lease while the kernel still shares the backing
// OS page cache. Reads are plain memory loads and the cache's Segment lifetime
// controls munmap. Returns nil for a zero-length file.
func mmapReadOnly(f *os.File) ([]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() == 0 {
		return nil, nil
	}
	return syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
}

// munmap releases a mapping from mmapReadOnly (no-op on nil/empty).
func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return munmapFn(b)
}
