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

package spillio

import (
	"math"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const retainedWriteCacheBytes = int64(256 << 10)

var errInvalidSequentialWrite = moerr.NewInternalErrorNoCtx("invalid sequential spill write")

// SequentialWriteCache bounds clean and writeback page-cache residency for one
// append-only spill file. Spill durability across a process crash is not part
// of the contract; completed ranges only need to become clean before the
// kernel is advised that their cache pages can be reclaimed.
type SequentialWriteCache struct {
	written int64
	dropped int64
}

// RecordWrite advances one successful sequential write. The newest bounded
// tail remains cached so small appends can coalesce in the kernel; older ranges
// are written back and made immediately reclaimable.
func (c *SequentialWriteCache) RecordWrite(file *os.File, size int) error {
	if c == nil || file == nil || size <= 0 || c.written < 0 || c.dropped < 0 ||
		c.dropped > c.written || int64(size) > math.MaxInt64-c.written {
		return errInvalidSequentialWrite
	}
	start := c.written
	c.written += int64(size)
	startWriteback(file, start, int64(size))
	if c.written-c.dropped <= retainedWriteCacheBytes {
		return nil
	}
	dropEnd := c.written - retainedWriteCacheBytes
	finishWritebackAndDrop(file, c.dropped, dropEnd-c.dropped)
	c.dropped = dropEnd
	return nil
}

// Finish makes the complete written range reclaimable before ownership moves
// to a reader. It is idempotent.
func (c *SequentialWriteCache) Finish(file *os.File) {
	if c == nil || file == nil || c.dropped >= c.written {
		return
	}
	finishWritebackAndDrop(file, c.dropped, c.written-c.dropped)
	c.dropped = c.written
}

// DropReadCache releases clean pages after a spill reader has consumed a file.
// It is advisory and never changes query correctness.
func DropReadCache(file *os.File) {
	if file != nil {
		dropCleanPages(file, 0, 0)
	}
}
