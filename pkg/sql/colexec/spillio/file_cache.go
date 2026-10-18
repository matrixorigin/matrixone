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
	written   int64
	submitted int64
	dropped   int64
	retained  int64
}

// NewSequentialWriteCache uses a larger window only when its caller has
// admitted the full capacity. The zero value keeps the conservative default.
func NewSequentialWriteCache(retained int64) SequentialWriteCache {
	pageSize := int64(os.Getpagesize())
	if retained < retainedWriteCacheBytes || retained%(2*pageSize) != 0 {
		return SequentialWriteCache{}
	}
	return SequentialWriteCache{retained: retained}
}

func (c *SequentialWriteCache) window() int64 {
	if c.retained == 0 {
		return retainedWriteCacheBytes
	}
	return c.retained
}

// RecordWrite advances one successful sequential write. Small appends accumulate
// before submitting a contiguous batch for writeback. Keep a second batch in the
// bounded window so writing can overlap the previous batch's I/O, and wait/drop
// only when reclaiming an older batch is necessary to maintain the bound.
func (c *SequentialWriteCache) RecordWrite(file *os.File, size int) error {
	if c == nil || file == nil || size <= 0 || c.written < 0 || c.dropped < 0 ||
		c.submitted < c.dropped || c.submitted > c.written ||
		int64(size) > math.MaxInt64-c.written {
		return errInvalidSequentialWrite
	}
	c.written += int64(size)
	retained := c.window()
	batchBytes := retained / 2
	submitBytes := batchBytes
	// Start I/O before the full retained window is populated. Reclamation
	// still uses half-window batches, independently of writeback submission.
	if submitBytes > 512<<10 {
		submitBytes = 512 << 10
	}
	pageSize := int64(os.Getpagesize())
	if c.written-c.submitted >= submitBytes {
		submitEnd := c.written - c.written%pageSize
		startWriteback(file, c.submitted, submitEnd-c.submitted)
		c.submitted = submitEnd
	}
	if c.written-c.dropped <= retained {
		return nil
	}
	// DONTNEED ignores partial pages. Advance only over whole pages so an
	// unaligned record boundary cannot strand a cached page at every flush.
	// Round up to reclaim a whole batch rather than waiting on every append.
	dropEnd := c.written - retained
	if remainder := dropEnd % batchBytes; remainder != 0 {
		dropEnd += batchBytes - remainder
	}
	finishWritebackAndDrop(file, c.dropped, dropEnd-c.dropped)
	c.dropped = dropEnd
	return nil
}

// SubmitTail starts the remaining writeback after the final append. A caller
// with several files can submit all tails before waiting in Finish, allowing
// their I/O to overlap instead of serializing one small file at a time.
func (c *SequentialWriteCache) SubmitTail(file *os.File) {
	if c == nil || file == nil || c.submitted >= c.written {
		return
	}
	startWriteback(file, c.submitted, c.written-c.submitted)
	c.submitted = c.written
}

// Finish makes the complete written range reclaimable before ownership moves
// to a reader. It is idempotent.
func (c *SequentialWriteCache) Finish(file *os.File) {
	if c == nil || file == nil || c.dropped >= c.written {
		return
	}
	finishWritebackAndDrop(file, c.dropped, c.written-c.dropped)
	c.submitted = c.written
	c.dropped = c.written
}

// DropReadCache releases clean pages after a spill reader has consumed a file.
// It is advisory and never changes query correctness.
func DropReadCache(file *os.File) {
	if file != nil {
		dropCleanPages(file, 0, 0)
	}
}
