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

package fileservice

import (
	"context"
	"os"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// fillTarget is the cache a fill of the disk cache publishes the file it writes into: the
// disk cache itself, or the index-file cache for the objects it keeps. The download, the
// path lock, the file written and the reads of it while it is written are the disk
// cache's either way.
type fillTarget interface {
	// heldBeforeLock reports whether the target already holds the file, before the fill
	// holds the path.
	heldBeforeLock(ctx context.Context) bool
	// held reports whether the target holds the file, while the fill holds the path.
	held(ctx context.Context) (bool, error)
	// reserve makes room for a file of size bytes about to be written.
	reserve(ctx context.Context, size int64)
	// noteWritten tells the target the size of a whole object written.
	noteWritten(size int64)
	// publish makes the file written at tempPath, of size bytes, the target's file.
	publish(ctx context.Context, tempPath string, size int64) error
}

// diskFillTarget publishes into the disk cache, at diskPath.
type diskFillTarget struct {
	d        *DiskCache
	diskPath string
}

func (t diskFillTarget) heldBeforeLock(ctx context.Context) bool {
	if _, ok := t.d.cache.Get(ctx, t.diskPath); ok {
		if _, err := os.Stat(t.diskPath); err == nil {
			return true
		}
	}
	return false
}

func (t diskFillTarget) held(ctx context.Context) (bool, error) {
	if _, ok := t.d.cache.Get(ctx, t.diskPath); ok {
		if _, err := os.Stat(t.diskPath); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
		// Repair the missing physical file and replace the existing index after the
		// rewrite so FIFO accounting uses the repaired file size.
	}
	if stat, err := os.Stat(t.diskPath); err == nil {
		t.d.cache.Set(ctx, t.diskPath, struct{}{}, fileSize(stat))
		return true, nil
	}
	return false, nil
}

func (t diskFillTarget) reserve(ctx context.Context, size int64) {
	t.d.cache.Evict(ctx, nil, size)
}

func (t diskFillTarget) noteWritten(size int64) {
	t.d.noteWholeFile(size)
}

func (t diskFillTarget) publish(ctx context.Context, tempPath string, size int64) error {
	if err := os.Rename(tempPath, t.diskPath); err != nil {
		return err
	}
	logutil.Debug("disk cache file written",
		zap.Any("path", t.diskPath),
	)
	if !t.d.cache.Replace(ctx, t.diskPath, struct{}{}, size) {
		t.d.cache.Set(ctx, t.diskPath, struct{}{}, size)
	}
	if !t.d.cache.Contains(t.diskPath) {
		if err := os.Remove(t.diskPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
