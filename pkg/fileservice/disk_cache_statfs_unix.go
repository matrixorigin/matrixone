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

//go:build darwin || linux

package fileservice

import (
	"math"

	"golang.org/x/sys/unix"
)

func statDiskCacheFilesystem(path string) (diskCacheFilesystemStat, bool) {
	var stat unix.Statfs_t
	if unix.Statfs(path, &stat) != nil || int64(stat.Bsize) <= 0 {
		return diskCacheFilesystemStat{}, false
	}
	blockSize := uint64(stat.Bsize)
	availableBlocks := uint64(stat.Bavail)
	return diskCacheFilesystemStat{
		totalBytes:     blocksToBytes(uint64(stat.Blocks), blockSize),
		availableBytes: blocksToBytes(availableBlocks, blockSize),
		totalInodes:    uint64(stat.Files),
		freeInodes:     uint64(stat.Ffree),
	}, true
}

func blocksToBytes(blocks, blockSize uint64) uint64 {
	if blocks > math.MaxUint64/blockSize {
		return math.MaxUint64
	}
	return blocks * blockSize
}
