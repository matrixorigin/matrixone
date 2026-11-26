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

import "testing"

// probeFill is what the probes observe of a waiting fill.
type probeFill struct {
	due   int64
	share int
	scans []*PrefetchScan
}

// probeBusy keeps the fills of fs prefetches make waiting until the returned function.
func probeBusy(t *testing.T, fs *S3FS) func() { return busyFills(t, fs) }

func probeQueued(fs *S3FS, filePath string) bool { return fs.diskCache.isQueued(filePath) }

func probeWaiting(fs *S3FS, filePath string) []probeFill {
	fills := fs.diskCache.waitingFillsOf(filePath)
	out := make([]probeFill, 0, len(fills))
	for _, q := range fills {
		out = append(out, probeFill{due: q.req.Due, share: q.req.share(), scans: q.scans})
	}
	return out
}

// probeJoined reports whether a read joined a fill of the object at filePath.
func probeJoined(fs *S3FS, filePath string) bool {
	for _, q := range fs.diskCache.waitingFillsOf(filePath) {
		if q.joiners > 0 {
			return true
		}
	}
	return false
}

// probeJoinedRunning reports whether a read joined a started fill of the object at
// filePath whose prefetch object is not registered yet.
func probeJoinedRunning(fs *S3FS, filePath string) bool {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, q := range f.fills[fillKey{diskPath: fs.diskCache.objectKey(filePath)}] {
		if q.joiners > 0 && q.pos < 0 {
			return true
		}
	}
	return false
}

func probeWaitIdle(t *testing.T, fs *S3FS) { waitFillsIdle(t, fs.diskCache) }

func probeQueueLimit(fs *S3FS) int64 { return fs.diskCache.fillQueueLimit() }

func probeFlightBytes(fs *S3FS) (bytes, background int64) {
	used := fs.server.memoryInUse()
	return used[foregroundRequest] + used[backgroundRequest], used[backgroundRequest]
}

func probeUncharge(fs *S3FS, n int64) {
	fs.server.freeMemory(n, foregroundRequest)
}

func probeUnderWay(fs *S3FS, filePath string) bool { return fs.diskCache.fillsUnderWay(filePath) }
