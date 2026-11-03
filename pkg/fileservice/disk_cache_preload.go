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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	// preloadUsageMinSamples evictions are observed before thrashing is reported.
	preloadUsageMinSamples = 8
	// preloadUsageWeight is the weight of the newest eviction in the moving average.
	preloadUsageWeight = 0.2
	// Thrashing is reported when the average share of a whole-object file read
	// before its eviction falls below preloadThrashingShare.
	preloadThrashingShare = 0.4
	// Thrashing ends after preloadQuietPeriod without an eviction.
	preloadQuietPeriod = 30 * time.Second
)

// preloadUsage records the share of each whole-object disk-cache file that is read
// before the file is evicted, and reports thrashing while that share is low.
type preloadUsage struct {
	mu        sync.Mutex
	served    map[string]int64 // cached whole-object file -> bytes read from it
	deleting  map[string]struct{}
	average   float64
	samples   int
	thrashing bool
	lastEvict time.Time
	now       func() time.Time
}

func newPreloadUsage() *preloadUsage {
	return &preloadUsage{
		served:   make(map[string]int64),
		deleting: make(map[string]struct{}),
		average:  1,
		now:      time.Now,
	}
}

func isFullFileCachePath(diskPath string) bool {
	return strings.HasPrefix(filepath.Base(diskPath), "full")
}

// cached starts tracking a whole-object file that entered the cache.
func (p *preloadUsage) cached(diskPath string) {
	if p == nil {
		return
	}
	if !isFullFileCachePath(diskPath) {
		return
	}
	p.mu.Lock()
	p.served[diskPath] = 0
	p.mu.Unlock()
}

// read adds n bytes read from a cached whole-object file.
func (p *preloadUsage) read(diskPath string, n int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if v, ok := p.served[diskPath]; ok {
		p.served[diskPath] = v + n
	}
	p.mu.Unlock()
}

// deleted marks diskPath as leaving the cache by an explicit delete, not an eviction.
func (p *preloadUsage) deleted(diskPath string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.served, diskPath)
	p.deleting[diskPath] = struct{}{}
	p.mu.Unlock()
}

// removed records diskPath of size bytes leaving the cache.
func (p *preloadUsage) removed(diskPath string, size int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.deleting[diskPath]; ok {
		delete(p.deleting, diskPath)
		return
	}
	p.lastEvict = p.now()
	served, ok := p.served[diskPath]
	if !ok {
		return
	}
	delete(p.served, diskPath)
	if size <= 0 {
		return
	}
	share := min(float64(served)/float64(size), 1)
	p.average = (1-preloadUsageWeight)*p.average + preloadUsageWeight*share
	p.samples++
	if !p.thrashing && p.samples >= preloadUsageMinSamples && p.average < preloadThrashingShare {
		p.thrashing = true
		logutil.Info("disk cache: whole-object files are evicted before use",
			zap.Float64("average-read-share", p.average),
			zap.Int("evictions", p.samples),
		)
	}
}

// isThrashing reports whether whole-object files are evicted before most of their
// bytes are read.
func (p *preloadUsage) isThrashing() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.thrashing && p.now().Sub(p.lastEvict) > preloadQuietPeriod {
		p.thrashing = false
		p.average = 1
		p.samples = 0
		logutil.Info("disk cache: no eviction for the quiet period, whole-object files fit again")
	}
	return p.thrashing
}

type fullFilePreloadsThrashing interface {
	fullFilePreloadsThrashing() bool
}

// FullFilePreloadsSkipped reports whether reads from fs under ctx's policy skip
// whole-object preloads.
func FullFilePreloadsSkipped(ctx context.Context, fs FileService) bool {
	policy := GetFileServicePolicy(ctx)
	if policy.Any(SkipFullFilePreloads) {
		return true
	}
	if !policy.Any(NarrowRead) {
		return false
	}
	t, ok := fs.(fullFilePreloadsThrashing)
	return ok && t.fullFilePreloadsThrashing()
}
