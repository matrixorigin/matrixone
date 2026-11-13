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

// wholeObjectSizeEstimate is the size of a whole-object file assumed before the disk
// cache has written one.
const wholeObjectSizeEstimate = 128 << 20

const (
	// A group is thrashing when the average share of its whole-object files read
	// before their eviction falls below preloadUnusedShare over at least
	// preloadUsageMinSamples evictions, or below preloadThrashingShare over at least
	// preloadUsageMoreSamples evictions.
	preloadUsageMinSamples  = 4
	preloadUsageMoreSamples = 16
	preloadUnusedShare      = 0.1
	preloadThrashingShare   = 0.4
	// preloadUsageWeight is the weight of the most recently completed sample.
	preloadUsageWeight = 0.2
	// A group's thrashing ends after its quiet period without an eviction of its
	// whole-object files, once another whole object fits in the disk cache. The
	// period starts at preloadQuietPeriod and doubles, up to
	// preloadQuietPeriodMax, each time the group thrashes again within twice the
	// period after it ended.
	preloadQuietPeriod    = 30 * time.Second
	preloadQuietPeriodMax = 10 * time.Minute
	// preloadObjectsMax objects are mapped to their group at most.
	preloadObjectsMax = 1 << 16
	// preloadGroupsMax groups are recorded at most. When it is reached, groups idle
	// for twice their quiet period are forgotten, and a new group is not recorded
	// while none is.
	preloadGroupsMax = 1 << 12
)

type contextKeyPreloadGroup struct{}

// WithPreloadGroup attaches the group, a table, whose reads ctx carries. The disk
// cache decides whole-object preloads of narrow reads per group.
func WithPreloadGroup(ctx context.Context, group uint64) context.Context {
	return context.WithValue(ctx, contextKeyPreloadGroup{}, group)
}

// GetPreloadGroup returns the group stored in ctx, or 0, no group, if none.
func GetPreloadGroup(ctx context.Context) uint64 {
	if g, ok := ctx.Value(contextKeyPreloadGroup{}).(uint64); ok {
		return g
	}
	return 0
}

type groupUsage struct {
	average   float64
	samples   int
	epoch     uint64
	thrashing bool
	lastEvict time.Time     // last eviction of one of the group's whole-object files
	ended     time.Time     // when thrashing last ended
	quiet     time.Duration // the quiet period that ends thrashing
}

// preloadUsage records the share of each whole-object disk-cache file that is read
// before the file is evicted, and reports per group whether that share is low.
type pendingPreloadRemoval struct {
	size      int64
	served    int64
	group     uint64
	usage     *groupUsage
	epoch     uint64
	evictedAt time.Time
}

// preloadUsage records the share of each whole-object disk-cache file that is read
// before the file is evicted, and reports per group whether that share is low.
type preloadUsage struct {
	mu        sync.Mutex
	served    map[string]int64 // cached whole-object file -> bytes read from it
	deleting  map[string]struct{}
	objects   map[string]uint64 // whole-object file -> group of the narrow reads of it
	downloads map[string]*objectDownload
	pending   map[*objectDownload]*pendingPreloadRemoval
	groups    map[uint64]*groupUsage
	now       func() time.Time
}

func newPreloadUsage() *preloadUsage {
	return &preloadUsage{
		served:    make(map[string]int64),
		deleting:  make(map[string]struct{}),
		objects:   make(map[string]uint64),
		downloads: make(map[string]*objectDownload),
		pending:   make(map[*objectDownload]*pendingPreloadRemoval),
		groups:    make(map[uint64]*groupUsage),
		now:       time.Now,
	}
}

func isFullFileCachePath(diskPath string) bool {
	return strings.HasPrefix(filepath.Base(diskPath), "full")
}

// cached starts tracking a whole-object file that entered the cache.
func (p *preloadUsage) cached(diskPath string) {
	p.cachedDownload(diskPath, nil)
}

// cachedDownload associates a cached file with its download generation. Credits
// from an older download can then never be applied to a newer file at the same path.
func (p *preloadUsage) cachedDownload(diskPath string, o *objectDownload) {
	if p == nil {
		return
	}
	if !isFullFileCachePath(diskPath) {
		return
	}
	p.mu.Lock()
	p.served[diskPath] = 0
	if o == nil {
		delete(p.downloads, diskPath)
	} else {
		p.downloads[diskPath] = o
	}
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

// readDownload adds bytes to the download generation that supplied them. Once
// that generation is evicted, its pending record replaces the path-based entry
// until its active readers finish.
func (p *preloadUsage) readDownload(diskPath string, o *objectDownload, n int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if pending := p.pending[o]; pending != nil {
		pending.served += n
	} else if p.downloads[diskPath] == o {
		if v, ok := p.served[diskPath]; ok {
			p.served[diskPath] = v + n
		}
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
	delete(p.objects, diskPath)
	delete(p.downloads, diskPath)
	p.deleting[diskPath] = struct{}{}
	p.mu.Unlock()
}

// removed records diskPath of size bytes leaving the cache. A download with
// active readers keeps a generation-owned pending record until they finish, so
// their successful reads are included and failed reads are excluded.
func (p *preloadUsage) removed(diskPath string, size int64) {
	if p == nil {
		return
	}
	o := p.prepareRemoval(diskPath, size)
	if o != nil {
		o.afterReads(func() { p.finishRemoval(o) })
	}
}

// prepareRemoval detaches the current path generation. If it belongs to a
// download, the returned object owns the deferred final sample.
func (p *preloadUsage) prepareRemoval(diskPath string, size int64) *objectDownload {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.deleting[diskPath]; ok {
		delete(p.deleting, diskPath)
		return nil
	}
	served, ok := p.served[diskPath]
	if !ok {
		return nil
	}
	delete(p.served, diskPath)
	group, hasGroup := p.objects[diskPath]
	delete(p.objects, diskPath)
	o := p.downloads[diskPath]
	delete(p.downloads, diskPath)
	if !hasGroup || size <= 0 {
		return nil
	}
	evictedAt := p.now()
	usage := p.observeEviction(group, evictedAt)
	if usage == nil {
		return nil
	}
	if o == nil {
		p.recordRemoval(usage, served, size, group, evictedAt)
		return nil
	}
	p.pending[o] = &pendingPreloadRemoval{
		size: size, served: served, group: group, usage: usage,
		epoch: usage.epoch, evictedAt: evictedAt,
	}
	return o
}

func (p *preloadUsage) finishRemoval(o *objectDownload) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := p.pending[o]
	delete(p.pending, o)
	if pending == nil || p.groups[pending.group] != pending.usage || pending.usage.epoch != pending.epoch {
		return
	}
	p.recordRemoval(pending.usage, pending.served, pending.size, pending.group, pending.evictedAt)
}

// observeEviction records when the eviction happened, even if its sample must
// wait for an admitted reader. p.mu must be held.
func (p *preloadUsage) observeEviction(group uint64, at time.Time) *groupUsage {
	g := p.groups[group]
	if g == nil {
		if len(p.groups) >= preloadGroupsMax {
			p.forgetIdleGroups(at)
			if len(p.groups) >= preloadGroupsMax {
				return nil
			}
		}
		g = &groupUsage{quiet: preloadQuietPeriod}
		p.groups[group] = g
	}
	if at.After(g.lastEvict) {
		g.lastEvict = at
	}
	return g
}

// recordRemoval records one final read-share sample; p.mu must be held.
func (p *preloadUsage) recordRemoval(g *groupUsage, served, size int64, group uint64, at time.Time) {
	share := min(float64(served)/float64(size), 1)
	if g.samples == 0 {
		g.average = share
	} else {
		g.average = (1-preloadUsageWeight)*g.average + preloadUsageWeight*share
	}
	g.samples++
	if !g.thrashing && (g.samples >= preloadUsageMinSamples && g.average < preloadUnusedShare ||
		g.samples >= preloadUsageMoreSamples && g.average < preloadThrashingShare) {
		g.thrashing = true
		if !g.ended.IsZero() && at.Sub(g.ended) < 2*g.quiet {
			g.quiet = min(2*g.quiet, preloadQuietPeriodMax)
		} else {
			g.quiet = preloadQuietPeriod
		}
		logutil.Info("disk cache: whole-object files of a group are evicted before use",
			zap.Uint64("group", group),
			zap.Float64("average-read-share", g.average),
			zap.Int("evictions", g.samples),
			zap.Duration("quiet-period", g.quiet),
		)
	}
}

// forgetIdleGroups forgets the groups with no eviction, and no end of thrashing,
// for twice their quiet period.
func (p *preloadUsage) forgetIdleGroups(now time.Time) {
	for group, g := range p.groups {
		last := g.lastEvict
		if g.ended.After(last) {
			last = g.ended
		}
		if now.Sub(last) > 2*g.quiet {
			delete(p.groups, group)
		}
	}
}

// narrowRead maps diskPath to group and reports whether group's whole-object
// files are evicted before most of their bytes are read. Group 0 is no group.
// Thrashing ends only when fits reports room for another whole object: a group
// that reads ranges evicts no whole-object file, whether or not its objects fit.
func (p *preloadUsage) narrowRead(diskPath string, group uint64, fits func() bool) bool {
	if p == nil || group == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.objects[diskPath]; !ok && len(p.objects) >= preloadObjectsMax {
		for path := range p.objects {
			delete(p.objects, path)
			break
		}
	}
	p.objects[diskPath] = group
	g := p.groups[group]
	if g == nil {
		return false
	}
	if now := p.now(); g.thrashing && now.Sub(g.lastEvict) > g.quiet && fits() {
		g.thrashing = false
		g.ended = now
		g.samples = 0
		g.epoch++
		logutil.Info("disk cache: no eviction of a group's whole-object files for the quiet period and room for one, they fit again",
			zap.Uint64("group", group),
			zap.Duration("quiet-period", g.quiet),
		)
	}
	return g.thrashing
}

// WholeObjectPreloads is a file service whose disk cache decides whole-object
// preloads of narrow reads.
type WholeObjectPreloads interface {
	WholeObjectPreloadSkipped(filePath string, group uint64) bool
}

// FullFilePreloadSkipped reports whether a read of filePath from fs under policy,
// for ctx's group, skips the whole-object preload.
func FullFilePreloadSkipped(ctx context.Context, fs FileService, policy Policy, filePath string) bool {
	if policy.Any(SkipFullFilePreloads) {
		return true
	}
	if !policy.Any(NarrowRead) {
		return false
	}
	w, ok := fs.(WholeObjectPreloads)
	return ok && w.WholeObjectPreloadSkipped(filePath, GetPreloadGroup(ctx))
}

// wholeObjectFits reports whether a whole-object file, sized as the largest one written,
// fits in the disk cache beside the cached files and the whole-object fills in progress.
func (d *DiskCache) wholeObjectFits() bool {
	size := d.wholeFileMax.Load()
	if size <= 0 {
		size = wholeObjectSizeEstimate
	}
	return d.cache.Used()+(d.wholeFills.Load()+1)*size <= d.capacityFunc()
}

// noteWholeFile records the size of a whole-object file written to the cache.
func (d *DiskCache) noteWholeFile(size int64) {
	for {
		cur := d.wholeFileMax.Load()
		if size <= cur || d.wholeFileMax.CompareAndSwap(cur, size) {
			return
		}
	}
}
