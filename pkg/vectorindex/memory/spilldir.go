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

package memory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// localSpillSubdir is the vector index's own scratch directory under the LOCAL
// fileservice — a sibling of the JOIN spill's "__spill" and fulltext2's
// "__fulltext2", deliberately not shared with either so one feature's cleanup
// cannot delete another's in-flight files.
const localSpillSubdir = "__vectorindex"

// HostSpillDir returns the on-disk scratch directory for GPU index build
// artifacts, under the LOCAL fileservice, creating it if absent.
//
// WHY NOT $TMPDIR: a GPU index build packs each finished sub-index to a tar
// before freeing its device memory, so the bytes written here are the whole
// index — at 88M rows with m=192 that is ~17.6 GB per build. /tmp is frequently
// a small or slow mount (on AWS it is the ~128 MB/s root volume, which would add
// minutes of pure write stall), while the LOCAL fileservice is by definition the
// data directory the operator provisioned for exactly this kind of traffic.
// fulltext2 moved its spill for the same reason; see HostSpillDir there.
//
// Returns "" when rootFS has no LOCAL fileservice (unit tests, one-shot tools).
// "" is what os.MkdirTemp/os.CreateTemp already interpret as $TMPDIR, so callers
// need no branch — the fallback is the previous behaviour.
//
// This is deliberately NOT carried on IndexTableConfig: that struct is JSON
// marshalled into the table-function argument list and travels between CNs,
// whereas this path is only meaningful on the node that resolved it.
func HostSpillDir(ctx context.Context, rootFS fileservice.FileService, service string) string {
	if rootFS == nil {
		return ""
	}
	local, err := fileservice.Get[*fileservice.LocalFS](rootFS, defines.LocalFileServiceName)
	if err != nil {
		return ""
	}
	dir := filepath.Join(local.RootPath(), localSpillSubdir, spillOwner(service))
	sweepOnce(dir, service)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		// Non-fatal: fall back to $TMPDIR rather than failing a build because a
		// scratch directory could not be created.
		return ""
	}
	return dir
}

// spillOwner is the per-CN subdirectory name. Spill files are owned by exactly one CN, so
// keying the directory by service id lets that CN reclaim its own leftovers without ever
// touching a neighbour's -- two CNs configured onto one LOCAL volume stay independent.
// A CN's uuid is configuration, not generated per boot, so the name is stable across a
// restart, which is what makes the sweep below find the previous incarnation's files
// instead of accumulating one directory per boot.
func spillOwner(service string) string {
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, service)
	if clean == "" {
		return "shared"
	}
	return clean
}

// sweeping tracks the one sweep per directory per process.
var sweeping sync.Map

// sweepOnce deletes everything left in this CN's spill directory, once per process.
//
// A spill file is removed by Destroy, so one still present when this process first needs the
// directory was left by an earlier incarnation that did not get to run it. Nothing else
// collects them -- the LOCAL volume is the operator's data directory, not $TMPDIR -- so
// without this a CN killed mid-load leaves a full-size model behind for good, and repeated
// crashes fill the volume.
//
// Three things make the delete safe. Only this CN's own subdirectory is touched, so a
// neighbour sharing the volume is never affected. It runs before this process creates
// anything here, so it cannot reach a file this process is about to map. And if an earlier
// process is somehow still winding down with a file mapped, unlinking it does not disturb
// that mapping -- the inode outlives the name for as long as the mapping holds it.
//
// Deliberately lazy rather than wired into CN startup: a CN that never loads a vector index
// has nothing here to reclaim, and this keeps the reclaim beside the code that owns the
// directory instead of adding a startup dependency.
//
// Skipped when the service id is empty (unit tests, one-shot tools): with no owner to
// attribute files to, "shared" may be in use by something still running.
func sweepOnce(dir, service string) {
	if service == "" {
		return
	}
	once, _ := sweeping.LoadOrStore(dir, &sync.Once{})
	once.(*sync.Once).Do(func() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return // absent on a first start, which is the common case
		}
		for _, e := range entries {
			victim := filepath.Join(dir, e.Name())
			if err := os.RemoveAll(victim); err != nil {
				logutil.Warnf("vectorindex spill sweep: remove %s: %v", victim, err)
				continue
			}
			logutil.Infof("vectorindex spill sweep: reclaimed orphaned %s", victim)
		}
	})
}
