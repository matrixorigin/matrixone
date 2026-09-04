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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		// Non-fatal: fall back to $TMPDIR rather than failing a build because a
		// scratch directory could not be created.
		return ""
	}
	return dir
}

// spillOwner selects a per-CN namespace, not a process-liveness proof. Never
// delete existing files on first use: a previous process with the same identity
// may still be loading them. Owners clean up their own files explicitly.
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
