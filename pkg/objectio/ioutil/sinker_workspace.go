// Copyright 2021 Matrix Origin
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

package ioutil

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// UncommittedCapacityHook reserves one object before its backing reaches Sync.
type UncommittedCapacityHook func(name string) error

// SinkerOwner supplies transaction-local object admission without an engine dependency.
type SinkerOwner interface{ SinkerCapacityOption() SinkerOption }

// UnrecordedSinkerObjectsRetainer takes cleanup ownership after failed publication.
type UnrecordedSinkerObjectsRetainer interface{ RetainUnrecordedS3Objects([]string, string) }
type activeObjectNamer interface{ ActiveObjectName() string }

func (s *FSinkerImpl) ActiveObjectName() string {
	if s.writer == nil {
		return ""
	}
	return s.writer.GetName().String()
}

// WithWorkspaceOwnedCleanup leaves persistence cleanup to the workspace.
// Before Reset or Close, the owner must take AbandonAndRetainableNames.
func WithWorkspaceOwnedCleanup() SinkerOption {
	return func(sinker *Sinker) { sinker.config.workspaceOwnedCleanup = true }
}

func (sinker *Sinker) WorkspaceOwnsCleanup() bool {
	return sinker != nil && sinker.config.workspaceOwnedCleanup
}

func (sinker *Sinker) ClearWorkspaceOwnedCleanup() {
	sinker.config.workspaceOwnedCleanup = false
}

func WithUncommittedCapacityHook(hook UncommittedCapacityHook) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.capacityHook = hook
	}
}

func (sinker *Sinker) HasUncommittedCapacityHook() bool {
	return sinker != nil && sinker.config.capacityHook != nil
}

func (sinker *Sinker) RebindUncommittedCapacityHook(hook UncommittedCapacityHook) {
	sinker.config.capacityHook = hook
}

func activeFileSinkerObjectName(sinker FileSinker) string {
	namer, ok := sinker.(activeObjectNamer)
	if !ok {
		return ""
	}
	return namer.ActiveObjectName()
}

func (sinker *Sinker) CancelPending() {
	if sinker.pipe.result != nil {
		sinker.pipe.result.cancel()
	}
	if sinker.runSizeLimit() > 0 {
		sinker.staged.aborted = true
	}
}

// AbandonAndRetainableNames cancels and joins every accepted pipeline job
// before the caller takes ownership of the returned names. It performs no I/O.
func (sinker *Sinker) AbandonAndRetainableNames() []string {
	sinker.CancelPending()
	files := sinker.persistedSnapshot()
	if sinker.config.workspaceOwnedCleanup {
		return files
	}
	files = files[:0]
	for i := range sinker.result.persisted {
		files = append(files, sinker.result.persisted[i].ObjectName().String())
	}
	return normalizeUnpublishedObjectNames(files)
}

func (sinker *Sinker) persistedSnapshot() []string {
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		// A failed pipeline may still have successful sibling writes. Wait for
		// all of them before taking the ownership snapshot.
		_ = sinker.drainPipeline()
	}

	files := make([]string, 0,
		len(sinker.staged.persisted)+len(sinker.result.persisted)+
			len(sinker.staged.unpublished))
	files = append(files, sinker.staged.unpublished...)
	appendStats := func(stats []objectio.ObjectStats) {
		for i := range stats {
			files = append(files, stats[i].ObjectName().String())
		}
	}
	appendStats(sinker.staged.persisted)
	appendStats(sinker.result.persisted)
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.RLock()
		appendStats(sinker.pipe.result.persisted)
		files = append(files, sinker.pipe.result.unpublished...)
		sinker.pipe.result.mu.RUnlock()
	}
	return normalizeUnpublishedObjectNames(files)
}

func (sinker *Sinker) PersistedNames() []string {
	return sinker.persistedSnapshot()
}

func normalizeUnpublishedObjectNames(files []string) []string {
	seen := make(map[string]struct{}, len(files))
	out := make([]string, 0, len(files))
	for _, file := range files {
		if file == "" {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		out = append(out, file)
	}
	sort.Strings(out)
	return out
}
