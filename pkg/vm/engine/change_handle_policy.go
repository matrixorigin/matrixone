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

package engine

import "context"

// noCommitTSColumnError is returned by CollectChanges when a data object
// lacks per-row commit-ts columns (e.g. TN merge objects on 3.0-dev that
// predate the commit-ts-in-objects change).  The caller cannot rely on
// row-level time-range filtering and must fall back to a full-table-scan
// diff strategy.
type noCommitTSColumnError struct{}

func (noCommitTSColumnError) Error() string { return "object has no per-row commit-ts column" }

// ErrNoCommitTSColumn is the singleton used by CollectChanges callers.
var ErrNoCommitTSColumn error = noCommitTSColumnError{}

// IsErrNoCommitTSColumn reports whether err is (or wraps) ErrNoCommitTSColumn.
func IsErrNoCommitTSColumn(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(noCommitTSColumnError)
	return ok
}

// SnapshotReadPolicy controls how CollectChanges rebuilds changes once the
// requested range can no longer be served purely from the in-memory partition
// state and has to rely on snapshot-based recovery.
//
// The default policy must remain compatible with existing CDC/replay callers.
type SnapshotReadPolicy uint8

const (
	// SnapshotReadPolicyCheckpointReplay keeps the existing checkpoint replay
	// behavior. It reconstructs change batches from checkpoint metadata and
	// preserves the historical semantics already used by CDC restart paths.
	SnapshotReadPolicyCheckpointReplay SnapshotReadPolicy = iota
	// SnapshotReadPolicyVisibleState preserves the exact visible net effect of
	// CollectChanges(start, end) when snapshot read is required. The primary
	// recovery path still uses range-aware checkpoint object selection; if those
	// object files are gone as well, it falls back to reconstructing the range
	// from visible snapshots.
	SnapshotReadPolicyVisibleState
)

type snapshotReadPolicyKey struct{}

// WithSnapshotReadPolicy attaches a snapshot-read policy to the context so
// callers can opt into a different recovery semantic without widening the
// engine.Relation interface. The default policy is intentionally omitted from
// the context to keep existing callers unchanged.
func WithSnapshotReadPolicy(ctx context.Context, policy SnapshotReadPolicy) context.Context {
	if ctx == nil || policy == SnapshotReadPolicyCheckpointReplay {
		return ctx
	}
	return context.WithValue(ctx, snapshotReadPolicyKey{}, policy)
}

// SnapshotReadPolicyFromContext returns the policy requested by the caller.
// Missing context state always falls back to the checkpoint replay semantic.
func SnapshotReadPolicyFromContext(ctx context.Context) SnapshotReadPolicy {
	if ctx == nil {
		return SnapshotReadPolicyCheckpointReplay
	}
	policy, ok := ctx.Value(snapshotReadPolicyKey{}).(SnapshotReadPolicy)
	if !ok {
		return SnapshotReadPolicyCheckpointReplay
	}
	return policy
}
