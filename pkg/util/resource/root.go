// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package resource

import (
	"context"
	"sync"
)

// Root owns the resource summary for one client or standalone internal
// request. Producers publish immutable child summaries; they never share its
// counters in their hot paths.
type Root struct {
	mu            sync.Mutex
	summary       StatementResourceSummary
	memoryPreview func() (uint64, bool)
	attemptOwner  bool
	sealed        bool
}

// NewRoot creates an empty request root.
func NewRoot(conn ConnType) *Root {
	return &Root{summary: StatementResourceSummary{ConnType: conn}}
}

// SetMemoryPeakPreview installs the live-safe statement allocator peak used by
// pre-response consumers such as EXPLAIN ANALYZE. Terminal publication still
// happens exactly once through AddMemoryDomain or MarkMemoryDomainMissing.
func (r *Root) SetMemoryPeakPreview(preview func() (uint64, bool)) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.memoryPreview = preview
	return true
}

// ClearMemoryPeakPreview removes the non-authoritative preview before the
// terminal memory domain is published.
func (r *Root) ClearMemoryPeakPreview() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memoryPreview = nil
}

// AddLocal merges quiescent frontend/root-local work.
func (r *Root) AddLocal(delta Delta) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.Quality |= delta.Quality | MergeUsage(&r.summary.Usage, delta.Usage)
	return true
}

// TryClaimAttemptOwner grants the single execution allowed to publish retry
// generations for this statement root. Callers must first establish that the
// execution is the explicit top-level statement candidate; arbitrary child
// executions must not race for ownership.
func (r *Root) TryClaimAttemptOwner() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed || r.attemptOwner {
		return false
	}
	r.attemptOwner = true
	return true
}

// MergeExecution publishes one sealed logical execution.
func (r *Root) MergeExecution(execution ExecutionSummary) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.MergeExecution(execution)
	return true
}

// AddProtocolOutput records application-protocol bytes accepted for client
// write and completed protocol packets.
func (r *Root) AddProtocolOutput(bytes, packets uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.Usage.ClientEgressBytes, r.summary.Quality = addChecked(
		r.summary.Usage.ClientEgressBytes, bytes, r.summary.Quality)
	r.summary.OutputPacketCount, r.summary.Quality = addChecked(
		r.summary.OutputPacketCount, packets, r.summary.Quality)
	return true
}

// AddMemoryDomain publishes one terminal allocator domain owned by the
// statement root.
func (r *Root) AddMemoryDomain(domain MemoryDomainSummary) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.Quality |= MergeMemoryDomain(&r.summary.Memory, domain)
	return true
}

// AddMemoryPeakObservation records the maximum live-byte occupancy observed
// in a non-isolated but request-serialized MPool. It intentionally contributes
// no allocation/free/live-at-seal facts: those require an isolated domain.
func (r *Root) AddMemoryPeakObservation(peak uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.Memory.SumDomainPeakLiveBytesBound, r.summary.Quality = addChecked(
		r.summary.Memory.SumDomainPeakLiveBytesBound, peak, r.summary.Quality)
	if peak > r.summary.Memory.MaxDomainPeakLiveBytes {
		r.summary.Memory.MaxDomainPeakLiveBytes = peak
	}
	return true
}

// MarkMemoryDomainMissing records that an expected allocator domain or peak
// observation could not be published for this statement.
func (r *Root) MarkMemoryDomainMissing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false
	}
	r.summary.MissingMemoryDomainCount, r.summary.Quality = addChecked(
		r.summary.MissingMemoryDomainCount, 1,
		r.summary.Quality|QualityPartial|QualityMissingMemoryDomain,
	)
	return true
}

// PreResponseSummary returns an immutable snapshot without sealing the root.
func (r *Root) PreResponseSummary() StatementResourceSummary {
	r.mu.Lock()
	summary := r.summary
	preview := r.memoryPreview
	r.mu.Unlock()
	if preview == nil {
		return summary
	}
	if peak, exact := preview(); exact {
		summary.Memory.SumDomainPeakLiveBytesBound, summary.Quality = addChecked(
			summary.Memory.SumDomainPeakLiveBytesBound, peak, summary.Quality)
		if peak > summary.Memory.MaxDomainPeakLiveBytes {
			summary.Memory.MaxDomainPeakLiveBytes = peak
		}
	} else {
		summary.MissingMemoryDomainCount, summary.Quality = addChecked(
			summary.MissingMemoryDomainCount, 1,
			summary.Quality|QualityPartial|QualityMissingMemoryDomain,
		)
	}
	return summary
}

// Seal sets the request wall duration and returns an immutable summary. It is
// idempotent so panic/error defers can safely converge on one path.
func (r *Root) Seal(statementWallNS uint64) StatementResourceSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.sealed {
		r.summary.StatementWallNS = statementWallNS
		r.sealed = true
	}
	return r.summary
}

// IsSealed reports whether the terminal summary was captured.
func (r *Root) IsSealed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sealed
}

type rootContextKey struct{}

// ContextWithRoot attaches the single request root.
func ContextWithRoot(ctx context.Context, root *Root) context.Context {
	if root == nil {
		return ctx
	}
	return context.WithValue(ctx, rootContextKey{}, root)
}

// RootFromContext returns the request root, if any.
func RootFromContext(ctx context.Context) *Root {
	if ctx == nil {
		return nil
	}
	root, _ := ctx.Value(rootContextKey{}).(*Root)
	return root
}
