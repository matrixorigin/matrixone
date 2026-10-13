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

package lifecycle

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

const moLifecycleFaultPrefix = "tae-object-lifecycle/"

// FaultPoint is a stable failure-injection boundary in the Lifecycle owner
// graph. Production connects these points to MO's existing fault control
// plane; deterministic tests can use the local programmable implementation.
type FaultPoint string

const (
	FaultAfterRootRegister       FaultPoint = "after-root-register"
	FaultBeforeRootCAS           FaultPoint = "before-root-cas"
	FaultAfterRootCAS            FaultPoint = "after-root-cas"
	FaultAfterProtection         FaultPoint = "after-protection"
	FaultBeforeSourceRead        FaultPoint = "before-source-read"
	FaultBeforePayloadPut        FaultPoint = "before-payload-put"
	FaultAfterPayloadPut         FaultPoint = "after-payload-put"
	FaultBeforeManifestPut       FaultPoint = "before-manifest-put"
	FaultAfterManifestPut        FaultPoint = "after-manifest-put"
	FaultBeforeFullReadback      FaultPoint = "before-full-readback"
	FaultAfterPayloadWrite       FaultPoint = "after-payload-write"
	FaultAfterFullReadback       FaultPoint = "after-full-readback"
	FaultBeforeRewriteStaging    FaultPoint = "before-rewrite-staging"
	FaultAfterRewriteStaging     FaultPoint = "after-rewrite-staging"
	FaultBeforeFinalCommit       FaultPoint = "before-final-commit"
	FaultAfterFinalCommit        FaultPoint = "after-final-commit"
	FaultBeforeCleanupList       FaultPoint = "before-cleanup-list"
	FaultBeforeCleanupDelete     FaultPoint = "before-cleanup-delete"
	FaultAfterCleanupDelete      FaultPoint = "after-cleanup-delete"
	FaultBeforeRestoreInitialize FaultPoint = "before-restore-initialize"
	FaultAfterRestoreInitialize  FaultPoint = "after-restore-initialize"
	FaultBeforeRestoreChunk      FaultPoint = "before-restore-chunk"
	FaultAfterRestoreChunk       FaultPoint = "after-restore-chunk"
	FaultBeforeRestorePublish    FaultPoint = "before-restore-publish"
	FaultAfterRestorePublish     FaultPoint = "after-restore-publish"
)

type FaultInjector interface {
	Inject(context.Context, FaultPoint) error
}

type NoLifecycleFaults struct{}

func (NoLifecycleFaults) Inject(context.Context, FaultPoint) error {
	return nil
}

// MOFaultPointName is the stable name accepted by MO's existing fault
// injection control plane. It is intentionally namespaced so no ordinary MO
// path is affected by a Lifecycle chaos campaign.
func MOFaultPointName(point FaultPoint) string {
	return moLifecycleFaultPrefix + string(point)
}

// MOFaultInjector is a thin adapter over pkg/util/fault. When fault injection
// is disabled (the production default), TriggerFaultWithContext returns
// immediately without allocation or mutation. Existing RETURN/SLEEP/WAIT and
// PANIC actions remain available to cluster certification.
type MOFaultInjector struct{}

func (MOFaultInjector) Inject(
	ctx context.Context,
	point FaultPoint,
) error {
	code, message, injected := fault.TriggerFaultWithContext(
		ctx,
		MOFaultPointName(point),
	)
	if !injected {
		return nil
	}
	if message == "" {
		message = "injected Lifecycle fault"
	}
	return moerr.NewInternalErrorNoCtxf(
		"%s at %s (code=%d)",
		message,
		point,
		code,
	)
}

type FaultAction func(context.Context, uint64) error

// ProgrammableFaultInjector is shared by deterministic unit tests and
// MO's existing fault/chaos plane. It is scoped to Lifecycle and has no global
// failpoint registry or ordinary-MO hot-path check.
type ProgrammableFaultInjector struct {
	mu      sync.Mutex
	actions map[FaultPoint]FaultAction
	hits    map[FaultPoint]uint64
}

func NewProgrammableFaultInjector(
	actions map[FaultPoint]FaultAction,
) *ProgrammableFaultInjector {
	cloned := make(map[FaultPoint]FaultAction, len(actions))
	for point, action := range actions {
		cloned[point] = action
	}
	return &ProgrammableFaultInjector{
		actions: cloned,
		hits:    make(map[FaultPoint]uint64),
	}
}

func (injector *ProgrammableFaultInjector) Inject(
	ctx context.Context,
	point FaultPoint,
) error {
	if injector == nil {
		return nil
	}
	injector.mu.Lock()
	injector.hits[point]++
	hit := injector.hits[point]
	action := injector.actions[point]
	injector.mu.Unlock()
	if action == nil {
		return nil
	}
	return action(ctx, hit)
}

func (injector *ProgrammableFaultInjector) Hits(point FaultPoint) uint64 {
	if injector == nil {
		return 0
	}
	injector.mu.Lock()
	defer injector.mu.Unlock()
	return injector.hits[point]
}

func FailOnHit(want uint64, message string) FaultAction {
	return func(_ context.Context, hit uint64) error {
		if hit == want {
			return moerr.NewInternalErrorNoCtxf("%s", message)
		}
		return nil
	}
}
