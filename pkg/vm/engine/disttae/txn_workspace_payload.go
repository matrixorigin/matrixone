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

package disttae

import (
	"slices"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type workspacePayloadID uint64

type workspacePayloadState uint8

const (
	workspacePayloadMemory workspacePayloadState = iota + 1
	workspacePayloadSpilling
	workspacePayloadObject
	workspacePayloadRetired
)

// workspacePayloadVersion has immutable identity and physical content after
// publication. Visibility bounds and the pin count are lifecycle metadata
// maintained only while workspacePayloadStore.mu is held. Keeping physical
// generations until they can be reclaimed lets an already published read
// view continue to resolve its original payload after a selection change,
// compaction or spill replacement.
type workspacePayloadVersion struct {
	generation      uint64
	createdRevision uint64
	retiredRevision uint64
	state           workspacePayloadState
	bat             *batch.Batch
	selections      []int64
	pins            uint64
}

type workspacePayload struct {
	id         workspacePayloadID
	generation uint64
	versions   []*workspacePayloadVersion
}

// workspaceSpillPayload is the physical identity captured when a spill
// attempt starts. generation is validated again at publish time; a stale
// attempt can therefore never replace a payload changed by rollback,
// compaction, selection publication, or another spill.
type workspaceSpillPayload struct {
	payloadID  workspacePayloadID
	generation uint64
	lease      *workspacePayloadLease
}

// workspacePayloadStore owns the mapping from logical PayloadID to immutable
// physical generations. Logical workspace metadata never exposes a raw Batch;
// readers must pin one generation through a lease.
type workspacePayloadStore struct {
	mu sync.Mutex

	nextID   workspacePayloadID
	payloads map[workspacePayloadID]*workspacePayload
	byBatch  map[*batch.Batch]workspacePayloadID

	// reclaimable contains only retired, unpinned generations. It is updated
	// at the retirement and lease-release boundaries so statement completion
	// never has to rediscover candidates by scanning transaction history.
	reclaimable map[*workspacePayloadVersion]*workspacePayload
	// batchReferences counts logical generations, not current logical owners.
	// Selection-only publications deliberately share one physical Batch; the
	// Batch leaves the store only when its final generation is reclaimed.
	batchReferences map[*batch.Batch]uint64
}

type workspaceMemoryCompaction struct {
	dstPayloadID  workspacePayloadID
	dstOldBat     *batch.Batch
	dstNewBat     *batch.Batch
	srcPayloadIDs []workspacePayloadID
	srcOldBats    []*batch.Batch
}

type workspacePayloadRewrite struct {
	payloadID  workspacePayloadID
	oldBat     *batch.Batch
	newBat     *batch.Batch
	state      workspacePayloadState
	selections []int64
}

type workspacePayloadTransitionSource struct {
	payloadID workspacePayloadID
	oldBat    *batch.Batch
}

type workspacePayloadTransitionTarget struct {
	bat        *batch.Batch
	state      workspacePayloadState
	selections []int64
}

type workspacePayloadSelectionRestore struct {
	payloadID  workspacePayloadID
	selections []int64
}

type workspacePayloadRewriteRestore struct {
	payloadID  workspacePayloadID
	bat        *batch.Batch
	selections []int64
}

func newWorkspacePayloadStore() *workspacePayloadStore {
	return &workspacePayloadStore{
		payloads:        make(map[workspacePayloadID]*workspacePayload),
		byBatch:         make(map[*batch.Batch]workspacePayloadID),
		reclaimable:     make(map[*workspacePayloadVersion]*workspacePayload),
		batchReferences: make(map[*batch.Batch]uint64),
	}
}

func (s *workspacePayloadStore) appendVersionLocked(
	payload *workspacePayload,
	version *workspacePayloadVersion,
) {
	payload.versions = append(payload.versions, version)
	if version.bat != nil {
		s.batchReferences[version.bat]++
	}
}

func (s *workspacePayloadStore) retireVersionLocked(
	payload *workspacePayload,
	version *workspacePayloadVersion,
	revision uint64,
) {
	if version.retiredRevision != 0 {
		panic("BUG: retiring an already retired workspace payload generation")
	}
	version.retiredRevision = revision
	if version.pins == 0 {
		s.reclaimable[version] = payload
	}
}

func (s *workspacePayloadStore) addMemory(
	bat *batch.Batch,
	selections []int64,
	revision uint64,
) workspacePayloadID {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	payload := &workspacePayload{id: s.nextID, generation: 1}
	s.appendVersionLocked(payload, &workspacePayloadVersion{
		generation:      payload.generation,
		createdRevision: revision,
		state:           workspacePayloadMemory,
		bat:             bat,
		selections:      normalizeWorkspaceSelections(selections),
	})
	s.payloads[payload.id] = payload
	if bat != nil {
		s.byBatch[bat] = payload.id
	}
	return payload.id
}

func (s *workspacePayloadStore) addObjectLocked(
	bat *batch.Batch,
	revision uint64,
) workspacePayloadID {
	s.nextID++
	payload := &workspacePayload{id: s.nextID, generation: 1}
	s.appendVersionLocked(payload, &workspacePayloadVersion{
		generation:      payload.generation,
		createdRevision: revision,
		state:           workspacePayloadObject,
		bat:             bat,
	})
	s.payloads[payload.id] = payload
	if bat != nil {
		s.byBatch[bat] = payload.id
	}
	return payload.id
}

// currentBatch returns the currently published physical batch. The caller
// must hold txnWorkspace.mu so the logical mutation cannot be replaced or
// retired between resolving its PayloadID and accounting the transition.
func (s *workspacePayloadStore) currentBatch(id workspacePayloadID) (*batch.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("workspace payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return nil, moerr.NewInternalErrorNoCtx("workspace payload is retired")
	}
	return current.bat, nil
}

// tryBeginSpill pins the current memory generation and marks it as being
// materialized. The physical batch remains readable through old and current
// read views; Spilling is an ownership state, not a visibility change.
//
// A concurrently running spill owns a Spilling generation. That generation is
// still readable, but it is not an error and must not be materialized twice;
// callers simply leave it to the owning attempt. The bool reports whether this
// call acquired the generation.
func (s *workspacePayloadStore) tryBeginSpill(
	id workspacePayloadID,
) (workspaceSpillPayload, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return workspaceSpillPayload{}, false, moerr.NewInternalErrorNoCtx(
			"workspace spill payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return workspaceSpillPayload{}, false, moerr.NewInternalErrorNoCtx(
			"workspace payload is not spillable")
	}
	if current.state == workspacePayloadSpilling {
		return workspaceSpillPayload{}, false, nil
	}
	if current.state != workspacePayloadMemory {
		return workspaceSpillPayload{}, false, moerr.NewInternalErrorNoCtx(
			"workspace payload is not spillable")
	}
	current.state = workspacePayloadSpilling
	current.pins++
	lease := &workspacePayloadLease{
		store:      s,
		payloadID:  id,
		generation: current.generation,
		bat:        current.bat,
		selections: slices.Clone(current.selections),
	}
	return workspaceSpillPayload{
		payloadID:  id,
		generation: current.generation,
		lease:      lease,
	}, true, nil
}

// abortSpills restores every still-current source generation to Memory. A
// source changed or retired while IO was in flight is deliberately left
// untouched; its owner already established the newer state.
func (s *workspacePayloadStore) abortSpills(spills []workspaceSpillPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, spill := range spills {
		payload := s.payloads[spill.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			continue
		}
		current := payload.versions[len(payload.versions)-1]
		if current.generation == spill.generation &&
			current.retiredRevision == 0 &&
			current.state == workspacePayloadSpilling {
			current.state = workspacePayloadMemory
		}
	}
}

// commitSpills validates all sources before making any change, then retires
// the source generations and publishes all object payloads at one workspace
// revision. No partial physical transition is observable.
func (s *workspacePayloadStore) commitSpills(
	spills []workspaceSpillPayload,
	objectBatches []*batch.Batch,
	revision uint64,
) ([]workspacePayloadID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenTargets := make(map[*batch.Batch]struct{}, len(objectBatches))
	for _, bat := range objectBatches {
		if bat == nil {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill target has no payload")
		}
		if _, duplicate := seenTargets[bat]; duplicate {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill contains duplicate target payload")
		}
		seenTargets[bat] = struct{}{}
		if _, owned := s.byBatch[bat]; owned {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill target is already owned")
		}
	}

	for _, spill := range spills {
		payload := s.payloads[spill.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill source does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.generation != spill.generation ||
			current.retiredRevision != 0 ||
			current.state != workspacePayloadSpilling {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill source changed before publication")
		}
	}

	for _, spill := range spills {
		payload := s.payloads[spill.payloadID]
		current := payload.versions[len(payload.versions)-1]
		current.state = workspacePayloadRetired
		s.retireVersionLocked(payload, current, revision)
		if current.bat != nil {
			delete(s.byBatch, current.bat)
		}
	}

	ids := make([]workspacePayloadID, len(objectBatches))
	for idx, bat := range objectBatches {
		ids[idx] = s.addObjectLocked(bat, revision)
	}
	return ids, nil
}

// replaceSelections publishes a new immutable logical generation while
// retaining the same physical batch. Old read views continue to resolve the
// preceding selection set.
func (s *workspacePayloadStore) replaceSelections(
	id workspacePayloadID,
	selections []int64,
	revision uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace payload is retired")
	}
	s.retireVersionLocked(payload, current, revision)
	payload.generation++
	s.appendVersionLocked(payload, &workspacePayloadVersion{
		generation:      payload.generation,
		createdRevision: revision,
		state:           current.state,
		bat:             current.bat,
		selections:      normalizeWorkspaceSelections(selections),
	})
	return nil
}

// rollbackAttempt atomically retires payloads created by the failed attempt
// and restores selection sets changed on mutations owned by earlier attempts.
// It validates the complete rollback set before publishing one revision.
func (s *workspacePayloadStore) rollbackAttempt(
	retireIDs []workspacePayloadID,
	selectionRestores []workspacePayloadSelectionRestore,
	rewriteRestores []workspacePayloadRewriteRestore,
	revision uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[workspacePayloadID]struct{},
		len(retireIDs)+len(selectionRestores)+len(rewriteRestores))
	for _, id := range retireIDs {
		if _, duplicate := seen[id]; duplicate {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback contains duplicate payload")
		}
		seen[id] = struct{}{}
		payload := s.payloads[id]
		if payload == nil || len(payload.versions) == 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload is already retired")
		}
	}
	for _, restore := range selectionRestores {
		if _, duplicate := seen[restore.payloadID]; duplicate {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback payload has conflicting actions")
		}
		seen[restore.payloadID] = struct{}{}
		payload := s.payloads[restore.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload is already retired")
		}
	}
	for _, restore := range rewriteRestores {
		if _, duplicate := seen[restore.payloadID]; duplicate {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback payload has conflicting actions")
		}
		seen[restore.payloadID] = struct{}{}
		payload := s.payloads[restore.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision == 0 || current.bat != restore.bat {
			return moerr.NewInternalErrorNoCtx(
				"workspace rewrite payload cannot be restored")
		}
		if owner, owned := s.byBatch[current.bat]; owned && owner != restore.payloadID {
			return moerr.NewInternalErrorNoCtx(
				"workspace rewrite payload is owned by another mutation")
		}
	}

	for _, id := range retireIDs {
		payload := s.payloads[id]
		current := payload.versions[len(payload.versions)-1]
		s.retireVersionLocked(payload, current, revision)
		if current.bat != nil {
			delete(s.byBatch, current.bat)
		}
	}
	for _, restore := range selectionRestores {
		payload := s.payloads[restore.payloadID]
		current := payload.versions[len(payload.versions)-1]
		s.retireVersionLocked(payload, current, revision)
		payload.generation++
		s.appendVersionLocked(payload, &workspacePayloadVersion{
			generation:      payload.generation,
			createdRevision: revision,
			state:           current.state,
			bat:             current.bat,
			selections:      normalizeWorkspaceSelections(restore.selections),
		})
	}
	for _, restore := range rewriteRestores {
		payload := s.payloads[restore.payloadID]
		current := payload.versions[len(payload.versions)-1]
		payload.generation++
		s.appendVersionLocked(payload, &workspacePayloadVersion{
			generation:      payload.generation,
			createdRevision: revision,
			state:           current.state,
			bat:             current.bat,
			selections:      normalizeWorkspaceSelections(restore.selections),
		})
		if current.bat != nil {
			s.byBatch[current.bat] = restore.payloadID
		}
	}
	return nil
}

func (s *workspacePayloadStore) replaceMemory(
	id workspacePayloadID,
	oldBat *batch.Batch,
	newBat *batch.Batch,
	selections []int64,
	revision uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace payload is retired")
	}
	if current.state != workspacePayloadMemory {
		return moerr.NewInternalErrorNoCtx("workspace payload is not mutable memory")
	}
	if current.bat != oldBat {
		return moerr.NewInternalErrorNoCtx("workspace payload generation changed")
	}
	if newBat != nil && newBat != oldBat {
		if _, owned := s.byBatch[newBat]; owned {
			return moerr.NewInternalErrorNoCtx(
				"workspace replacement target is already owned")
		}
	}
	s.retireVersionLocked(payload, current, revision)
	payload.generation++
	s.appendVersionLocked(payload, &workspacePayloadVersion{
		generation:      payload.generation,
		createdRevision: revision,
		state:           workspacePayloadMemory,
		bat:             newBat,
		selections:      normalizeWorkspaceSelections(selections),
	})
	if oldBat != nil {
		delete(s.byBatch, oldBat)
	}
	if newBat != nil {
		s.byBatch[newBat] = id
	}
	return nil
}

// rewriteMany retires a set of logical source payloads and creates their
// replacements at one revision. All sources and targets are validated before
// publication, so a metadata rewrite such as ALTER TABLE cannot expose only a
// prefix of the rewritten workspace.
func (s *workspacePayloadStore) rewriteMany(
	rewrites []workspacePayloadRewrite,
	revision uint64,
) ([]workspacePayloadID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenPayloads := make(map[workspacePayloadID]struct{}, len(rewrites))
	seenBatches := make(map[*batch.Batch]struct{}, len(rewrites))
	for _, rewrite := range rewrites {
		if _, ok := seenPayloads[rewrite.payloadID]; ok {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite contains duplicate payload")
		}
		seenPayloads[rewrite.payloadID] = struct{}{}
		payload := s.payloads[rewrite.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite source does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision != 0 || current.state == workspacePayloadSpilling {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite source is not stable")
		}
		if current.bat != rewrite.oldBat {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite source generation changed")
		}
		if rewrite.newBat == nil {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite target has no payload")
		}
		if rewrite.state != workspacePayloadMemory &&
			rewrite.state != workspacePayloadObject {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite target has invalid state")
		}
		if _, ok := seenBatches[rewrite.newBat]; ok {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite contains duplicate target payload")
		}
		seenBatches[rewrite.newBat] = struct{}{}
		if _, ok := s.byBatch[rewrite.newBat]; ok {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite target is already owned")
		}
	}

	for _, rewrite := range rewrites {
		payload := s.payloads[rewrite.payloadID]
		current := payload.versions[len(payload.versions)-1]
		s.retireVersionLocked(payload, current, revision)
		if current.bat != nil {
			delete(s.byBatch, current.bat)
		}
	}

	ids := make([]workspacePayloadID, len(rewrites))
	for idx, rewrite := range rewrites {
		s.nextID++
		payload := &workspacePayload{id: s.nextID, generation: 1}
		s.appendVersionLocked(payload, &workspacePayloadVersion{
			generation:      payload.generation,
			createdRevision: revision,
			state:           rewrite.state,
			bat:             rewrite.newBat,
			selections:      normalizeWorkspaceSelections(rewrite.selections),
		})
		s.payloads[payload.id] = payload
		s.byBatch[rewrite.newBat] = payload.id
		ids[idx] = payload.id
	}
	return ids, nil
}

// transitionMany is the payload-store counterpart of Workspace's generic
// source-set to target-set publication. Complete validation precedes any
// generation change; therefore zero-target retirement and N-to-M object
// compaction have identical all-or-nothing visibility semantics.
func (s *workspacePayloadStore) transitionMany(
	sources []workspacePayloadTransitionSource,
	targets []workspacePayloadTransitionTarget,
	revision uint64,
) ([]workspacePayloadID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenSources := make(map[workspacePayloadID]struct{}, len(sources))
	seenTargets := make(map[*batch.Batch]struct{}, len(targets))
	for _, source := range sources {
		if _, duplicate := seenSources[source.payloadID]; duplicate {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition contains duplicate source payload")
		}
		seenSources[source.payloadID] = struct{}{}
		payload := s.payloads[source.payloadID]
		if payload == nil || len(payload.versions) == 0 {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition source does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision != 0 || current.state == workspacePayloadSpilling {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition source is not stable")
		}
		if current.bat != source.oldBat {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition source generation changed")
		}
	}
	for _, target := range targets {
		if target.bat == nil {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition target has no payload")
		}
		if target.state != workspacePayloadMemory &&
			target.state != workspacePayloadObject {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition target has invalid state")
		}
		if _, duplicate := seenTargets[target.bat]; duplicate {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition contains duplicate target payload")
		}
		seenTargets[target.bat] = struct{}{}
		if _, owned := s.byBatch[target.bat]; owned {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace transition target is already owned")
		}
	}

	for _, source := range sources {
		payload := s.payloads[source.payloadID]
		current := payload.versions[len(payload.versions)-1]
		s.retireVersionLocked(payload, current, revision)
		if current.bat != nil {
			delete(s.byBatch, current.bat)
		}
	}
	ids := make([]workspacePayloadID, len(targets))
	for idx, target := range targets {
		s.nextID++
		payload := &workspacePayload{id: s.nextID, generation: 1}
		s.appendVersionLocked(payload, &workspacePayloadVersion{
			generation:      payload.generation,
			createdRevision: revision,
			state:           target.state,
			bat:             target.bat,
			selections:      normalizeWorkspaceSelections(target.selections),
		})
		s.payloads[payload.id] = payload
		s.byBatch[target.bat] = payload.id
		ids[idx] = payload.id
	}
	return ids, nil
}

// compactMemoryMany publishes all destination replacements and source
// retirements at one revision. Validation is deliberately completed before
// touching any generation so a failed compaction cannot leave only a prefix
// of its logical merges visible.
func (s *workspacePayloadStore) compactMemoryMany(
	compactions []workspaceMemoryCompaction,
	revision uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[workspacePayloadID]struct{})
	for _, merge := range compactions {
		if merge.dstNewBat == nil {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge destination has no payload")
		}
		if len(merge.srcPayloadIDs) != len(merge.srcOldBats) {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge sources are invalid")
		}
		if _, ok := seen[merge.dstPayloadID]; ok {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge contains duplicate payload")
		}
		seen[merge.dstPayloadID] = struct{}{}
		if _, ok := s.byBatch[merge.dstNewBat]; ok {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge destination is already owned")
		}
		if err := s.validateMemoryGenerationLocked(
			merge.dstPayloadID, merge.dstOldBat); err != nil {
			return err
		}
		for idx, srcID := range merge.srcPayloadIDs {
			if _, ok := seen[srcID]; ok {
				return moerr.NewInternalErrorNoCtx(
					"workspace merge contains duplicate payload")
			}
			seen[srcID] = struct{}{}
			if err := s.validateMemoryGenerationLocked(
				srcID, merge.srcOldBats[idx]); err != nil {
				return err
			}
		}
	}

	for _, merge := range compactions {
		dst := s.payloads[merge.dstPayloadID]
		dstCurrent := dst.versions[len(dst.versions)-1]
		s.retireVersionLocked(dst, dstCurrent, revision)
		dst.generation++
		s.appendVersionLocked(dst, &workspacePayloadVersion{
			generation:      dst.generation,
			createdRevision: revision,
			state:           workspacePayloadMemory,
			bat:             merge.dstNewBat,
		})
		if merge.dstOldBat != nil {
			delete(s.byBatch, merge.dstOldBat)
		}
		s.byBatch[merge.dstNewBat] = merge.dstPayloadID

		for idx, srcID := range merge.srcPayloadIDs {
			src := s.payloads[srcID]
			srcCurrent := src.versions[len(src.versions)-1]
			s.retireVersionLocked(src, srcCurrent, revision)
			if merge.srcOldBats[idx] != nil {
				delete(s.byBatch, merge.srcOldBats[idx])
			}
		}
	}
	return nil
}

func (s *workspacePayloadStore) validateMemoryGenerationLocked(
	id workspacePayloadID,
	oldBat *batch.Batch,
) error {
	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return moerr.NewInternalErrorNoCtx("workspace merge payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace merge payload is retired")
	}
	if current.state != workspacePayloadMemory {
		return moerr.NewInternalErrorNoCtx("workspace merge payload is not memory")
	}
	if current.bat != oldBat {
		return moerr.NewInternalErrorNoCtx(
			"workspace merge payload generation changed")
	}
	return nil
}

func (s *workspacePayloadStore) retire(id workspacePayloadID, revision uint64) error {
	return s.retireMany([]workspacePayloadID{id}, revision)
}

// retireMany publishes one retirement revision for a complete logical
// operation. Validation happens before any version is changed so callers
// never expose a partially retired statement attempt.
func (s *workspacePayloadStore) retireMany(
	ids []workspacePayloadID,
	revision uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		payload := s.payloads[id]
		if payload == nil || len(payload.versions) == 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload does not exist")
		}
		current := payload.versions[len(payload.versions)-1]
		if current.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx("workspace payload is already retired")
		}
	}
	for _, id := range ids {
		payload := s.payloads[id]
		current := payload.versions[len(payload.versions)-1]
		s.retireVersionLocked(payload, current, revision)
		if current.bat != nil {
			delete(s.byBatch, current.bat)
		}
	}
	return nil
}

func (s *workspacePayloadStore) currentSelections(id workspacePayloadID) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil || len(payload.versions) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("workspace payload does not exist")
	}
	current := payload.versions[len(payload.versions)-1]
	if current.retiredRevision != 0 {
		return nil, moerr.NewInternalErrorNoCtx("workspace payload is retired")
	}
	return slices.Clone(current.selections), nil
}

func (s *workspacePayloadStore) pin(
	id workspacePayloadID,
	revision uint64,
) (*workspacePayloadLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil {
		return nil, moerr.NewInternalErrorNoCtx("workspace payload does not exist")
	}
	for idx := len(payload.versions) - 1; idx >= 0; idx-- {
		version := payload.versions[idx]
		if version.createdRevision > revision ||
			(version.retiredRevision != 0 && revision >= version.retiredRevision) {
			continue
		}
		// A historical read view may pin a retired generation after its
		// retirement was published but before statement-boundary reclamation.
		// Remove that generation from the ready set until the final lease closes.
		if version.pins == 0 && version.retiredRevision != 0 {
			delete(s.reclaimable, version)
		}
		version.pins++
		return &workspacePayloadLease{
			store:      s,
			payloadID:  id,
			generation: version.generation,
			bat:        version.bat,
			selections: slices.Clone(version.selections),
		}, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("workspace payload is not visible in read view")
}

func (s *workspacePayloadStore) release(id workspacePayloadID, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload := s.payloads[id]
	if payload == nil {
		panic("BUG: releasing unknown workspace payload")
	}
	for _, version := range payload.versions {
		if version.generation != generation {
			continue
		}
		if version.pins == 0 {
			panic("BUG: workspace payload lease underflow")
		}
		version.pins--
		if version.pins == 0 && version.retiredRevision != 0 {
			s.reclaimable[version] = payload
		}
		return
	}
	panic("BUG: releasing unknown workspace payload generation")
}

// reclaimRetired removes retired logical generations that can no longer be
// observed by a live reader or restored by statement rollback. It returns the
// physical batches whose ownership left the store; callers must clean them
// after releasing txnWorkspace.mu.
//
// Selection-only generations may share one Batch. A physical batch is
// returned only when no retained generation references it. protected contains
// batches captured by the current statement's rewrite undo journal: the
// journal can still restore those batches until the next statement boundary
// advances successfully.
func (s *workspacePayloadStore) reclaimRetired(
	protected map[*batch.Batch]struct{},
) []*batch.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Group ready generations by logical payload so each affected version
	// slice is compacted once. Pinned generations are absent from reclaimable;
	// release publishes them here only when their final lease closes.
	readyByPayload := make(
		map[*workspacePayload]map[*workspacePayloadVersion]struct{})
	for version, payload := range s.reclaimable {
		if _, rollbackProtected := protected[version.bat]; rollbackProtected {
			continue
		}
		ready := readyByPayload[payload]
		if ready == nil {
			ready = make(map[*workspacePayloadVersion]struct{})
			readyByPayload[payload] = ready
		}
		ready[version] = struct{}{}
	}

	var result []*batch.Batch
	for payload, ready := range readyByPayload {
		retained := payload.versions[:0]
		for _, version := range payload.versions {
			if _, reclaim := ready[version]; !reclaim {
				retained = append(retained, version)
				continue
			}
			if version.pins != 0 || version.retiredRevision == 0 {
				panic("BUG: invalid workspace payload reclamation candidate")
			}
			delete(s.reclaimable, version)
			if version.bat == nil {
				continue
			}
			references := s.batchReferences[version.bat]
			if references == 0 {
				panic("BUG: workspace payload Batch reference underflow")
			}
			if references == 1 {
				delete(s.batchReferences, version.bat)
				result = append(result, version.bat)
			} else {
				s.batchReferences[version.bat] = references - 1
			}
		}
		payload.versions = retained
		if len(payload.versions) == 0 {
			delete(s.payloads, payload.id)
		}
	}
	return result
}

// takeAll transfers every physical memory generation out of the store exactly
// once. A transaction cannot be destroyed while a reader still owns a lease:
// doing so would turn an otherwise safe generation pin into a dangling Batch
// pointer. The store remains intact when this invariant is violated so the
// caller can diagnose and close the outstanding reader before retrying.
//
// The caller owns every returned batch and must clean it without holding
// txnWorkspace.mu or workspacePayloadStore.mu.
func (s *workspacePayloadStore) takeAll() ([]*batch.Batch, error) {
	s.mu.Lock()
	for _, payload := range s.payloads {
		for _, version := range payload.versions {
			if version.pins != 0 {
				s.mu.Unlock()
				return nil, moerr.NewInternalErrorNoCtxf(
					"workspace payload %d generation %d still has %d lease(s)",
					payload.id,
					version.generation,
					version.pins,
				)
			}
		}
	}

	// Selection-only generations may share one physical Batch. A map is used
	// deliberately here: physical ownership is by pointer identity, while a
	// logical generation is only a visibility/versioning concept.
	batches := make(map[*batch.Batch]struct{})
	for _, payload := range s.payloads {
		for _, version := range payload.versions {
			if version.bat != nil {
				batches[version.bat] = struct{}{}
			}
		}
	}
	s.payloads = make(map[workspacePayloadID]*workspacePayload)
	s.byBatch = make(map[*batch.Batch]workspacePayloadID)
	s.reclaimable = make(map[*workspacePayloadVersion]*workspacePayload)
	s.batchReferences = make(map[*batch.Batch]uint64)
	s.mu.Unlock()

	result := make([]*batch.Batch, 0, len(batches))
	for bat := range batches {
		result = append(result, bat)
	}
	return result, nil
}

func (s *workspacePayloadStore) close(mp *mpool.MPool) error {
	batches, err := s.takeAll()
	if err != nil {
		return err
	}
	for _, bat := range batches {
		bat.Clean(mp)
	}
	return nil
}

type workspacePayloadLease struct {
	store      *workspacePayloadStore
	payloadID  workspacePayloadID
	generation uint64
	bat        *batch.Batch
	selections []int64
	closed     bool
}

func (l *workspacePayloadLease) Close() {
	if l == nil || l.closed {
		return
	}
	l.closed = true
	l.store.release(l.payloadID, l.generation)
}

func normalizeWorkspaceSelections(selections []int64) []int64 {
	if len(selections) == 0 {
		return nil
	}
	ret := slices.Clone(selections)
	slices.Sort(ret)
	return slices.Compact(ret)
}

// workspaceEntryView couples logical metadata with one pinned payload
// generation. It is valid only until the owning workspaceEntrySet is closed.
type workspaceEntryView struct {
	Entry
	statementID uint64
	attemptID   uint64
	selections  []int64
	lease       *workspacePayloadLease
}

func (v *workspaceEntryView) visibleRowCount() int {
	if v == nil || v.bat == nil {
		return 0
	}
	return v.bat.RowCount() - len(v.selections)
}

// forEachVisibleRow applies fn to rows not hidden by this payload generation's
// logical selection set. PayloadStore keeps selections normalized and sorted.
func (v *workspaceEntryView) forEachVisibleRow(fn func(int)) {
	selection := 0
	for row := range v.bat.RowCount() {
		for selection < len(v.selections) && v.selections[selection] < int64(row) {
			selection++
		}
		if selection < len(v.selections) && v.selections[selection] == int64(row) {
			selection++
			continue
		}
		fn(row)
	}
}

// forEachVisibleObjectStats visits the object stats carried by this pinned
// payload generation. It returns false when the entry does not contain an
// object-stats column, allowing callers to distinguish persisted mutations
// from in-memory row mutations without consulting a parallel registry.
func (v *workspaceEntryView) forEachVisibleObjectStats(fn func(objectio.ObjectStats)) bool {
	if v == nil || v.bat == nil {
		return false
	}
	statsIdx := slices.Index(v.bat.Attrs, catalog.ObjectMeta_ObjectStats)
	if statsIdx == -1 {
		return false
	}
	vec := v.bat.Vecs[statsIdx]
	v.forEachVisibleRow(func(row int) {
		fn(objectio.ObjectStats(vec.GetBytesAt(row)))
	})
	return true
}

type workspaceEntrySet struct {
	entries []workspaceEntryView
	closed  bool
}

func (s *workspaceEntrySet) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	for idx := range s.entries {
		if s.entries[idx].lease != nil {
			s.entries[idx].lease.Close()
		}
	}
}
