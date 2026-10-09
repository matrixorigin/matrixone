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
	"fmt"
	"sync"
)

type memoryCleanupRootRepository struct {
	mu    sync.Mutex
	roots map[string]CleanupRoot
}

func newMemoryCleanupRootRepository() *memoryCleanupRootRepository {
	return &memoryCleanupRootRepository{roots: make(map[string]CleanupRoot)}
}

func (repository *memoryCleanupRootRepository) Register(
	_ context.Context,
	root CleanupRoot,
) error {
	if err := ValidateCleanupRoot(root); err != nil {
		return err
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if _, exists := repository.roots[root.RootID]; exists {
		return fmt.Errorf("Cleanup Root already exists")
	}
	repository.roots[root.RootID] = root
	return nil
}

func (repository *memoryCleanupRootRepository) Get(
	_ context.Context,
	rootID string,
) (CleanupRoot, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	root, exists := repository.roots[rootID]
	if !exists {
		return CleanupRoot{}, fmt.Errorf("Cleanup Root not found")
	}
	return root, nil
}

func (repository *memoryCleanupRootRepository) HasUnresolvedSource(
	_ context.Context,
	ownerAccountID uint32,
	physicalTableID uint64,
	sourceSetDigest [32]byte,
) (bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	for _, root := range repository.roots {
		if root.OwnerAccountID == ownerAccountID &&
			root.PhysicalTableID == physicalTableID &&
			root.SourceSetDigest == sourceSetDigest &&
			(root.State == CleanupRootFinalizing ||
				root.State == CleanupRootCommitUnknown) {
			return true, nil
		}
	}
	return false, nil
}

func (repository *memoryCleanupRootRepository) Transition(
	_ context.Context,
	rootID string,
	attemptID string,
	executorEpoch uint64,
	from CleanupRootState,
	expectedVersion uint64,
	to CleanupRootState,
) (CleanupRoot, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	root, exists := repository.roots[rootID]
	if !exists ||
		root.AttemptID != attemptID ||
		root.ExecutorEpoch != executorEpoch ||
		root.State != from ||
		root.StateVersion != expectedVersion ||
		!validateCleanupRootTransition(from, to) {
		return CleanupRoot{}, fmt.Errorf("Cleanup Root transition CAS failed")
	}
	root.State = to
	root.StateVersion++
	repository.roots[rootID] = root
	return root, nil
}

func (repository *memoryCleanupRootRepository) UpdateCleanup(
	_ context.Context,
	root CleanupRoot,
	expectedVersion uint64,
) (CleanupRoot, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, exists := repository.roots[root.RootID]
	if !exists ||
		current.AttemptID != root.AttemptID ||
		current.State != root.State ||
		current.StateVersion != expectedVersion {
		return CleanupRoot{}, fmt.Errorf("Cleanup Root update CAS failed")
	}
	root.StateVersion++
	repository.roots[root.RootID] = root
	return root, nil
}
