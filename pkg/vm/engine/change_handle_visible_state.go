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

// VisibleStateEntry is one encoded logical row retained while a visible-state
// changes handle compares two snapshot boundaries. Key is the encoded primary
// key and Value is private to the changes-handle implementation.
type VisibleStateEntry struct {
	Key   []byte
	Value []byte
}

// VisibleStateStore is the bounded, spillable key/value store required by
// exact visible-state recovery. Pop and Drain return detached byte slices.
type VisibleStateStore interface {
	PutBatch(entries []VisibleStateEntry) error
	Pop(key []byte) (value []byte, ok bool, err error)
	Drain(maxEntries int, fn func(key, value []byte) error) (int, error)
	Len() int64
	Close() error
}

// VisibleStateRecoveryResources owns both storage for the before snapshot and
// admission for batches buffered while replay is still recoverable. Data
// Branch supplies one implementation backed by its shared memory throttler.
type VisibleStateRecoveryResources interface {
	NewVisibleStateStore() (VisibleStateStore, error)
	ReserveBuffer(bytes int64) error
	ReleaseBuffer(bytes int64)
}

type visibleStateStartRelationKey struct{}

// WithVisibleStateStartRelation supplies the protected lineage boundary that
// precedes a CollectChanges range. Data Branch uses the parent relation at a
// clone or ALTER edge so recovery does not depend on the child table's
// unprotected materialization objects.
func WithVisibleStateStartRelation(ctx context.Context, relation Relation) context.Context {
	if ctx == nil || relation == nil {
		return ctx
	}
	return context.WithValue(ctx, visibleStateStartRelationKey{}, relation)
}

func VisibleStateStartRelationFromContext(ctx context.Context) Relation {
	if ctx == nil {
		return nil
	}
	relation, _ := ctx.Value(visibleStateStartRelationKey{}).(Relation)
	return relation
}

type visibleStateRecoveryResourcesKey struct{}

// WithVisibleStateRecoveryResources attaches the bounded resources required
// by SnapshotReadPolicyVisibleState. Nil resources leave the context unchanged.
func WithVisibleStateRecoveryResources(
	ctx context.Context,
	resources VisibleStateRecoveryResources,
) context.Context {
	if ctx == nil || resources == nil {
		return ctx
	}
	return context.WithValue(ctx, visibleStateRecoveryResourcesKey{}, resources)
}

// VisibleStateRecoveryResourcesFromContext returns the resources selected by
// the visible-state caller.
func VisibleStateRecoveryResourcesFromContext(
	ctx context.Context,
) VisibleStateRecoveryResources {
	if ctx == nil {
		return nil
	}
	resources, _ := ctx.Value(visibleStateRecoveryResourcesKey{}).(VisibleStateRecoveryResources)
	return resources
}
