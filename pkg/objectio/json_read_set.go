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

package objectio

import (
	"context"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var jsonReadSetSize = reflect.TypeFor[JSONReadSet]().Size()

type jsonReadSetKey struct{}

// JSONReadSet is a single synchronous storage Read publication scope. Requested
// logical column ordinals are bound to the reader's physical schema, never to
// Vector addresses. Unsupported row transformations disable the entire scope.
// Published handles are owned here until Close; consumers must retain them.
type JSONReadSet struct {
	selection          *vector.AllocationAccountSelection
	transientSelection *vector.AllocationAccountSelection
	cachePin           JSONCachePinAdmission
	scopeMeta          func()
	requested          []int
	sequences          map[uint16]int
	readers            map[int]vector.JSONReader
	disabled           bool
}

// NewJSONReadSetWithAdmission reserves the read scope's control maps and slice
// before publication. SQL callers must supply the statement selection or its
// transient-memory admission; no scope is prepared for empty demand.
func NewJSONReadSetWithAdmission(columns []int, selection *vector.AllocationAccountSelection, admission vector.JSONReaderMemoryAdmission) (*JSONReadSet, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	if selection == nil && admission == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON read scope requires statement accounting")
	}
	capacity, err := vector.JSONReaderContainerMetadataCapacity(jsonReadSetSize, len(columns))
	if err != nil {
		return nil, err
	}
	release, err := vector.ReserveJSONReaderMetadata(selection, admission, capacity)
	if err != nil {
		return nil, err
	}
	set := NewJSONReadSet(columns, selection)
	set.cachePin = admission
	set.scopeMeta = release
	set.transientSelection = vector.JSONReaderAllocationSelection(selection, admission)
	return set, nil
}

// SetCachePinAdmission binds retained cache backing and reader metadata to the
// read's statement budget. Accounted SQL scopes set this at construction.
func (set *JSONReadSet) SetCachePinAdmission(admission JSONCachePinAdmission) {
	if set != nil && !set.disabled {
		set.cachePin = admission
		set.transientSelection = vector.JSONReaderAllocationSelection(set.selection, admission)
	}
}

func (set *JSONReadSet) CachePinAdmission() JSONCachePinAdmission {
	if set == nil || set.disabled {
		return nil
	}
	return set.cachePin
}

func NewJSONReadSet(columns []int, selections ...*vector.AllocationAccountSelection) *JSONReadSet {
	set := &JSONReadSet{requested: append([]int(nil), columns...), readers: make(map[int]vector.JSONReader)}
	if len(selections) != 0 {
		set.selection = selections[0]
		set.transientSelection = selections[0]
	}
	return set
}
func WithJSONReadSet(ctx context.Context, publication *JSONReadSet) context.Context {
	return context.WithValue(ctx, jsonReadSetKey{}, publication)
}
func JSONReadSetFromContext(ctx context.Context) *JSONReadSet {
	value, _ := ctx.Value(jsonReadSetKey{}).(*JSONReadSet)
	return value
}

// BindColumns is called by the physical reader after resolving output schema.
func (set *JSONReadSet) BindColumns(columns []uint16) {
	if set == nil || set.disabled {
		return
	}
	// A merge reader can exhaust an empty child and bind the next child
	// within this same Read. No version has been published in that case.
	if set.sequences != nil && len(set.readers) != 0 {
		set.Disable()
		return
	}
	set.sequences = make(map[uint16]int, len(set.requested))
	for _, ordinal := range set.requested {
		if ordinal < 0 || ordinal >= len(columns) {
			set.Disable()
			return
		}
		sequence := columns[ordinal]
		if _, duplicate := set.sequences[sequence]; duplicate {
			set.Disable()
			return
		}
		set.sequences[sequence] = ordinal
	}
}
func (set *JSONReadSet) Requested(sequence uint16) bool {
	if set == nil || set.disabled {
		return false
	}
	_, ok := set.sequences[sequence]
	return ok
}

// Publish transfers the handle. Repeated publication is ambiguous (for example
// a concatenated block), so it disables reuse instead of guessing a mapping.
func (set *JSONReadSet) Publish(sequence uint16, reader vector.JSONReader) {
	if reader == nil {
		return
	}
	if !set.Requested(sequence) {
		reader.Close()
		return
	}
	ordinal := set.sequences[sequence]
	if set.readers[ordinal] != nil {
		reader.Close()
		set.Disable()
		return
	}
	set.readers[ordinal] = reader
}
func (set *JSONReadSet) Readers() map[int]vector.JSONReader {
	if set == nil || set.disabled {
		return nil
	}
	return set.readers
}
func (set *JSONReadSet) Disable() {
	if set == nil {
		return
	}
	for _, reader := range set.readers {
		reader.Close()
	}
	set.readers = nil
	set.sequences = nil
	set.requested = nil
	set.cachePin = nil
	set.transientSelection = nil
	if set.scopeMeta != nil {
		set.scopeMeta()
		set.scopeMeta = nil
	}
	set.disabled = true
}
func (set *JSONReadSet) Close() { set.Disable() }

// MatchesJSONColumns excludes nested metadata/tombstone readers from the
// publication scope belonging to the outer data reader.
func (set *JSONReadSet) MatchesJSONColumns(columns []types.Type) bool {
	if set == nil || set.disabled {
		return false
	}
	for _, ordinal := range set.requested {
		if ordinal >= 0 && ordinal < len(columns) && columns[ordinal].Oid == types.T_json {
			return true
		}
	}
	return false
}

// ApplyDeletes composes a storage tombstone mapping without copying payloads.
// The row indices are the exact pre-shrink positions consumed by Vector.Shrink.
func (set *JSONReadSet) ApplyDeletes(deleted []int64, mp *mpool.MPool) (err error) {
	if set == nil || set.disabled || len(set.readers) == 0 || len(deleted) == 0 {
		return nil
	}
	defer func() {
		if err != nil {
			set.Disable()
		}
	}()
	count := -1
	for _, reader := range set.readers {
		if count < 0 {
			count = reader.Length()
		} else if count != reader.Length() {
			return moerr.NewInvalidInputNoCtx("JSON publication column row domains differ")
		}
	}
	for i, row := range deleted {
		if row < 0 || row >= int64(count) || i > 0 && row <= deleted[i-1] {
			return moerr.NewInvalidInputNoCtx("invalid JSON publication delete mapping")
		}
	}
	kept, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), set.transientSelection)
	if err != nil {
		return err
	}
	defer kept.Free(mp)
	nextDelete := 0
	for row := 0; row < count; row++ {
		if nextDelete < len(deleted) && int64(row) == deleted[nextDelete] {
			nextDelete++
			continue
		}
		if err = vector.AppendFixed(kept, int64(row), false, mp); err != nil {
			return err
		}
	}
	return set.SelectRows(vector.MustFixedColNoTypeCheck[int64](kept), mp)
}

// SelectRows composes the exact row positions used by an upstream filter.
// All columns change together; a failed mapping releases the publication.
func (set *JSONReadSet) SelectRows(indices []int64, mp *mpool.MPool) (err error) {
	if set == nil || set.disabled || len(set.readers) == 0 {
		return nil
	}
	mapped := make(map[int]vector.JSONReader, len(set.readers))
	defer func() {
		if err != nil {
			for _, reader := range mapped {
				reader.Close()
			}
			set.Disable()
		}
	}()
	for column, reader := range set.readers {
		var selected vector.JSONReader
		selected, err = reader.Select(indices, mp)
		if err != nil {
			return err
		}
		mapped[column] = selected
	}
	for _, reader := range set.readers {
		reader.Close()
	}
	set.readers = mapped
	return nil
}

// PublishMutableColumns is the admission boundary for memory and received
// batches that still have writable aliases. It copies once per requested
// input version, validates the private copy, and publishes no alias to it.
// The caller must keep the input stable for the duration of this call.
func (set *JSONReadSet) PublishMutableColumns(columns []*vector.Vector, mp *mpool.MPool, rowDomain ...int) (err error) {
	return set.publishMutableColumns(columns, mp, false, rowDomain...)
}

// PublishMutableColumnsForEvaluation declines a malformed candidate without
// publishing any version. The expression evaluator then preserves its own
// NULL and row-mask order and validates each document it actually reads.
func (set *JSONReadSet) PublishMutableColumnsForEvaluation(columns []*vector.Vector, mp *mpool.MPool, rowDomain ...int) error {
	return set.publishMutableColumns(columns, mp, true, rowDomain...)
}

func (set *JSONReadSet) publishMutableColumns(columns []*vector.Vector, mp *mpool.MPool, deferInvalid bool, rowDomain ...int) (err error) {
	if set == nil || set.disabled {
		return nil
	}
	defer func() {
		if err != nil {
			set.Disable()
		}
	}()
	for _, ordinal := range set.requested {
		if ordinal < 0 || ordinal >= len(columns) || columns[ordinal] == nil || columns[ordinal].GetType().Oid != types.T_json {
			return moerr.NewInvalidInputNoCtx("JSON publication input column mismatch")
		}
		if set.readers[ordinal] != nil {
			return moerr.NewInvalidInputNoCtx("JSON input version already published")
		}
		selection := set.selection
		if selection == nil {
			selection = columns[ordinal].AllocationAccountSelection()
		}
		rows := columns[ordinal].Length()
		if len(rowDomain) != 0 {
			rows = rowDomain[0]
		}
		reader, invalid, snapshotErr := vector.TryNewJSONReaderSnapshotForRows(columns[ordinal], rows, mp, selection, set.cachePin)
		if snapshotErr != nil {
			if deferInvalid && invalid {
				set.Disable()
				return nil
			}
			return snapshotErr
		}
		set.readers[ordinal] = reader
	}
	return nil
}
