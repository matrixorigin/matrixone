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

package hashbuild

import (
	"reflect"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (b *HashmapBuilder) marshalMembershipKeys(proc *process.Process) (*keycodec.KeyPayload, error) {
	count := 1 + len(b.uniqueKeys.rest)
	bytes := uint64(count) * uint64(reflect.TypeFor[[]byte]().Size()+reflect.TypeFor[func()]().Size())
	reservation, err := b.mapAllocationAccount.ReserveCapacity(bytes, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
	if err != nil {
		return nil, err
	}
	defer reservation.Abort()
	lease, err := reservation.Commit(bytes)
	if err != nil {
		return nil, err
	}
	chunks := make([][]byte, 0, count)
	releases := make([]func(), 0, count)
	cleanup := func() {
		for _, release := range releases {
			release()
		}
		lease.Release()
	}
	owned := true
	defer func() {
		if owned {
			cleanup()
		}
	}()
	totalBytes := int64(bytes)
	err = b.uniqueKeys.visit(func(keys []*vector.Vector) error {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		if len(keys) == 0 || keys[0] == nil {
			return process.ErrExecutionResourceInvalid
		}
		data, release, err := b.marshalRuntimeFilterVector(keys[0], proc.Mp())
		if err != nil {
			return err
		}
		chunks = append(chunks, data)
		releases = append(releases, release)
		totalBytes += int64(cap(data))
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys, err := keycodec.NewKeyPayload(proc.Ctx, chunks, totalBytes, cleanup)
	if err != nil {
		return nil, err
	}
	owned = false
	return keys, nil
}

func (h *HashBuild) materializeSerializedRuntimeFilterRuns(proc *process.Process, spec *plan.RuntimeFilterSpec, componentTypes []types.Type) ([]byte, func(), int, bool, error) {
	b := &h.ctr.hashmapBuilder
	count := 1 + len(b.uniqueKeys.rest)
	reservation, err := b.mapAllocationAccount.ReserveCapacity(uint64(count)*uint64(reflect.TypeFor[*vector.Vector]().Size()), mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterScratch)
	if err != nil {
		return nil, nil, 0, false, err
	}
	defer reservation.Abort()
	lease, err := reservation.Commit(reservation.UpperBound())
	if err != nil {
		return nil, nil, 0, false, err
	}
	defer lease.Release()
	runs := make([]*vector.Vector, 0, count)
	defer func() {
		for _, v := range runs {
			v.Free(proc.Mp())
		}
	}()
	executors, ok, err := h.serializedRuntimeFilterExecutors(proc, spec)
	if err != nil || !ok {
		return nil, nil, 0, false, err
	}
	defer freeRuntimeFilterExecutors(executors)
	rows := 0
	usable := true
	err = b.uniqueKeys.visit(func(keys []*vector.Vector) error {
		currentTypes, n, valid := materializedRuntimeFilterComponents(spec, keys)
		if !valid || !slices.Equal(currentTypes, componentTypes) {
			usable = false
			return nil
		}
		v, n, valid, err := h.materializeSerializedRuntimeFilterChunk(proc, spec, componentTypes, n, keys, executors)
		if err != nil {
			return err
		}
		if !valid {
			usable = false
			return nil
		}
		runs = append(runs, v)
		rows += n
		return nil
	})
	if err != nil || !usable || rows == 0 {
		return nil, nil, rows, usable, err
	}
	data, release, err := runtimefilter.MarshalExactFilterRuns(proc.Ctx, runs, proc.Mp(), b.mapAllocationAccount, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
	return data, release, rows, true, err
}

func (h *HashBuild) handleDirectRuntimeFilterRuns(proc *process.Process, msg *message.RuntimeFilterMessage, spec *plan.RuntimeFilterSpec, slot int, encoding keycodec.ExactRuntimeFilterEncoding) error {
	b := &h.ctr.hashmapBuilder
	pass := func() error { msg.Typ = message.RuntimeFilter_PASS; h.sendRuntimeFilter(*msg, spec, proc); return nil }
	if b.GetGroupCount() > uint64(spec.UpperLimit) || b.uniqueKeys.count() > int(spec.UpperLimit) {
		return pass()
	}
	count := 1 + len(b.uniqueKeys.rest)
	reservation, err := b.mapAllocationAccount.ReserveCapacity(uint64(count)*uint64(reflect.TypeFor[*vector.Vector]().Size()), mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterScratch)
	if err != nil {
		if h.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
			return nil
		}
		return err
	}
	defer reservation.Abort()
	lease, err := reservation.Commit(reservation.UpperBound())
	if err != nil {
		return err
	}
	defer lease.Release()
	runs := make([]*vector.Vector, 0, count)
	rows := 0
	err = b.uniqueKeys.visit(func(vecs []*vector.Vector) error {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		if slot >= len(vecs) || vecs[slot] == nil {
			return process.ErrExecutionResourceInvalid
		}
		v := vecs[slot]
		if encoding == keycodec.ExactRuntimeFilterFloatZeroClosed {
			if err := runtimefilter.CloseFloatSignedZero(v, proc.Mp()); err != nil {
				return err
			}
		}
		v.GetNulls().Reset()
		if err := v.InplaceSort(proc.Mp()); err != nil {
			return err
		}
		runs = append(runs, v)
		rows += v.Length()
		return nil
	})
	if err != nil {
		if h.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
			return nil
		}
		return err
	}
	if rows > int(spec.UpperLimit) {
		return pass()
	}
	data, release, err := runtimefilter.MarshalExactFilterRuns(proc.Ctx, runs, proc.Mp(), b.mapAllocationAccount, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
	if err != nil {
		if h.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
			return nil
		}
		return err
	}
	msg.Typ = message.RuntimeFilter_IN
	msg.Card = int32(rows)
	msg.Data = data
	msg.SetMemoryRelease(release)
	h.sendRuntimeFilter(*msg, spec, proc)
	h.ctr.runtimeFilterIn = true
	return nil
}
