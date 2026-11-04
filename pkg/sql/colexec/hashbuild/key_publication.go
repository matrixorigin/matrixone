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

func (hb *HashmapBuilder) marshalMembershipKeys(proc *process.Process) (*keycodec.KeyPayload, error) {
	if err := proc.Ctx.Err(); err != nil {
		return nil, err
	}
	count := 1 + len(hb.uniqueKeys.rest)
	if count == 1 {
		data, release, err := hb.marshalRuntimeFilterVector(hb.uniqueKeys.first.vecs[0], proc.Mp())
		if err != nil {
			return nil, err
		}
		keys, err := keycodec.NewTrustedSingleKeyPayload(proc.Ctx, data, int64(cap(data)), release)
		if err != nil {
			release()
		}
		return keys, err
	}
	bytes := uint64(count) * uint64(reflect.TypeFor[[]byte]().Size()+reflect.TypeFor[func()]().Size())
	lease, err := hb.runtimeKeyMetadata(bytes, HashBuildAllocationSiteRuntimeFilterPayload)
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
	scratch, err := hb.runtimeKeyMetadata(uint64(count)*uint64(reflect.TypeFor[vector.MarshalBinaryPlan]().Size()), HashBuildAllocationSiteRuntimeFilterScratch)
	if err != nil {
		return nil, err
	}
	defer scratch.Release()
	plans := make([]vector.MarshalBinaryPlan, count)
	index := 0
	err = hb.uniqueKeys.visit(func(keys []*vector.Vector) error {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		if len(keys) == 0 || keys[0] == nil || keys[0].HasNull() {
			return process.ErrExecutionResourceInvalid
		}
		var err error
		plans[index], err = keys[0].PrepareMarshalBinary()
		if err != nil {
			return err
		}
		if int64(plans[index].Size()) > mpool.MaxAllocationSize() {
			return runtimefilter.ErrOptionalKeySize
		}
		index++
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Frames remain independently bounded, but share bounded backing arenas:
	// the complete payload has one lifetime, so per-frame MPool allocations
	// and release closures only add overhead. Wide frames get their own arena.
	const arenaBytes = 1 << 20
	totalBytes := int64(bytes)
	for start := 0; start < count; {
		end, size := start+1, plans[start].Size()
		for end < count && size <= arenaBytes && plans[end].Size() <= arenaBytes-size {
			size += plans[end].Size()
			end++
		}
		buf, err := mpool.NewAccountedBuffer(proc.Mp(), hb.mapAllocationAccount, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
		if err != nil {
			return nil, err
		}
		releases = append(releases, buf.Free)
		if err = buf.EnsureCapacity(size); err != nil {
			return nil, runtimefilter.MarkOptionalAllocationError(err)
		}
		totalBytes += int64(buf.Cap())
		for i := start; i < end; i++ {
			if err := proc.Ctx.Err(); err != nil {
				return nil, err
			}
			offset := buf.Len()
			if err := plans[i].MarshalTo(buf); err != nil {
				return nil, err
			}
			chunks = append(chunks, buf.Bytes()[offset:buf.Len():buf.Len()])
		}
		start = end
	}
	keys, err := keycodec.NewTrustedKeyPayload(proc.Ctx, chunks, totalBytes, cleanup)
	if err != nil {
		return nil, err
	}
	owned = false
	return keys, nil
}

func (hb *HashmapBuilder) runtimeKeyMetadata(bytes uint64, site mpool.AllocationSite) (*mpool.CapacityLease, error) {
	reservation, err := hb.mapAllocationAccount.ReserveCapacity(bytes, mpool.AllocationOwnerHashBuild, site)
	if err != nil {
		return nil, err
	}
	defer reservation.Abort()
	return reservation.Commit(bytes)
}

func (hashBuild *HashBuild) materializeSerializedRuntimeFilterRuns(proc *process.Process, spec *plan.RuntimeFilterSpec, componentTypes []types.Type) ([]byte, func(), int, bool, error) {
	hb := &hashBuild.ctr.hashmapBuilder
	count := 1 + len(hb.uniqueKeys.rest)
	reservation, err := hb.mapAllocationAccount.ReserveCapacity(uint64(count)*uint64(reflect.TypeFor[*vector.Vector]().Size()), mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterScratch)
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
	executors, ok, err := hashBuild.serializedRuntimeFilterExecutors(proc, spec)
	if err != nil || !ok {
		return nil, nil, 0, false, err
	}
	defer freeRuntimeFilterExecutors(executors)
	rows := 0
	usable := true
	err = hb.uniqueKeys.visit(func(keys []*vector.Vector) error {
		currentTypes, n, valid := materializedRuntimeFilterComponents(spec, keys)
		if !valid || !slices.Equal(currentTypes, componentTypes) {
			usable = false
			return nil
		}
		v, n, valid, err := hashBuild.materializeSerializedRuntimeFilterChunk(proc, spec, componentTypes, n, keys, executors)
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
	data, release, err := runtimefilter.MarshalExactFilterRuns(proc.Ctx, runs, proc.Mp(), hb.mapAllocationAccount, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
	return data, release, rows, true, err
}

func (hashBuild *HashBuild) handleDirectRuntimeFilterRuns(proc *process.Process, msg *message.RuntimeFilterMessage, spec *plan.RuntimeFilterSpec, slot int, encoding keycodec.ExactRuntimeFilterEncoding) error {
	hb := &hashBuild.ctr.hashmapBuilder
	pass := func() error {
		msg.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*msg, spec, proc)
		return nil
	}
	if hb.GetGroupCount() > uint64(spec.UpperLimit) || hb.uniqueKeys.count() > int(spec.UpperLimit) {
		return pass()
	}
	count := 1 + len(hb.uniqueKeys.rest)
	reservation, err := hb.mapAllocationAccount.ReserveCapacity(uint64(count)*uint64(reflect.TypeFor[*vector.Vector]().Size()), mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterScratch)
	if err != nil {
		if hashBuild.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
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
	sortInEncoding := runtimefilter.ExactFilterDataSorter(*hb.uniqueKeys.first.vecs[slot].GetType()) != nil
	err = hb.uniqueKeys.visit(func(vecs []*vector.Vector) error {
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
		if !sortInEncoding {
			if err := v.InplaceSort(proc.Mp()); err != nil {
				return err
			}
		}
		runs = append(runs, v)
		rows += v.Length()
		return nil
	})
	if err != nil {
		if hashBuild.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
			return nil
		}
		return err
	}
	if rows > int(spec.UpperLimit) {
		return pass()
	}
	data, release, err := runtimefilter.MarshalExactFilterRuns(proc.Ctx, runs, proc.Mp(), hb.mapAllocationAccount, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteRuntimeFilterPayload)
	if err != nil {
		if hashBuild.fallbackOptionalRuntimeFilter(err, msg, spec, proc) {
			return nil
		}
		return err
	}
	msg.Typ = message.RuntimeFilter_IN
	msg.Card = int32(rows)
	msg.Data = data
	msg.SetMemoryRelease(release)
	hashBuild.sendRuntimeFilter(*msg, spec, proc)
	hashBuild.ctr.runtimeFilterIn = true
	return nil
}
