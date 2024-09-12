// Copyright 2024 Matrix Origin
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

package malloc

type LeaksTrackingAllocator[U Allocator] struct {
	upstream        U
	deallocatorPool *ClosureDeallocatorPool[leaksTrackingDeallocatorArgs, *leaksTrackingDeallocatorArgs]
	tracker         *LeaksTracker
}

type leaksTrackingDeallocatorArgs struct {
	stacktrace Stacktrace
}

func (leaksTrackingDeallocatorArgs) As(Trait) bool {
	return false
}

func NewLeaksTrackingAllocator[U Allocator](
	upstream U,
	tracker *LeaksTracker,
) (ret *LeaksTrackingAllocator[U]) {

	ret = &LeaksTrackingAllocator[U]{
		upstream: upstream,
		tracker:  tracker,

		deallocatorPool: NewClosureDeallocatorPool(
			func(hints Hints, args *leaksTrackingDeallocatorArgs) {
				ret.tracker.deallocate(args.stacktrace)
			},
		),
	}

	return ret
}

var _ Allocator = new(LeaksTrackingAllocator[Allocator])

func (t *LeaksTrackingAllocator[U]) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	slice, dec, err := t.upstream.Allocate(size, hints)
	if err != nil {
		return nil, nil, err
	}
	stacktraceID := GetStacktrace(0)
	t.tracker.allocate(stacktraceID)
	return slice, ChainDeallocator(
		dec,
		t.deallocatorPool.Get(leaksTrackingDeallocatorArgs{
			stacktrace: stacktraceID,
		}),
	), nil
}
