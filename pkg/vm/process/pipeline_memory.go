// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"math"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

// PipelineEdgeMetadataBound covers one local edge and a conservative share of
// receiver/attempt bookkeeping. Shared edges are intentionally charged once per
// sending spool receiver, so their actual allocation can only be smaller.
// channelCapacity bounds the final execution graph, including parallelization.
func PipelineEdgeMetadataBound(channelCapacity uint64) (int64, bool) {
	if channelCapacity == 0 || channelCapacity > uint64(math.MaxInt) {
		return 0, false
	}
	var total uint64
	add := func(count, width uint64, scanned bool) bool {
		if count != 0 && width > math.MaxUint64/count {
			return false
		}
		bytes := count * width
		if scanned {
			if bytes > math.MaxUint64-8 {
				return false
			}
			bytes += 8
		}
		rounded, ok := malloc.ClassAllocationSize(bytes)
		if !ok || rounded > math.MaxInt64-total {
			return false
		}
		total += rounded
		return true
	}
	// PipelineEdge includes its durable terminal, mutex, atomics and Once.
	// A reuse may replace Ch2/Done while the previous empty generation has not
	// yet been collected. Charge both generations, including separate scanned
	// channel payload allocations. 256 bounds hchan/alignment on supported Go.
	if !add(1, uint64(unsafe.Sizeof(PipelineEdge{})), true) {
		return 0, false
	}
	for range 2 {
		if !add(1, 256, true) || !add(channelCapacity, uint64(unsafe.Sizeof(PipelineSignal{})), true) || !add(1, 256, false) {
			return 0, false
		}
	}
	// Overcharge one whole receiver per edge. Its arrays are rounded separately
	// per element, covering aggregate size-class rounding and the 2*n+1 reflect
	// cases (the extra context case is covered even for fan-in one).
	if !add(1, uint64(unsafe.Sizeof(pipelineSpoolRegistry{})), true) {
		return 0, false
	}
	if !add(1, uint64(unsafe.Sizeof(PipelineSignalReceiver{})), true) {
		return 0, false
	}
	for _, width := range []uintptr{unsafe.Sizeof((*WaitRegister)(nil)), unsafe.Sizeof((<-chan struct{})(nil)), unsafe.Sizeof(int(0)), unsafe.Sizeof(reflect.SelectCase{}), unsafe.Sizeof(reflect.SelectCase{}), unsafe.Sizeof(reflect.SelectCase{})} {
		if !add(1, uint64(width), true) {
			return 0, false
		}
	}
	// Reg.MergeReceivers plus append growth of the spool/edge registry. For n
	// entries append capacity is below 2*n; charge four pointers per owner so
	// small scanned allocation headers and rounding are also independently bound.
	for range 4 {
		if !add(4, uint64(unsafe.Sizeof(uintptr(0))), true) {
			return 0, false
		}
	}
	return int64(total), true
}
