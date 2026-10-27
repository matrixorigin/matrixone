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
	"context"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestPipelineEdgeMetadataCoversConstructedOwners(t *testing.T) {
	for _, fanIn := range []int{1, 2, 8, 9, 17} {
		regs := make([]*WaitRegister, fanIn)
		for i := range regs {
			regs[i] = NewPipelineEdge(33, 1)
		}
		receiver := InitPipelineSignalReceiver(context.Background(), regs)
		perEdge, ok := PipelineEdgeMetadataBound(33)
		require.True(t, ok)
		// Independent concrete inventory, including each actual receiver array,
		// durable edge struct, two hchan headers and scanned signal payload.
		live := uint64(unsafe.Sizeof(*receiver)) + uint64(cap(receiver.srcReg))*uint64(unsafe.Sizeof(regs[0])) + uint64(cap(receiver.doneCh))*uint64(unsafe.Sizeof(regs[0].done)) + uint64(cap(receiver.nbs))*uint64(unsafe.Sizeof(int(0))) + uint64(cap(receiver.regs))*uint64(unsafe.Sizeof(reflect.SelectCase{}))
		for _, reg := range regs {
			live += uint64(unsafe.Sizeof(*reg)) + 512 + uint64(cap(reg.Ch2))*uint64(unsafe.Sizeof(PipelineSignal{}))
		}
		require.GreaterOrEqual(t, uint64(perEdge)*uint64(fanIn), live)
	}
	_, ok := PipelineEdgeMetadataBound(math.MaxUint64)
	require.False(t, ok)
	_, ok = PipelineEdgeMetadataBound(0)
	require.False(t, ok)
}
