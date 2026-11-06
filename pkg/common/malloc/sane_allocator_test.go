// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package malloc

import (
	"context"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func BenchmarkSimpleCAllocatorAllocate(b *testing.B) {
	for _, size := range []uint64{128, 4096} {
		b.Run(strconv.FormatUint(size, 10), func(b *testing.B) {
			sca := NewSimpleCAllocator(nil, nil, nil, nil, nil)
			b.ReportAllocs()
			for b.Loop() {
				buf, err := sca.Allocate(size)
				if err != nil {
					b.Fatal(err)
				}
				sca.Deallocate(buf, size)
			}
		})
	}
}

func TestSimpleCAllocatorCallocFailure(t *testing.T) {
	fault.EnableDomain(fault.DomainTest)
	t.Cleanup(func() { fault.DisableDomain(fault.DomainTest) })
	sca := NewSimpleCAllocator(nil, nil, nil, nil, nil)
	for _, failure := range []string{"oom", "unrelated native failure"} {
		require.NoError(t, fault.AddFaultPointInDomain(context.Background(), fault.DomainTest,
			"simple_c_allocator_calloc", ":::", "echo", 128, failure, false))
		buf, err := sca.Allocate(128)
		require.Nil(t, buf)
		if failure == "oom" {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
		} else {
			require.ErrorContains(t, err, failure)
		}
		require.Zero(t, sca.currentInuse.Load(), "failed calloc must not register physical ownership")
		// The hook targets only its exact size; other calloc requests still work.
		func() {
			buf, err := sca.Allocate(64)
			require.NoError(t, err)
			defer sca.Deallocate(buf, 64)
			require.Equal(t, int64(64), sca.currentInuse.Load())
		}()
		require.Zero(t, sca.currentInuse.Load())
	}
}
