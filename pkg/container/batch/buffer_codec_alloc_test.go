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

//go:build !race

package batch

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Race instrumentation changes allocation accounting. Byte compatibility and
// streaming failures are independently covered by stream_codec_test.go in race.
func TestBufferCodecSteadyStateAllocations(t *testing.T) {
	for _, rows := range []int{1, 8192} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			b := New([]string{"v"})
			t.Cleanup(func() { b.Clean(mp) })
			b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(b.Vecs[0], make([]int64, rows), nil, mp))
			b.SetRowCount(rows)
			var buffer bytes.Buffer
			buffer.Grow(int(b.MarshalBinarySize()))
			for _, encode := range []struct {
				name string
				run  func() error
			}{
				{"vector-buffer", func() error { return b.Vecs[0].MarshalBinaryWithBuffer(&buffer) }},
				{"vector-writer-buffer", func() error { return b.Vecs[0].MarshalBinaryTo(&buffer) }},
				{"batch-buffer", func() error { _, err := b.MarshalBinaryWithBuffer(&buffer, false); return err }},
				{"batch-writer-buffer", func() error { return b.MarshalBinaryTo(&buffer) }},
			} {
				t.Run(encode.name, func(t *testing.T) {
					allocations := testing.AllocsPerRun(100, func() {
						buffer.Reset()
						if err := encode.run(); err != nil {
							panic(err)
						}
					})
					require.Zero(t, allocations, "reusing a buffer must not allocate scalar headers")
				})
			}
		})
	}
}
