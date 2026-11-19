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

package ioutil

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type trackingStreamSinker struct {
	received *batch.Batch
	err      error
	rows     int
}

func (s *trackingStreamSinker) Sink(_ context.Context, b *batch.Batch) error {
	s.received = b
	if s.err != nil {
		return s.err
	}
	s.rows += b.RowCount()
	return nil
}

func (*trackingStreamSinker) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*trackingStreamSinker) Reset()       {}
func (*trackingStreamSinker) Close() error { return nil }

func TestStreamingSpillReleasesMergeBuffer(t *testing.T) {
	for _, offHeap := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			for _, rows := range []int{100, int(objectio.BlockMaxRows)} {
				t.Run(fmt.Sprintf("offHeap=%t/fail=%t/rows=%d", offHeap, fail, rows), func(t *testing.T) {
					proc := testutil.NewProc(t)
					fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
					require.NoError(t, err)
					attrs, typs, _ := mockSchema(3, 2)
					wantErr := errors.New("injected stream sink failure")
					file := &trackingStreamSinker{}
					if fail {
						file.err = wantErr
					}
					opts := []SinkerOption{WithMemorySizeThreshold(1)}
					if offHeap {
						opts = append(opts, WithOffHeap())
					}
					sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), fs, opts...)
					t.Cleanup(func() { _ = sinker.Close() })
					data := containers.MockBatch(typs, rows, 2, nil)
					defer data.Close()
					beforeBytes, beforeObjects := proc.Mp().OnHeapOutstanding()
					beforeOffHeap := proc.Mp().CurrNB()
					err = sinker.Write(context.Background(), containers.ToCNBatch(data))
					if fail {
						require.ErrorIs(t, err, wantErr)
					} else {
						require.NoError(t, err)
						require.Equal(t, rows, file.rows)
					}
					require.NotNil(t, file.received)
					require.NoError(t, sinker.Close())
					afterBytes, afterObjects := proc.Mp().OnHeapOutstanding()
					afterOffHeap := proc.Mp().CurrNB()
					if afterBytes != beforeBytes || afterObjects != beforeObjects || afterOffHeap != beforeOffHeap {
						// Release a leaked buffer before reporting a failing baseline.
						file.received.Clean(proc.Mp())
					}
					require.Equal(t, beforeBytes, afterBytes, "on-heap bytes after close")
					require.Equal(t, beforeObjects, afterObjects, "on-heap objects after close")
					require.Equal(t, beforeOffHeap, afterOffHeap, "off-heap bytes after close")
				})
			}
		}
	}
}
