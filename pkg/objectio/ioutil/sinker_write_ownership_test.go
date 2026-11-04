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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestFailedWriteReturnsStagedBufferOnce(t *testing.T) {
	for _, tc := range []struct {
		name            string
		firstRows, rows int
	}{
		{name: "partial block", rows: 100},
		{name: "full block", rows: int(objectio.BlockMaxRows)},
		{name: "previous full block", firstRows: int(objectio.BlockMaxRows), rows: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs, typs, _ := mockSchema(3, 2)
			wantErr := errors.New("injected sink failure")
			threshold := 1
			if tc.firstRows > 0 {
				threshold = 1 << 30
			}
			sinker := NewSinker(-1, attrs, typs, mockFactory(wantErr, nil), proc.Mp(), fs, WithMemorySizeThreshold(threshold))
			t.Cleanup(func() { _ = sinker.Close() })
			if tc.firstRows > 0 {
				first := containers.MockBatch(typs, tc.firstRows, 2, nil)
				defer first.Close()
				require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(first)))
				sinker.staged.memorySizeThreshold = 1
			}
			data := containers.MockBatch(typs, tc.rows, 2, nil)
			defer data.Close()
			require.ErrorIs(t, sinker.Write(context.Background(), containers.ToCNBatch(data)), wantErr)
			require.Empty(t, sinker.staged.inMemory)
			require.Equal(t, 1, sinker.buf.buffers.Len())
			first := sinker.buf.buffers.FetchWithSchema(attrs, typs)
			second := sinker.buf.buffers.FetchWithSchema(attrs, typs)
			require.NotSame(t, first, second)
			sinker.putBackBuffer(first)
			sinker.putBackBuffer(second)
		})
	}
}
