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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

var spillInjectedError = errors.New("injected sink failure")

type spillFailureSink struct {
	mode     string
	received *batch.Batch
}

func (s *spillFailureSink) Sink(_ context.Context, b *batch.Batch) error {
	s.received = b
	switch s.mode {
	case "error":
		return spillInjectedError
	case "panic":
		panic(spillInjectedError)
	}
	return nil
}
func (*spillFailureSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*spillFailureSink) Reset()       {}
func (*spillFailureSink) Close() error { return nil }

func TestStreamingSpillFailureBufferOwnership(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs, typs, _ := mockSchema(3, 2)
			file := &spillFailureSink{mode: mode}
			sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), fs, WithMemorySizeThreshold(1), WithOffHeap())
			data := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
			defer data.Close()
			before := proc.Mp().CurrNB()
			var gotErr error
			var gotPanic any
			func() {
				defer func() { gotPanic = recover() }()
				gotErr = sinker.Write(context.Background(), containers.ToCNBatch(data))
			}()
			require.NoError(t, sinker.Close())
			leaked := proc.Mp().CurrNB() - before
			// Reclaim the orphan only in a failing run to avoid contaminating other tests.
			if leaked != 0 && file.received != nil {
				file.received.Clean(proc.Mp())
			}
			require.NotNil(t, file.received)
			switch mode {
			case "success":
				require.NoError(t, gotErr)
				require.Nil(t, gotPanic)
			case "error":
				require.ErrorIs(t, gotErr, spillInjectedError)
				require.Nil(t, gotPanic)
			case "panic":
				require.Equal(t, spillInjectedError, gotPanic)
			}
			require.Zero(t, leaked, "mpool bytes retained after Close")
		})
	}
}
