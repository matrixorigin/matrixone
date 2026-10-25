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

package colexec

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type failedSyncSinker struct{ err error }

func (*failedSyncSinker) Sink(context.Context, *batch.Batch) error              { return nil }
func (s *failedSyncSinker) Sync(context.Context) (*objectio.ObjectStats, error) { return nil, s.err }
func (*failedSyncSinker) Reset()                                                {}
func (*failedSyncSinker) Close() error                                          { return nil }

func TestCNS3WriterCloseReleasesMetadataAfterPipelineError(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	input := batch.NewWithSize(1)
	input.Attrs = []string{"id"}
	input.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
	input.SetRowCount(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	wantBytes := proc.Mp().CurrNB()
	writeErr := errors.New("injected pipeline sync failure")
	w := &CNS3Writer{sinker: ioutil.NewSinker(-1, input.Attrs, []types.Type{types.T_int64.ToType()},
		func(*mpool.MPool, fileservice.FileService) ioutil.FileSinker {
			return &failedSyncSinker{err: writeErr}
		}, proc.Mp(), nil, ioutil.WithPipelineFlush(), ioutil.WithMemorySizeThreshold(1), ioutil.WithTailSizeCap(0))}
	w.ResetBlockInfoBat()
	metadata := w.blockInfoBat
	t.Cleanup(func() {
		_ = w.Close()
		// Keep the failure oracle from leaking under the original Close path.
		metadata.Clean(proc.Mp())
	})
	require.NoError(t, vector.AppendBytes(w.blockInfoBat.Vecs[0], make([]byte, 1024), false, proc.Mp()))
	require.Positive(t, metadata.Allocated())
	require.NoError(t, w.Write(proc.Ctx, input))
	_, err := w.Sync(proc.Ctx)
	require.ErrorIs(t, err, writeErr)
	require.ErrorIs(t, w.Close(), writeErr)
	require.Nil(t, w.sinker)
	require.Nil(t, w.blockInfoBat)
	require.Zero(t, metadata.Allocated(), "metadata buffers must be freed, not only detached")
	require.Equal(t, wantBytes, proc.Mp().CurrNB(), "close error must not abandon metadata allocations")
	require.NoError(t, w.Close())
}
