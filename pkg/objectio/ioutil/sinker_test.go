// Copyright 2021 Matrix Origin
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

package ioutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockSchema(colCnt int, pkIdx int) ([]string, []types.Type, []uint16) {
	attrs := make([]string, 0)
	typs := make([]types.Type, 0)
	seq := make([]uint16, 0)
	for i := 0; i < colCnt; i++ {
		colName := fmt.Sprintf("mock_%d", i)
		attrs = append(attrs, colName)
		typs = append(typs, types.T_int32.ToType())
		seq = append(seq, uint16(i))
	}
	return attrs, typs, seq
}

func TestNewSinker(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	attrs, typs, seqnums := mockSchema(3, 2)

	buffer := containers.NewOneSchemaBatchBuffer(mpool.GB, attrs, typs)

	factory := NewFSinkerImplFactory(
		seqnums,
		2,
		true,
		false,
		0,
	)

	sinker := NewSinker(
		2,
		attrs,
		typs,
		factory,
		proc.Mp(),
		fs,
		WithMemorySizeThreshold(1),
		WithBufferSizeCap(1),
		WithAllMergeSorted(),
		WithDedupAll(),
		WithTailSizeCap(1),
		WithBuffer(buffer, false),
	)

	bat := containers.MockBatch(typs, 1000, 2, nil)
	err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
	assert.Nil(t, err)

	err = sinker.Sync(context.Background())
	assert.Nil(t, err)

	require.Equal(t, 0, len(sinker.staged.inMemory))
	require.Equal(t, 0, len(sinker.staged.persisted))

	rows := 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}

	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}

	require.Equal(t, 1000, rows)

	require.NoError(t, sinker.Close())
	buffer.Close(proc.Mp())

	require.Equal(t, 0, int(proc.Mp().CurrNB()))
}

func makeTestSinker(
	mp *mpool.MPool,
	fs fileservice.FileService,
) (attr []string, typs []types.Type, seq []uint16, sinker *Sinker) {
	attr, typs, seq = mockSchema(3, 2)

	factory := NewFSinkerImplFactory(
		seq,
		2,
		true,
		false,
		0,
	)

	sinker = NewSinker(
		2,
		attr,
		typs,
		factory,
		mp,
		fs,
	)
	return
}

func TestNewSinker2(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	_, typs, _, sinker := makeTestSinker(proc.Mp(), fs)

	for i := 0; i < 5; i++ {
		bat := containers.MockBatch(typs, 8192*2, 2, nil)
		err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
		assert.Nil(t, err)

		for j := range sinker.staged.inMemory {
			if j != len(sinker.staged.inMemory)-1 {
				require.Equal(t, 8192, int(sinker.staged.inMemory[j].RowCount()))
			}
		}
	}

	err = sinker.Sync(context.Background())
	assert.Nil(t, err)

	require.Equal(t, 0, len(sinker.staged.inMemory))

	rows := 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}

	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}

	require.Equal(t, 8192*2*5, rows)
	require.NoError(t, sinker.Close())

	fmt.Println(sinker.staged.inMemStats.String())
	fmt.Println(sinker.buf.bufStats.String())

	require.Equal(t, 0, int(proc.Mp().CurrNB()))
}

func TestSinkerCancel(t *testing.T) {
	var sinker Sinker
	ctx, cancel := context.WithCancelCause(context.Background())
	expectErr := moerr.NewInternalErrorNoCtx("tt")
	cancel(expectErr)
	err := sinker.Sync(ctx)
	require.ErrorContains(t, err, expectErr.Error())
}
