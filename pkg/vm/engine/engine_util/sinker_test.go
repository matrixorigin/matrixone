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

package engine_util

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSinker(t *testing.T) {
	proc := testutil.NewProc()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	schema := catalog.MockSchema(3, 2)
	seqnums := make([]uint16, len(schema.Attrs()))
	for i := range schema.Attrs() {
		seqnums[i] = schema.GetSeqnum(schema.Attrs()[i])
	}

	buffer := containers.NewOneSchemaBatchBuffer(mpool.GB, schema.Attrs(), schema.Types())

	factory := NewFSinkerImplFactory(
		seqnums,
		schema.GetPrimaryKey().Idx,
		true,
		false,
		schema.Version,
	)

	sinker := NewSinker(
		schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
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

	bat := catalog.MockBatch(schema, 1000)
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

func TestNewSinker2(t *testing.T) {
	proc := testutil.NewProc()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	schema := catalog.MockSchema(3, 2)
	seqnums := make([]uint16, len(schema.Attrs()))
	for i := range schema.Attrs() {
		seqnums[i] = schema.GetSeqnum(schema.Attrs()[i])
	}

	factory := NewFSinkerImplFactory(
		seqnums,
		schema.GetPrimaryKey().Idx,
		true,
		false,
		schema.Version,
	)

	sinker := NewSinker(
		schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
		factory,
		proc.Mp(),
		fs,
	)

	for i := 0; i < 100; i++ {
		bat := catalog.MockBatch(schema, 8192*10)
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

	require.Equal(t, 8192*10*100, rows)
	require.NoError(t, sinker.Close())

	fmt.Println(sinker.staged.inMemStats.String())
	fmt.Println(sinker.buf.bufStats.String())

	require.Equal(t, 0, int(proc.Mp().CurrNB()))
}
