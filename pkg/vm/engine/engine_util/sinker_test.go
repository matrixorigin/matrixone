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

	sinker := NewSinker(schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
		seqnums,
		true,
		false,
		schema.Version,
		proc.Mp(),
		fs,
		WithMemorySizeThreshold(1),
		WithBufferSizeCap(1),
		WithAllMergeSorted(),
		WithDedupAll(),
		WithTailSizeCap(1),
		WithBuffer(containers.NewOneSchemaBatchBuffer(mpool.GB, schema.Attrs(), schema.Types())),
	)

	bat := catalog.MockBatch(schema, 100)
	err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
	assert.Nil(t, err)

	require.Equal(t, 1, len(sinker.staged.persisted))
	require.Equal(t, 100, int(sinker.staged.persisted[0].Rows()))
}
