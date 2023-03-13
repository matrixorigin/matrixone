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

package blockio

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "BlockIO"
)

func newBatch() *batch.Batch {
	mp := mpool.MustNewZero()
	types := []types.Type{
		{Oid: types.T_int32},
		{Oid: types.T_int16},
		{Oid: types.T_int32},
		{Oid: types.T_int64},
		{Oid: types.T_uint16},
		{Oid: types.T_uint32},
		{Oid: types.T_uint8},
		{Oid: types.T_uint64},
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}

func TestWriter_WriteBlockAndZoneMap(t *testing.T) {
	defer testutils.AfterTest(t)()
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	bat := newBatch()
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c, nil)
	assert.Nil(t, err)
	writer, _ := NewBlockWriter(service, name)
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 4
	_, err = writer.WriteBatch(bat)
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	fd := blocks[0]
	col, err := fd.GetColumn(0)
	assert.Nil(t, err)
	zm := index.NewZoneMap(*bat.Vecs[0].GetType())
	colZoneMap := col.GetMeta().GetZoneMap()

	err = zm.Unmarshal(colZoneMap.GetData().([]byte))
	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(79999))
	require.True(t, res)
	res = zm.Contains(int32(100000))
	require.False(t, res)
}
