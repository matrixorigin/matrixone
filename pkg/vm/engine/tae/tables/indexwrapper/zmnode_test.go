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

package indexwrapper

import (
	"fmt"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "IndexWrapper"
)

func TestBlockZoneMapIndex(t *testing.T) {
	var err error
	// var res bool
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	bat := newBatch()
	c := fileservice.Config{
		Name:    "LOCAL",
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	assert.Nil(t, err)

	objectWriter, err := objectio.NewObjectWriter(name, service)
	assert.Nil(t, err)
	/*fd*/ _, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	blocks, err := objectWriter.WriteEnd()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	cType := common.Plain
	typ := types.Type{Oid: types.T_int32}
	pkColIdx := uint16(0)
	interIdx := uint16(0)
	// var visibility *roaring.Bitmap

	writer := NewZMWriter()
	err = writer.Init(objectWriter, blocks[0], cType, pkColIdx, interIdx)
	require.NoError(t, err)

	keys := containers.MockVector2(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)

	_, err = writer.Finalize()
	require.NoError(t, err)

	// col, err := fd.GetColumn(0)
	// assert.Nil(t, err)
	// reader := NewZMReader(col, typ)
	// require.NoError(t, err)

	// res = reader.Contains(int32(500))
	// require.True(t, res)

	// res = reader.Contains(int32(1000))
	// require.False(t, res)

	// keys = containers.MockVector2(typ, 100, 1000)
	// visibility, res = reader.ContainsAny(keys)
	// require.False(t, res)
	// require.Equal(t, uint64(0), visibility.GetCardinality())

	// keys = containers.MockVector2(typ, 100, 0)
	// visibility, res = reader.ContainsAny(keys)
	// require.True(t, res)
	// require.Equal(t, uint64(100), visibility.GetCardinality())
}

func newBatch() *batch.Batch {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	mp := mheap.New(gm)
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
