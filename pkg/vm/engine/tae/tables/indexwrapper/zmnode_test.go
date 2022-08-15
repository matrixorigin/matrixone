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
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestBlockZoneMapIndex(t *testing.T) {
	bufManager := buffer.NewNodeManager(1024*1024, nil)
	file := common.MockRWFile()
	cType := Plain
	typ := types.Type{Oid: types.T_int32}
	pkColIdx := uint16(0)
	interIdx := uint16(0)
	var err error
	var res bool
	var visibility *roaring.Bitmap

	writer := NewZMWriter()
	err = writer.Init(file, cType, pkColIdx, interIdx)
	require.NoError(t, err)

	keys := containers.MockVector2(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)

	_, err = writer.Finalize()
	require.NoError(t, err)

	reader := NewZMReader(bufManager, file, new(common.ID), typ)
	require.NoError(t, err)

	res = reader.Contains(int32(500))
	require.True(t, res)

	res = reader.Contains(int32(1000))
	require.False(t, res)

	keys = containers.MockVector2(typ, 100, 1000)
	visibility, res = reader.ContainsAny(keys)
	require.False(t, res)
	require.Equal(t, uint64(0), visibility.GetCardinality())

	keys = containers.MockVector2(typ, 100, 0)
	visibility, res = reader.ContainsAny(keys)
	require.True(t, res)
	require.Equal(t, uint64(100), visibility.GetCardinality())
}
