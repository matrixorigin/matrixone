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

package io

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/stretchr/testify/require"
)

func TestBlockZoneMapIndex(t *testing.T) {
	bufManager := buffer.NewNodeManager(1024*1024, nil)
	file := common.MockRWFile()
	cType := idxCommon.Plain
	typ := types.Type{Oid: types.T_int32}
	pkColIdx := uint16(0)
	interIdx := uint16(0)
	var err error
	var res bool
	var ans *roaring.Bitmap

	writer := NewBlockZoneMapIndexWriter()
	err = writer.Init(file, cType, pkColIdx, interIdx)
	require.NoError(t, err)

	keys := idxCommon.MockVec(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)

	_, err = writer.Finalize()
	require.NoError(t, err)

	reader := NewBlockZoneMapIndexReader()
	err = reader.Init(bufManager, file, &common.ID{})
	require.NoError(t, err)

	//t.Log(bufManager.String())

	res, err = reader.MayContainsKey(int32(500))
	require.NoError(t, err)
	require.True(t, res)

	res, err = reader.MayContainsKey(int32(1000))
	require.NoError(t, err)
	require.False(t, res)

	keys = idxCommon.MockVec(typ, 100, 1000)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.False(t, res)
	require.Equal(t, uint64(0), ans.GetCardinality())

	keys = idxCommon.MockVec(typ, 100, 0)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.True(t, res)
	require.Equal(t, uint64(100), ans.GetCardinality())

	//t.Log(bufManager.String())
}
