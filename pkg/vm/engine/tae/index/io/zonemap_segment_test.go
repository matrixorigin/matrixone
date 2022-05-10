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

// TODO: refactor

//import (
//	"github.com/RoaringBitmap/roaring"
//	"github.com/matrixorigin/matrixone/pkg/container/types"
//	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
//	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
//	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
//	"github.com/stretchr/testify/require"
//	"testing"
//)
//
//func TestSegmentZoneMapIndex(t *testing.T) {
//	bufManager := buffer.NewNodeManager(1024*1024, nil)
//	file := dataio.MockIndexFile()
//	cType := common.Plain
//	typ := types.Type{Oid: types.T_int32}
//	pkColIdx := uint16(0)
//	var err error
//	var meta *common.IndexMeta
//	var res bool
//	var blockOffset uint32
//	var anses []*roaring.Bitmap
//
//	writer := NewSegmentZoneMapIndexWriter()
//	err = writer.Init(file, cType, pkColIdx)
//	require.NoError(t, err)
//
//	keys := common.MockVec(typ, 1000, 0)
//	err = writer.AddValues(keys)
//	require.NoError(t, err)
//
//	err = writer.FinishBlock()
//	require.NoError(t, err)
//
//	keys = common.MockVec(typ, 1000, 1000)
//	err = writer.AddValues(keys)
//	require.NoError(t, err)
//
//	err = writer.FinishBlock()
//	require.NoError(t, err)
//
//	err = writer.SetMinMax(int32(9999), int32(10000), typ)
//	require.NoError(t, err)
//
//	meta, err = writer.Finalize()
//	require.NoError(t, err)
//
//	reader := NewSegmentZoneMapIndexReader()
//	err = reader.Init(bufManager, file, meta)
//	require.NoError(t, err)
//
//	res, blockOffset, err = reader.MayContainsKey(int32(1500))
//	require.NoError(t, err)
//	require.Equal(t, blockOffset, uint32(1))
//	require.True(t, res)
//
//	res, blockOffset, err = reader.MayContainsKey(int32(20000))
//	require.NoError(t, err)
//	require.False(t, res)
//
//	query := common.MockVec(typ, 400, 800)
//	res, anses, err = reader.MayContainsAnyKeys(query)
//	require.NoError(t, err)
//	require.True(t, res)
//	require.NotNil(t, anses)
//	require.Equal(t, anses[0].GetCardinality(), uint64(200))
//	require.Equal(t, anses[1].GetCardinality(), uint64(200))
//	require.Nil(t, anses[2])
//
//	query = common.MockVec(typ, 1000, 10001)
//	res, anses, err = reader.MayContainsAnyKeys(query)
//	require.NoError(t, err)
//	require.False(t, res)
//	require.Nil(t, anses)
//}
