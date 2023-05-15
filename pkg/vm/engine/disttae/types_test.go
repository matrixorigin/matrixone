// Copyright 2022 Matrix Origin
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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestZonemapMarshalAndUnmarshal(t *testing.T) {
	var z Zonemap
	for i := 0; i < len(z); i++ {
		z[i] = byte(i)
	}

	data, err := z.Marshal()
	require.NoError(t, err)

	var ret Zonemap
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, z, ret)
}

func getLocation() objectio.Location {
	uuid, _ := types.BuildUuid()
	name := objectio.BuildObjectName(&uuid, 1)
	extent := objectio.NewExtent(1, 1, 1, 1)
	return objectio.BuildLocation(name, extent, 1, 1)
}

func TestModifyBlockMarShalAnUnmarshal(t *testing.T) {
	var loca1 objectio.Location = getLocation()
	var loca2 objectio.Location = getLocation()
	var modifyBlockMeta *ModifyBlockMeta = &ModifyBlockMeta{
		meta:              catalog.BlockInfo{},
		cnRawBatchdeletes: []int{1, 2},
		cnDeleteLocations: []objectio.Location{loca1, loca2},
	}
	data := modifyBlockMeta.ModifyEncode()
	modifyBlockMeta2, err := ModifyDecode(data)
	require.Nil(t, err)
	require.Equal(t, modifyBlockMeta.meta, catalog.BlockInfo{})
	require.Equal(t, modifyBlockMeta.cnRawBatchdeletes, modifyBlockMeta2.cnRawBatchdeletes)
	require.Equal(t, modifyBlockMeta2.cnDeleteLocations, modifyBlockMeta2.cnDeleteLocations)
}
