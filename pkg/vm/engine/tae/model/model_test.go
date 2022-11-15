// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestPrepareHiddenData(t *testing.T) {
	defer testutils.AfterTest(t)()
	typ := types.T_Rowid.ToType()
	id := common.ID{
		TableID:   1,
		SegmentID: 2,
		BlockID:   3,
	}
	prefix := EncodeBlockKeyPrefix(id.SegmentID, id.BlockID)
	data, err := PreparePhyAddrData(typ, prefix, 0, 20)
	assert.NoError(t, err)
	assert.Equal(t, 20, data.Length())
	for i := 0; i < data.Length(); i++ {
		v := data.Get(i)
		sid, bid, off := DecodePhyAddrKey(v.(types.Rowid))
		assert.Equal(t, sid, id.SegmentID)
		assert.Equal(t, bid, id.BlockID)
		assert.Equal(t, off, uint32(i))
	}
}
