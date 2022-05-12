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

package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestId(t *testing.T) {
	tid := NewTransientID()
	assert.Equal(t, TRANSIENT_TABLE_START_ID, tid.TableID)
	assert.Equal(t, true, tid.IsTransient())
	newTid, _, err := ParseTBlkName(fmt.Sprintf("%d_0_0_0", TRANSIENT_TABLE_START_ID))
	assert.Nil(t, err)
	assert.Equal(t, true, tid.IsSameBlock(newTid))
	assert.Equal(t, tid.PartID, newTid.PartID)
	assert.Equal(t, uint32(0), tid.NextPart().PartID)
	sid0 := ID{
		TableID:   0,
		SegmentID: 0,
	}
	assert.Equal(t, false, sid0.IsTransient())
	sid1 := ID{
		TableID:   0,
		SegmentID: 1,
	}
	sid00 := ID{
		TableID:   0,
		SegmentID: 0,
	}
	assert.Equal(t, "<0:0-0-0-0-0>", sid0.String())
	assert.Equal(t, false, sid0.IsSameSegment(sid1))
	assert.Equal(t, true, sid0.IsSameSegment(sid00))
	sid00.NextSegment()
	assert.Equal(t, uint64(1), sid00.NextSegment().SegmentID)
	bid0 := ID{
		TableID:   0,
		SegmentID: 0,
		BlockID:   0,
	}
	bid1 := ID{
		TableID:   0,
		SegmentID: 0,
		BlockID:   1,
	}
	assert.Equal(t, false, bid0.IsSameBlock(bid1))
	assert.Equal(t, "TBL<0>", bid0.TableString())
	assert.Equal(t, "SEG<0:0-0>", bid0.SegmentString())
	assert.Equal(t, "BLK<0:0-0-0>", bid0.BlockString())
	assert.Equal(t, uint64(0), bid0.AsSegmentID().SegmentID)
	assert.Equal(t, uint64(0), bid0.AsBlockID().BlockID)
	assert.Equal(t, uint64(0), bid0.NextBlock().BlockID)
	bid0.Next()
	assert.Equal(t, uint64(1), bid0.Next().TableID)
	bid0.NextIter()
	assert.Equal(t, uint8(1), bid0.NextIter().Iter)
	assert.Equal(t, uint8(0), bid0.Iter)
	assert.Equal(t, "0_2_0_1_0", bid0.ToPartFileName())
	assert.Equal(t, "2/0/1/0/0.0", bid0.ToPartFilePath())
	assert.Equal(t, "2_0_1", bid0.ToBlockFileName())
	assert.Equal(t, "2/0/1/", bid0.ToBlockFilePath())
	assert.Equal(t, "2_0", bid0.ToSegmentFileName())
	assert.Equal(t, "2/0/", bid0.ToSegmentFilePath())
	_, _, err = ParseTBlkName("0_0_0")
	assert.NotNil(t, err)
	_, _, err = ParseTBlkName("a_0_0_0")
	assert.NotNil(t, err)
	_, _, err = ParseTBlkName("0_a_0_0")
	assert.NotNil(t, err)
	_, _, err = ParseTBlkName("0_0_a_0")
	assert.NotNil(t, err)
	_, _, err = ParseTBlkName("0_0_0_a")
	assert.Nil(t, err)
	_, err = ParseBlkNameToID("0_0")
	assert.NotNil(t, err)
	_, err = ParseBlkNameToID("a_0_0")
	assert.NotNil(t, err)
	_, err = ParseBlkNameToID("0_a_0")
	assert.NotNil(t, err)
	_, err = ParseBlkNameToID("0_0_a")
	assert.NotNil(t, err)
	_, err = ParseBlkNameToID("0_0_0")
	assert.Nil(t, err)
	_, err = ParseSegmentNameToID("0")
	assert.NotNil(t, err)
	_, err = ParseSegmentNameToID("a_0")
	assert.NotNil(t, err)
	_, err = ParseSegmentNameToID("0_a")
	assert.NotNil(t, err)
	_, err = ParseSegmentNameToID("0_0")
	assert.Nil(t, err)
}
