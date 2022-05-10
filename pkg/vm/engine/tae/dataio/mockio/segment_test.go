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

package mockio

import (
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "MOCKIO"
)

func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "seg")
	id := common.NextGlobalSeqNum()
	seg := SegmentFileMockFactory(name, id)
	fp := seg.Fingerprint()
	assert.Equal(t, id, fp.SegmentID)

	blkId1 := common.NextGlobalSeqNum()
	blk1, err := seg.OpenBlock(blkId1, 2, nil)
	assert.Nil(t, err)
	blkTs1 := common.NextGlobalSeqNum()
	blk1.WriteTS(blkTs1)

	ts, _ := blk1.ReadTS()
	assert.Equal(t, blkTs1, ts)
	blk1.Close()
	t.Log(seg.String())
	seg.Unref()
}
