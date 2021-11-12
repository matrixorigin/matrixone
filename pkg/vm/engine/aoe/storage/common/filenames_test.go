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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilenames(t *testing.T) {
	workDir := "/work"
	spillDir := MakeSpillDir(workDir)
	metaDir := MakeMetaDir(workDir)
	dataDir := MakeDataDir(workDir)
	assert.Equal(t, "/work/spill", spillDir)
	assert.Equal(t, "/work/meta", metaDir)
	assert.Equal(t, "/work/data", dataDir)

	lock := MakeLockFileName(workDir, "work")
	assert.Equal(t, "/work/work.lock", lock)
	blk1 := MakeBlockFileName(workDir, "blk-1", 0, false)
	assert.Equal(t, "/work/data/blk-1.blk", blk1)
	blk2 := MakeBlockFileName(workDir, "blk-2", 0, true)
	assert.Equal(t, "/work/data/blk-2.blk.tmp", blk2)
	assert.True(t, IsTempFile(blk2))
	res, err := FilenameFromTmpfile(blk2)
	assert.Nil(t, err)
	assert.Equal(t, res, "/work/data/blk-2.blk")
	_, err = FilenameFromTmpfile(blk1)
	assert.NotNil(t, err)
	seg1 := MakeSegmentFileName(workDir, "seg-1", 0, false)
	assert.Equal(t, "/work/data/seg-1.seg", seg1)
	tblk1 := MakeTBlockFileName(workDir, "tblk-1", false)
	assert.Equal(t, "/work/data/tblk-1.tblk", tblk1)

	res, ok := ParseSegmentFileName(seg1)
	assert.True(t, ok)
	res, ok = ParseSegmentFileName(res)
	assert.False(t, ok)
	res, ok = ParseBlockfileName(blk1)
	assert.True(t, ok)
	res, ok = ParseBlockfileName(res)
	assert.False(t, ok)
	res, ok = ParseTBlockfileName(tblk1)
	assert.True(t, ok)
	res, ok = ParseTBlockfileName(res)
	assert.False(t, ok)

	n := MakeFilename(workDir, FTTransientNode, "node", false)
	assert.Equal(t, "/work/spill/node.nod", n)

	assert.Panics(t, func() {
		MakeFilename("", 10, "", false)
	})
}
