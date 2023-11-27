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

package model

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	defer testutils.AfterTest(t)()
	tree := NewTree()
	seg1 := objectio.NewSegmentid()
	seg2 := objectio.NewObjectid()
	blk0 := objectio.NewBlockid(seg1, 0, 0)
	blk1 := objectio.NewBlockid(seg1, 1, 0)
	blk2 := objectio.NewBlockid(seg1, 2, 0)
	tree.AddSegment(1, 2, seg2)
	tree.AddBlock(4, 5, blk0)
	tree.AddBlock(4, 5, blk1)
	tree.AddBlock(4, 5, blk2)
	t.Log(tree.String())
	assert.Equal(t, 2, tree.TableCount())

	var w bytes.Buffer
	_, err := tree.WriteTo(&w)
	assert.NoError(t, err)

	tree2 := NewTree()
	_, err = tree2.ReadFromWithVersion(&w, MemoTreeVersion2)
	assert.NoError(t, err)
	t.Log(tree2.String())
	assert.True(t, tree.Equal(tree2))
}
