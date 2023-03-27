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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	defer testutils.AfterTest(t)()
	tree := NewTree()
	seg1 := NewSegmentid()
	tree.AddSegment(1, 2, NewSegmentid())
	tree.AddBlock(4, 5, seg1, NewBlockid(&seg1, 0, 0))
	tree.AddBlock(4, 5, seg1, NewBlockid(&seg1, 1, 0))
	tree.AddBlock(4, 5, seg1, NewBlockid(&seg1, 2, 0))
	t.Log(tree.String())
	assert.Equal(t, 2, tree.TableCount())

	var w bytes.Buffer
	_, err := tree.WriteTo(&w)
	assert.NoError(t, err)

	tree2 := NewTree()
	_, err = tree2.ReadFrom(&w)
	assert.NoError(t, err)
	t.Log(tree2.String())
	assert.True(t, tree.Equal(tree2))
}
