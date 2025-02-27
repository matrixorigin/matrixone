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
	obj2 := objectio.NewObjectid()
	tree.AddObject(1, 2, &obj2, false)
	t.Log(tree.String())
	assert.Equal(t, 1, tree.TableCount())

	var w bytes.Buffer
	_, err := tree.WriteTo(&w)
	assert.NoError(t, err)

	tree2 := NewTree()
	_, err = tree2.ReadFromWithVersion(&w, MemoTreeVersion3)
	assert.NoError(t, err)
	t.Log(tree2.String())
	assert.True(t, tree.Equal(tree2))
}

func TestTableTree_ReadOldVersion(t *testing.T) {
	defer testutils.AfterTest(t)()
	tree := NewTableTree(0, 0)
	var w bytes.Buffer
	obj := objectio.NewObjectid()
	tree.AddObject(&obj, false)
	_, err := tree.WriteTo(&w)
	assert.NoError(t, err)

	tree2 := NewTableTree(0, 0)
	assert.Panics(t, func() {
		tree2.ReadFromWithVersion(&w, MemoTreeVersion2)
	}, "not supported")
}
