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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	defer testutils.AfterTest(t)()
	n0 := NewSLLNode(nil)
	var mu sync.RWMutex
	n1 := NewSLLNode(&mu)
	n0.SetNextNode(n1)
	n1.SetNextNode(nil)
	assert.Equal(t, n1, n0.GetNextNode())
	n2 := NewSLLNode(nil)
	n2.SetNextNode(nil)
	n0.SetNextNode(n2)
	assert.Equal(t, n2, n0.GetNextNode())
	n0.ReleaseNextNode()
	assert.Equal(t, nil, n0.GetNextNode())
}
