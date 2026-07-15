// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestSetParentRejectsJoinGraphCycle(t *testing.T) {
	vertices := makeJoinVertices(3)
	setParent(0, 1, vertices)
	setParent(1, 2, vertices)

	setParent(2, 0, vertices)

	require.Equal(t, int32(-1), vertices[2].parent)
	require.False(t, vertices[0].children[2])
	require.True(t, findParent(0, 2, vertices))
	require.False(t, findParent(2, 0, vertices))
}

func TestJoinGraphWalksTerminateOnExistingCycle(t *testing.T) {
	vertices := makeJoinVertices(3)
	vertices[0].parent = 1
	vertices[1].parent = 2
	vertices[2].parent = 1
	vertices[1].children[2] = true
	vertices[2].children[1] = true

	require.False(t, findParent(0, 0, vertices))
	require.False(t, findSelectivityInChildren(1, vertices))
}

func TestJoinGraphWalksIgnoreInvalidVertexIDs(t *testing.T) {
	vertices := makeJoinVertices(2)
	vertices[0].parent = 3
	vertices[0].children[3] = true

	require.False(t, findParent(0, 1, vertices))
	require.False(t, findSelectivityInChildren(0, vertices))
}

func makeJoinVertices(n int) []*joinVertex {
	vertices := make([]*joinVertex, n)
	for i := range vertices {
		vertices[i] = &joinVertex{
			node: &plan.Node{
				Stats: &plan.Stats{
					Selectivity: 1,
					Outcnt:      1,
				},
			},
			children: make(map[int32]bool),
			parent:   -1,
		}
	}
	return vertices
}
