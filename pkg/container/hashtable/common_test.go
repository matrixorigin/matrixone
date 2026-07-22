// Copyright 2026 Matrix Origin
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

package hashtable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateHashMapSizeFollowsRuntimeGrowth(t *testing.T) {
	require.Equal(t, uint64(16*1024), EstimateInt64HashMapSize(0))
	require.Equal(t, uint64(16*1024), EstimateInt64HashMapSize(512))
	require.Equal(t, uint64(32*1024), EstimateInt64HashMapSize(513))

	require.Equal(t, uint64(32*1024), EstimateStringHashMapSize(0))
	require.Equal(t, uint64(32*1024), EstimateStringHashMapSize(512))
	require.Equal(t, uint64(64*1024), EstimateStringHashMapSize(513))

	require.Equal(t, ^uint64(0), EstimateInt64HashMapSize(^uint64(0)))
	require.Equal(t, ^uint64(0), EstimateStringHashMapSize(^uint64(0)))
}
