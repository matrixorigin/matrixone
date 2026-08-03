// Copyright 2026 Matrix Origin
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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestResolveSpillThreshold(t *testing.T) {
	require.Equal(t, int64(4096), ResolveSpillThreshold(4096))

	oldCacheSize := fileservice.GlobalMemoryCacheSizeHint.Load()
	fileservice.GlobalMemoryCacheSizeHint.Store(int64(system.MemoryTotal()))
	t.Cleanup(func() { fileservice.GlobalMemoryCacheSizeHint.Store(oldCacheSize) })
	require.Equal(t, int64(common.MiB*128), ResolveSpillThreshold(0))
}

func TestShouldSpill(t *testing.T) {
	require.False(t, ShouldSpill(100, 1000, 0))
	require.False(t, ShouldSpill(100, 900, 1000))
	require.True(t, ShouldSpill(100, 1000, 1000))
	require.False(t, ShouldSpill(200000, 1000, 200001))
	require.True(t, ShouldSpill(200001, 1000, 200000))
}
