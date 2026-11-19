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

package ioutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

type skippedFS struct {
	fileservice.FileService
	group uint64
}

func (f skippedFS) WholeObjectPreloadSkipped(_ string, group uint64) bool { return group == f.group }

func TestPrefetchSkipped(t *testing.T) {
	objID := types.Objectid{1}
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, true, true)
	blk := stats.ConstructBlockInfo(0)
	fs := skippedFS{group: 7}
	thrashing := fileservice.WithPreloadGroup(context.Background(), 7)
	kept := fileservice.WithPreloadGroup(context.Background(), 8)

	require.True(t, PrefetchSkipped(thrashing, fs, fileservice.NarrowRead, &blk), "a narrow read of a thrashing table")
	require.False(t, PrefetchSkipped(thrashing, fs, 0, &blk), "a scan of the same table")
	require.False(t, PrefetchSkipped(kept, fs, fileservice.NarrowRead, &blk), "a narrow read of a table that fits")
	require.True(t, PrefetchSkipped(kept, fs, fileservice.SkipFullFilePreloads, &blk))
}
