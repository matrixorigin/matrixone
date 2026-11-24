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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestSmallLocalScanPrefetchesColumnsOnReach(t *testing.T) {
	for _, small := range []bool{false, true} {
		name := "four_blocks"
		if small {
			name = "three_blocks"
		}
		t.Run(name, func(t *testing.T) {
			ls, fs, stats := newColumnPrefetchSource(t, true)
			defer ls.Close()
			ctx := context.Background()
			if !small {
				other, otherFS, otherStats := newColumnPrefetchSource(t, true)
				defer other.Close()
				// Add a readable block from a second object to exceed the local window cutoff.
				vec := &fileservice.IOVector{FilePath: otherStats.ObjectName().String(), Entries: []fileservice.IOEntry{{Size: -1}}}
				defer vec.Release()
				require.NoError(t, otherFS.Read(ctx, vec))
				require.NoError(t, fs.Write(ctx, *vec))
				ls.rangeSlice.AppendBlockInfo(other.rangeSlice.Get(0))
			}
			ls.table = &txnTable{db: &txnDatabase{}}
			ls.SetOrderBy(nil) // Apply the same four-block cutoff as NewLocalDataSource.
			ctx = fileservice.WithFileServicePolicy(ctx, fileservice.ExactColumnsRead|fileservice.ColumnPrefetch)
			blk, state, err := ls.Next(ctx, []string{"key"}, nil, []uint16{0}, 0, &readutil.MemPKFilter{}, nil, nil)
			require.NoError(t, err)
			require.Equal(t, engine.Persisted, state)
			require.NotNil(t, blk)
			var requests int
			for _, req := range fs.reqs {
				if req.FilePath == stats.ObjectName().String() {
					require.Len(t, req.Ranges, 1)
					requests++
				}
			}
			require.Positive(t, requests, "entering a narrow scan's first object should request its column range")
		})
	}
}

func TestSmallLocalScanPrefetchesColumnsAhead(t *testing.T) {
	for _, columns := range []bool{false, true} {
		name := "whole_object"
		if columns {
			name = "column_ranges"
		}
		t.Run(name, func(t *testing.T) {
			ls, fs, stats := newColumnPrefetchSource(t, true)
			defer ls.Close()
			ls.SetOrderBy(nil)
			var policy fileservice.Policy
			if columns {
				policy = fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch
			}
			ls.batchPrefetch(fileservice.WithFileServicePolicy(context.Background(), policy), []uint16{0})
			var requests int
			for _, req := range fs.reqs {
				if req.FilePath == stats.ObjectName().String() {
					require.Len(t, req.Ranges, 1)
					requests++
				}
			}
			if columns {
				require.Equal(t, 1, requests, "a three-block scan prefetches its column ranges ahead")
			} else {
				require.Zero(t, requests, "a three-block scan does not prefetch whole objects")
			}
		})
	}
}
