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

package fileservice

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// wantSources is the table of the shared sources a read may take its bytes from, by its
// policy, whether it is a read of a download and whether it streams an entry.
func wantSources(s *S3FS, p Policy, download, streams bool) readSources {
	// BC15: the reads of a download join no prefetch object.
	disk := s.diskCache != nil && !p.Any(SkipDiskCacheReads) && !download
	return readSources{
		indexRead: p.Any(IndexFileObject) && p&SkipCacheReads != SkipCacheReads && s.cachesIndexObjects(),
		// BC16: an exact read joins queued ranges as a column read does.
		queue:       disk && s.prefetcher != nil && !(!p.Any(ColumnPrefetch, ExactRead) && p.Any(SkipFullFilePreloads)),
		queueRanges: p.Any(ColumnPrefetch, ExactRead),
		flights:     disk && p.Any(ColumnPrefetch, ExactRead),
		window:      disk,
		fetch:       disk && !streams,
	}
}

func TestSourcesForTable(t *testing.T) {
	fs, _, _ := newIndexObjectFS(t, true)
	require.NotNil(t, fs.prefetcher)
	for p := Policy(0); p < IndexFileObject<<1; p++ {
		for _, download := range []bool{false, true} {
			for _, streams := range []bool{false, true} {
				ctx := context.Background()
				if download {
					ctx = withDownloadStarted(ctx, func() {})
				}
				vec := &IOVector{FilePath: "object", Policy: p, Entries: []IOEntry{{Size: 1}}}
				if streams {
					vec.Entries[0].WriterForRead = new(bytes.Buffer)
				}
				require.Equal(t, wantSources(fs, p, download, streams), fs.sourcesFor(ctx, vec),
					"policy %b, download %v, streams %v", p, download, streams)
			}
		}
	}
}
