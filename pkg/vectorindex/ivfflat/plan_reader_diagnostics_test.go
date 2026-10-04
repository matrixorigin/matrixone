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

package ivfflat

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestPlanReaderRecordsAndDrainsAdaptiveSearchRounds(t *testing.T) {
	reader := &planReader{recordExplainDiagnostics: true}
	cursor := &vectorindex.IvfSearchCursor{
		NextBucketOffset:   3,
		CurrentBucketCount: 4,
		Round:              3,
		Exhausted:          false,
	}
	reader.recordSearchRoundDiagnostic(cursor, 25, 2, 0)

	diagnostics := reader.TakeExplainDiagnostics()
	require.Len(t, diagnostics, 1)
	got, ok := vectorindex.DecodeIvfSearchRoundDiagnostic(diagnostics[0])
	require.True(t, ok)
	require.Equal(t, vectorindex.IvfSearchRoundDiagnostic{
		Round:        3,
		BucketOffset: 3,
		BucketCount:  4,
		RowLimit:     25,
		OutputRows:   0,
		Exhausted:    false,
	}, got)
	require.Nil(t, reader.TakeExplainDiagnostics())
}

func TestPlanReaderPublishesAndDrainsExecutionSummary(t *testing.T) {
	stats := &vectorindex.IvfExecutionDiagnostic{EntryBlocksSelected: 3, VectorRowsScored: 7}
	reader := &planReader{
		recordExplainDiagnostics: true,
		executionStats:           stats,
		scanner:                  &relationScanner{partitionIndex: 0, executionStats: stats},
		keys:                     []any{int64(1), int64(2)},
	}
	reader.publishExecutionDiagnostic()

	diagnostics := reader.TakeExplainDiagnostics()
	require.Len(t, diagnostics, 1)
	got, ok := vectorindex.DecodeIvfExecutionDiagnostic(diagnostics[0])
	require.True(t, ok)
	require.Equal(t, uint64(1), got.SearchCount)
	require.Equal(t, uint64(3), got.EntryBlocksSelected)
	require.Equal(t, uint64(7), got.VectorRowsScored)
	require.Equal(t, uint64(2), got.OutputRows)
	require.Nil(t, reader.executionStats)
	require.Nil(t, reader.scanner.executionStats)
	require.Nil(t, reader.TakeExplainDiagnostics())
}

func TestRelationScannerRecordsEntryExecutionWork(t *testing.T) {
	stats := new(vectorindex.IvfExecutionDiagnostic)
	scanner := &relationScanner{executionStats: stats}
	scanner.recordRelationExecutionStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		3,
		2,
		4,
		5*time.Millisecond,
		[]objectio.IndexReaderTopStats{{
			BlocksRead: 3, StorageFilterInputRows: 10, StorageFilterOutputRows: 4,
			VectorRowsScored: 4, VectorChunksRead: 2, VectorChunkCacheHits: 1,
			VectorCompressedBytes: 20, VectorDecodedBytes: 40, TopKOutputRows: 2,
		}},
	)

	require.Equal(t, uint64(2), stats.ReaderCount)
	require.Equal(t, uint64(3), stats.EntryBlocksSelected)
	require.Equal(t, uint64(3), stats.EntryBlocksRead)
	require.Equal(t, uint64(4), stats.EntryOutputRows)
	require.Equal(t, uint64(5*time.Millisecond), stats.EntryTimeNS)
	require.Equal(t, uint64(10), stats.StorageFilterInputRows)
	require.Equal(t, uint64(4), stats.StorageFilterOutputRows)
	require.Equal(t, uint64(4), stats.VectorRowsScored)
	require.Equal(t, uint64(2), stats.VectorChunksRead)
	require.Equal(t, uint64(1), stats.VectorChunkCacheHits)
	require.Equal(t, uint64(20), stats.VectorCompressedBytes)
	require.Equal(t, uint64(40), stats.VectorDecodedBytes)
	require.Equal(t, uint64(2), stats.TopKOutputRows)
}
