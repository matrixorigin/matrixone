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

package vectorindex

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIvfSearchRoundDiagnosticRoundTrip(t *testing.T) {
	want := IvfSearchRoundDiagnostic{
		Round:        4,
		BucketOffset: 7,
		BucketCount:  8,
		RowLimit:     25,
		OutputRows:   2,
		Exhausted:    true,
	}
	got, ok := DecodeIvfSearchRoundDiagnostic(EncodeIvfSearchRoundDiagnostic(want))
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestIvfSearchRoundDiagnosticRejectsMalformedCarriers(t *testing.T) {
	for _, query := range []*plan.Query{
		nil,
		{},
		{Headings: []string{ivfSearchRoundDiagnosticHeading}},
		{Headings: []string{ivfSearchRoundDiagnosticHeading, "x", "0", "1", "2", "3", "0"}},
		{Headings: []string{ivfSearchRoundDiagnosticHeading, "1", "0", "0", "2", "3", "0"}},
		{Headings: []string{ivfSearchRoundDiagnosticHeading, "1", "0", "1", "2", "3", "maybe"}},
	} {
		_, ok := DecodeIvfSearchRoundDiagnostic(query)
		require.False(t, ok, "query=%v", query)
	}
}

func TestIvfSearchRoundDiagnosticSurvivesRemoteJSONTransport(t *testing.T) {
	want := IvfSearchRoundDiagnostic{
		Round: 2, BucketOffset: 1, BucketCount: 2, RowLimit: 12, OutputRows: 4, Exhausted: true,
	}
	payload, err := json.Marshal(EncodeIvfSearchRoundDiagnostic(want))
	require.NoError(t, err)

	var query plan.Query
	require.NoError(t, json.Unmarshal(payload, &query))
	got, ok := DecodeIvfSearchRoundDiagnostic(&query)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestIvfExecutionDiagnosticRoundTripAndMerge(t *testing.T) {
	first := IvfExecutionDiagnostic{
		SearchCount: 1, ReaderCount: 1, MetadataBlocks: 2, MetadataRows: 1,
		MetadataTimeNS: 3, EntryBlocksSelected: 4, EntryBlocksRead: 3, EntryOutputRows: 5, EntryTimeNS: 6,
		StorageFilterInputRows: 7, StorageFilterOutputRows: 3,
		VectorRowsScored: 3, VectorChunksRead: 2, VectorChunkCacheHits: 1,
		VectorCompressedBytes: 8, VectorDecodedBytes: 16, TopKOutputRows: 2, OutputRows: 2,
	}
	second := IvfExecutionDiagnostic{
		ReaderCount: 1, CentroidBlocks: 1, CentroidRows: 9, CentroidTimeNS: 10,
		EntryBlocksSelected: 2, EntryBlocksRead: 1, EntryOutputRows: 1, VectorRowsScored: 1, OutputRows: 1,
	}
	want := first
	want.Merge(second)

	payload, err := json.Marshal(EncodeIvfExecutionDiagnostic(want))
	require.NoError(t, err)
	var query plan.Query
	require.NoError(t, json.Unmarshal(payload, &query))
	got, ok := DecodeIvfExecutionDiagnostic(&query)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestIvfExecutionDiagnosticRejectsMalformedCarriers(t *testing.T) {
	valid := EncodeIvfExecutionDiagnostic(IvfExecutionDiagnostic{SearchCount: 1})
	invalidNumber := EncodeIvfExecutionDiagnostic(IvfExecutionDiagnostic{SearchCount: 1})
	invalidNumber.Headings[2] = "not-a-number"
	for _, query := range []*plan.Query{
		nil,
		{},
		{Headings: []string{ivfExecutionDiagnosticHeading}},
		{Headings: append([]string(nil), valid.Headings[:21]...)},
		invalidNumber,
	} {
		_, ok := DecodeIvfExecutionDiagnostic(query)
		require.False(t, ok, "query=%v", query)
	}
	got, ok := DecodeIvfExecutionDiagnostic(EncodeIvfExecutionDiagnostic(IvfExecutionDiagnostic{}))
	require.True(t, ok)
	require.Equal(t, IvfExecutionDiagnostic{}, got)
}
