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
