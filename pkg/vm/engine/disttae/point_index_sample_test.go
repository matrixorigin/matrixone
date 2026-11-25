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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPointSampleCountsJointMatchesOnTheSameRow(t *testing.T) {
	keys := [][]engine.IndexPointKey{
		{{Seqnum: 1, Value: 5}, {Seqnum: 2, Value: 6}},
		{{Seqnum: 3, Value: 7}},
	}
	positions := [engine.MaxPointSampleCandidates][engine.MaxPointSampleColumns]int{{0, 1}, {2}}
	var counts [engine.MaxPointSampleCandidates]int
	rows := []objectio.CachedInt32Row{
		{Values: [4]int32{5, 8, 7}, Present: 0b111},
		{Values: [4]int32{9, 6, 7}, Present: 0b111},
		{Values: [4]int32{5, 6, 7}, Present: 0b001},
	}
	countPointSampleMatches(&counts, rows, keys, positions)
	require.Equal(t, [engine.MaxPointSampleCandidates]int{0, 2}, counts)
	// The marginals remain the same after flipping column 2 between rows.
	rows[0].Values[1], rows[1].Values[1] = rows[1].Values[1], rows[0].Values[1]
	clear(counts[:])
	countPointSampleMatches(&counts, rows, keys, positions)
	require.Equal(t, [engine.MaxPointSampleCandidates]int{1, 2}, counts)
}
