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

package statsinfo

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestByteRangeWireAndMerge(t *testing.T) {
	original := &StatsInfo{TableCnt: 4, ByteRangeMap: map[string]*ByteRangeDistribution{"c": {Version: 1, TypeId: 61, Charset: 1, CollationVersion: 2, TableVersion: 3, ColumnSeqnum: 4, PopulationRows: 4, SampleRows: 4, CompleteSample: true, Values: [][]byte{{}, {0, 255}, []byte("duplicate"), []byte("duplicate")}}}}
	raw, err := original.Marshal()
	require.NoError(t, err)
	var decoded StatsInfo
	require.NoError(t, decoded.Unmarshal(raw))
	require.Equal(t, original, &decoded)
	require.Equal(t, len(raw), original.ProtoSize())
	decoded.Merge(&StatsInfo{TableCnt: 1})
	require.Empty(t, decoded.ByteRangeMap)
	require.NotEmpty(t, original.ByteRangeMap)
	legacy := &StatsInfo{TableCnt: 5}
	raw, err = legacy.Marshal()
	require.NoError(t, err)
	require.NoError(t, decoded.Unmarshal(raw))
	require.Empty(t, decoded.ByteRangeMap)
}
