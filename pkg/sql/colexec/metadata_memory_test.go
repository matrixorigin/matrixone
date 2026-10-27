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

package colexec

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestInsertMetadataEnvelopeCoversActualObjectEncoding(t *testing.T) {
	for _, blocks := range []uint32{1, 65, 129} {
		for _, distinct := range []bool{false, true} {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			bat := AllocCNS3ResultBat(false, false)
			t.Cleanup(func() { bat.Clean(mp) })
			var stats []objectio.ObjectStats
			if distinct {
				stats = make([]objectio.ObjectStats, blocks)
				for i := range stats {
					require.NoError(t, objectio.SetObjectStatsObjectName(&stats[i], objectio.BuildObjectName(objectio.NewSegmentid(), uint16(i))))
					require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats[i], 1))
					require.NoError(t, objectio.SetObjectStatsRowCnt(&stats[i], 1))
				}
			} else {
				stats = make([]objectio.ObjectStats, 1)
				require.NoError(t, objectio.SetObjectStatsObjectName(&stats[0], objectio.BuildObjectName(objectio.NewSegmentid(), 0)))
				require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats[0], blocks))
				require.NoError(t, objectio.SetObjectStatsRowCnt(&stats[0], blocks*objectio.BlockMaxRows))
			}
			require.NoError(t, ExpandObjectStatsToBatch(mp, false, bat, true, stats...))
			unit := int64(blocks) * objectio.BlockMaxRows
			limit, err := InsertMetadataFrameLimit(unit, 1)
			require.NoError(t, err)
			output, err := InsertMetadataOutputEnvelope(unit, 1)
			require.NoError(t, err)
			var encoded bytes.Buffer
			require.NoError(t, bat.MarshalBinaryTo(&encoded))
			require.Equal(t, int(blocks), bat.Vecs[0].Length())
			require.Equal(t, len(stats), bat.Vecs[1].Length())
			require.LessOrEqual(t, int64(encoded.Len())+4096, limit)
			require.LessOrEqual(t, int64(encoded.Cap())+2*bat.MarshalBinarySize(), output)
		}
	}
}

func TestInsertMetadataEnvelopeArithmeticBoundaries(t *testing.T) {
	previous := int64(0)
	for _, unit := range []int64{1, objectio.BlockMaxRows - 1, objectio.BlockMaxRows, objectio.BlockMaxRows + 1, 64 << 20} {
		output, err := InsertMetadataOutputEnvelope(unit, 3)
		require.NoError(t, err)
		limit, err := InsertMetadataFrameLimit(unit, 3)
		require.NoError(t, err)
		require.Equal(t, output/4, limit)
		require.GreaterOrEqual(t, output, previous)
		previous = output
	}
	for _, args := range [][2]int64{{0, 1}, {-1, 1}, {1, -1}} {
		_, err := InsertMetadataOutputEnvelope(args[0], int(args[1]))
		require.Error(t, err, "invalid unit=%d tables=%d", args[0], args[1])
		_, err = InsertMetadataFrameLimit(args[0], int(args[1]))
		require.Error(t, err, "invalid unit=%d tables=%d", args[0], args[1])
	}
	// A maximal data unit is not necessarily an overflowing metadata bound:
	// the bound contains fixed entries per block, not a copy of those data
	// bytes. An arbitrary-precision version of the mathematical contract checks
	// both the large legal result and the exact last-fitting table count. It
	// deliberately multiplies 2*unit before division, unlike the production
	// overflow-avoiding quotient/remainder calculation.
	entry := int64(objectio.BlockInfoSize + objectio.ObjectStatsLen + 4*64)
	expect := func(unit, tables int64) *big.Int {
		blocks := new(big.Int).Mul(big.NewInt(unit), big.NewInt(2))
		blocks.Div(blocks, big.NewInt(objectio.BlockMaxRows))
		blocks.Add(blocks, new(big.Int).Mul(big.NewInt(tables), big.NewInt(4)))
		blocks.Add(blocks, big.NewInt(2))
		out := new(big.Int).Mul(blocks, big.NewInt(4*entry))
		return out.Add(out, big.NewInt(1<<20))
	}
	cases := [][2]int64{{math.MaxInt64, 1}, {1, math.MaxInt}, {math.MaxInt64, math.MaxInt}}
	maxBlocks := new(big.Int).Sub(big.NewInt(math.MaxInt64), big.NewInt(1<<20))
	maxBlocks.Div(maxBlocks, big.NewInt(4*entry))
	lastTables := new(big.Int).Sub(maxBlocks, big.NewInt(2)) // unit1 has no full blocks.
	lastTables.Div(lastTables, big.NewInt(4))
	if lastTables.IsInt64() && lastTables.Int64() < int64(math.MaxInt) {
		cases = append(cases, [2]int64{1, lastTables.Int64()}, [2]int64{1, lastTables.Int64() + 1})
	}
	for _, args := range cases {
		expected := expect(args[0], args[1])
		output, err := InsertMetadataOutputEnvelope(args[0], int(args[1]))
		frame, frameErr := InsertMetadataFrameLimit(args[0], int(args[1]))
		if expected.IsInt64() {
			require.NoError(t, err, "unit=%d tables=%d", args[0], args[1])
			require.NoError(t, frameErr)
			require.Equal(t, expected.Int64(), output)
			require.Equal(t, expected.Int64()/4, frame)
		} else {
			require.Error(t, err, "unrepresentable bound %s", expected.String())
			require.Error(t, frameErr)
		}
	}
}
