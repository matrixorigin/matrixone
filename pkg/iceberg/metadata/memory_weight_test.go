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

package metadata

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestMetadataMemoryWeightsAccountNestedRetainedData(t *testing.T) {
	require.Zero(t, metadataMemoryWeight(0, nil))
	require.Equal(t, int64(1024), metadataMemoryWeight(0, &api.TableMetadata{}))
	require.Equal(t, int64(40), metadataMemoryWeight(10, nil))

	manifestWeight := ManifestListMemoryWeight(10, []api.ManifestFile{{
		Path: "path", ManifestPathHash: "hash", ManifestPathRedacted: "redacted", KeyMetadata: []byte{1},
		Partitions: []api.PartitionFieldSummary{{LowerBound: []byte{1}, UpperBound: []byte{2}}},
	}})
	require.Greater(t, manifestWeight, int64(10))

	file := api.DataFile{
		FilePath: "path", FileFormat: "parquet", ReferencedDataFile: "referenced",
		KeyMetadata: []byte{1}, EncryptionKeyMetadata: []byte{2}, SplitOffsets: []int64{1}, EqualityIDs: []int{1},
		ColumnSizes: map[int]int64{1: 1}, ValueCounts: map[int]int64{1: 1}, NullValueCounts: map[int]int64{1: 0}, NaNValueCounts: map[int]int64{1: 0},
		PartitionFieldIDs: map[string]int{"nested": 1},
		Partition: map[string]any{
			"nil": nil, "string": "value", "bytes": []byte{1},
			"map": map[string]any{"nested": int64(1)}, "slice": []any{"value", []byte{2}}, "scalar": int64(1),
		},
		LowerBounds: map[int][]byte{1: {1}}, UpperBounds: map[int][]byte{1: {2}},
	}
	require.Greater(t, dataFileMemoryWeight(file), int64(320))
	require.Greater(t, ManifestEntriesMemoryWeight(10, []api.ManifestEntry{{DataFile: file}}), int64(10))
}

func TestMetadataMemoryWeightSaturates(t *testing.T) {
	require.Equal(t, int64(math.MaxInt64), saturatingMetadataAdd(math.MaxInt64, 1))
	require.Equal(t, int64(-1), saturatingMetadataAdd(1, -2))
	require.Zero(t, saturatingMetadataMul(0, 10))
	require.Zero(t, saturatingMetadataMul(10, -1))
	require.Equal(t, int64(math.MaxInt64), saturatingMetadataMul(math.MaxInt64, 2))
	require.Equal(t, int64(12), saturatingMetadataMul(3, 4))
}
