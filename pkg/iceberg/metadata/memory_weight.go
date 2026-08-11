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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func metadataMemoryWeight(serializedBytes int, meta *api.TableMetadata) int64 {
	if serializedBytes <= 0 && meta == nil {
		return 0
	}
	// Table metadata is JSON-backed and the cache retains both representations.
	// Three decoded bytes per source byte conservatively covers Go string/map/
	// slice headers without a second full reflective walk of this large model.
	decoded := saturatingMetadataMul(int64(serializedBytes), 3)
	if decoded < 1024 && meta != nil {
		decoded = 1024
	}
	return saturatingMetadataAdd(int64(serializedBytes), decoded)
}

// ManifestListMemoryWeight returns a conservative encoded-plus-decoded weight.
func ManifestListMemoryWeight(serializedBytes int, manifests []api.ManifestFile) int64 {
	weight := int64(serializedBytes)
	for _, manifest := range manifests {
		item := int64(256 + len(manifest.Path) + len(manifest.ManifestPathHash) + len(manifest.ManifestPathRedacted) + len(manifest.KeyMetadata))
		for _, partition := range manifest.Partitions {
			item = saturatingMetadataAdd(item, int64(64+len(partition.LowerBound)+len(partition.UpperBound)))
		}
		weight = saturatingMetadataAdd(weight, item)
	}
	return weight
}

// ManifestEntriesMemoryWeight returns a conservative encoded-plus-decoded weight.
func ManifestEntriesMemoryWeight(serializedBytes int, entries []api.ManifestEntry) int64 {
	weight := int64(serializedBytes)
	for _, entry := range entries {
		weight = saturatingMetadataAdd(weight, dataFileMemoryWeight(entry.DataFile)+128)
	}
	return weight
}

func dataFileMemoryWeight(file api.DataFile) int64 {
	weight := int64(320 + len(file.FilePath) + len(file.FileFormat) + len(file.ReferencedDataFile) + len(file.KeyMetadata) + len(file.EncryptionKeyMetadata))
	weight = saturatingMetadataAdd(weight, int64(len(file.SplitOffsets)*8+len(file.EqualityIDs)*8))
	weight = saturatingMetadataAdd(weight, int64(len(file.ColumnSizes)+len(file.ValueCounts)+len(file.NullValueCounts)+len(file.NaNValueCounts))*40)
	weight = saturatingMetadataAdd(weight, int64(len(file.PartitionFieldIDs))*48)
	for key := range file.PartitionFieldIDs {
		weight = saturatingMetadataAdd(weight, int64(len(key)))
	}
	for key, value := range file.Partition {
		weight = saturatingMetadataAdd(weight, int64(48+len(key))+metadataAnyMemoryWeight(value))
	}
	for _, bounds := range []map[int][]byte{file.LowerBounds, file.UpperBounds} {
		for _, value := range bounds {
			weight = saturatingMetadataAdd(weight, int64(48+len(value)))
		}
	}
	return weight
}

func metadataAnyMemoryWeight(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 8
	case string:
		return int64(16 + len(typed))
	case []byte:
		return int64(24 + len(typed))
	case map[string]any:
		weight := int64(48)
		for key, nested := range typed {
			weight = saturatingMetadataAdd(weight, int64(48+len(key))+metadataAnyMemoryWeight(nested))
		}
		return weight
	case []any:
		weight := int64(24 + len(typed)*16)
		for _, nested := range typed {
			weight = saturatingMetadataAdd(weight, metadataAnyMemoryWeight(nested))
		}
		return weight
	default:
		return 16
	}
}

func saturatingMetadataAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func saturatingMetadataMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}
