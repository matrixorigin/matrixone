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

import "github.com/matrixorigin/matrixone/pkg/iceberg/api"

type RowGroupFooter struct {
	Ordinal         int32
	RowCount        int64
	Bytes           int64
	LowerBounds     map[int][]byte
	UpperBounds     map[int][]byte
	NullValueCounts map[int]int64
	ValueCounts     map[int]int64
}

func BuildRowGroupSplits(footers []RowGroupFooter) []api.RowGroupSplit {
	if len(footers) == 0 {
		return nil
	}
	out := make([]api.RowGroupSplit, 0, len(footers))
	var start int64
	for idx, footer := range footers {
		ordinal := footer.Ordinal
		if ordinal == 0 && idx > 0 {
			ordinal = int32(idx)
		}
		split := api.RowGroupSplit{
			Ordinal:         ordinal,
			StartRowOrdinal: start,
			RowCount:        footer.RowCount,
			Bytes:           footer.Bytes,
			LowerBounds:     cloneIntBytesMap(footer.LowerBounds),
			UpperBounds:     cloneIntBytesMap(footer.UpperBounds),
			NullValueCounts: cloneIntInt64Map(footer.NullValueCounts),
			ValueCounts:     cloneIntInt64Map(footer.ValueCounts),
		}
		out = append(out, split)
		if footer.RowCount > 0 {
			start += footer.RowCount
		}
	}
	return out
}

func PruneRowGroupSplits(meta *api.TableMetadata, schema api.Schema, specID int, splits []api.RowGroupSplit, predicates []api.PrunePredicate) ([]api.RowGroupSplit, int) {
	if len(splits) == 0 || len(predicates) == 0 {
		return splits, 0
	}
	pruner := newScanPruner(meta, schema, predicates)
	if pruner.empty() {
		return splits, 0
	}
	out := make([]api.RowGroupSplit, 0, len(splits))
	pruned := 0
	for _, split := range splits {
		file := api.DataFile{
			Content:         api.DataFileContentData,
			SpecID:          specID,
			RecordCount:     split.RowCount,
			FileSizeInBytes: split.Bytes,
			LowerBounds:     split.LowerBounds,
			UpperBounds:     split.UpperBounds,
			NullValueCounts: split.NullValueCounts,
			ValueCounts:     split.ValueCounts,
		}
		if pruner.shouldPruneDataFile(file) {
			pruned++
			continue
		}
		out = append(out, split)
	}
	return out, pruned
}

func cloneIntBytesMap(in map[int][]byte) map[int][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int][]byte, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func cloneIntInt64Map(in map[int]int64) map[int]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
