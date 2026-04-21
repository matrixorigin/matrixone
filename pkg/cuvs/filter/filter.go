// Copyright 2021 - 2022 Matrix Origin
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

// Package filter holds pre-filter (INCLUDE column) metadata types for GPU
// vector indexes. Kept as a leaf package (no downstream imports) so both the
// SQL/plan layer and pkg/cuvs can depend on it without creating a cycle —
// pkg/cuvs already imports pkg/vectorindex (via multi_index.go), so the
// filter-meta types cannot live in pkg/vectorindex or directly in pkg/cuvs.
//
// Builds without the gpu tag: only this file is compiled from pkg/cuvs/filter,
// letting the plan / DDL layer construct metadata even on CPU-only builds.
package filter

// ColType identifies the physical type of a filter (INCLUDE) column.
// Values MUST match the C++ matrixone::FilterColType enum in
// cgo/cuvs/filter.hpp — do not reorder.
type ColType int32

const (
	ColTypeInt32   ColType = 0
	ColTypeInt64   ColType = 1
	ColTypeFloat32 ColType = 2
	ColTypeFloat64 ColType = 3
	ColTypeUint64  ColType = 4 // VARCHAR stored as 64-bit hash
)

// ElemSize returns the byte width of a single filter column value.
func (t ColType) ElemSize() uint32 {
	switch t {
	case ColTypeInt32, ColTypeFloat32:
		return 4
	case ColTypeInt64, ColTypeFloat64, ColTypeUint64:
		return 8
	}
	return 0
}

// ColumnMeta describes one INCLUDE column. The DDL layer populates this from
// the SQL column's types.T; the table function serialises the slice into the
// JSON format accepted by gpu_<idx>_set_filter_columns.
type ColumnMeta struct {
	Name    string  `json:"name"`
	TypeOid ColType `json:"type"`
}
