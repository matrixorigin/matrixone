// Copyright 2021 Matrix Origin
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

package catalog

import "github.com/matrixorigin/matrixone/pkg/container/types"

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

func (es EntryState) Repr() string {
	switch es {
	case ES_Appendable:
		return "Appendable"
	case ES_NotAppendable:
		return "NonAppendable"
	case ES_Frozen:
		return "Frozen"
	}
	panic("not supported")
}

type PersistentType int8

const (
	PT_Permanent PersistentType = iota
	PT_Temporary
)

type TableType int8

const (
	TT_Ordinary TableType = iota
	TT_Index
	TT_Sequence
	TT_View
	TT_MaterializedView
)

func EstimateColumnBlockSize(colIdx int, rows uint32, meta *BlockEntry) uint32 {
	switch meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx].Type.Oid {
	case types.T_json, types.T_char, types.T_varchar:
		return rows * 2 * 4
	default:
		return rows * uint32(meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx].Type.Size)
	}
}

func EstimateBlockSize(meta *BlockEntry, rows uint32) uint32 {
	size := uint32(0)
	for colIdx := range meta.GetSegment().GetTable().GetSchema().ColDefs {
		size += EstimateColumnBlockSize(colIdx, rows, meta)
	}
	return size
}
