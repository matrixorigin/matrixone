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

package objectio

import (
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const (
	BlockMaxRows = 8192

	SEQNUM_UPPER    = math.MaxUint16 - 5 // reserved 5 column for special committs、committs etc.
	SEQNUM_ROWID    = math.MaxUint16
	SEQNUM_ABORT    = math.MaxUint16 - 1
	SEQNUM_COMMITTS = math.MaxUint16 - 2
)

const (
	TombstoneAttr_Rowid_Idx = 0
	TombstoneAttr_PK_Idx    = 1

	// Appendable
	TombstoneAttr_A_PhyAddr_Idx  = 2
	TombstoneAttr_A_CommitTs_Idx = 3
	TombstoneAttr_A_Abort_Idx    = 4

	// non-Appendable tn created
	TombstoneAttr_NA_CommitTs_Idx = 2

	TombstoneAttr_Rowid_SeqNum = TombstoneAttr_Rowid_Idx
	TombstoneAttr_PK_SeqNum    = TombstoneAttr_PK_Idx

	TombstoneAttr_A_PhyAddr_SeqNum = TombstoneAttr_A_PhyAddr_Idx
	TombstoneAttr_CommitTs_SeqNum  = SEQNUM_COMMITTS
	TombstoneAttr_Abort_SeqNum     = SEQNUM_ABORT

	TombstonePrimaryKeyIdx = TombstoneAttr_Rowid_Idx
)

const (
	PhysicalAddr_Attr    = "__mo_rowid"
	DefaultCommitTS_Attr = "__mo_%1_commit_time"
	DefaultAbort_Attr    = "__mo_%1_abort"

	TombstoneAttr_Rowid_Attr    = "__mo_%1_delete_rowid"
	TombstoneAttr_PK_Attr       = "__mo_%1_pk_val"
	TombstoneAttr_CommitTs_Attr = DefaultCommitTS_Attr
	TombstoneAttr_Abort_Attr    = DefaultAbort_Attr
)

type HiddenColumnSelection uint64

const (
	HiddenColumnSelection_PhysicalAddr HiddenColumnSelection = 1 << iota
	HiddenColumnSelection_CommitTS
	HiddenColumnSelection_Abort
)

const HiddenColumnSelection_None HiddenColumnSelection = 0

const InvalidSpecialColumnPosition = math.MaxUint16

// SpecialColumnLayout describes the physical metadata positions of the
// appendable-only columns. Object writers map special seqnums, in declaration
// order, immediately after MaxSeqnum. PhysicalAddr may therefore appear before
// or after CommitTS, depending on the writer. The positions are stable across
// sparse user schemas and do not depend on the total column count.
//
// Old appendable objects contain only CommitTS. New appendable objects contain
// CommitTS followed by Abort.
type SpecialColumnLayout struct {
	PhysicalAddr uint16
	CommitTS     uint16
	Abort        uint16
}

func (layout SpecialColumnLayout) Resolve(seqnum uint16) (uint16, bool) {
	switch seqnum {
	case SEQNUM_COMMITTS:
		return layout.CommitTS, layout.CommitTS != InvalidSpecialColumnPosition
	case SEQNUM_ABORT:
		return layout.Abort, layout.Abort != InvalidSpecialColumnPosition
	default:
		return InvalidSpecialColumnPosition, false
	}
}

// ResolveSpecialColumnLayout resolves appendable special columns by their
// format-defined positions and validates their types. This keeps old
// commitTS-only objects readable while preventing a user TS/bool column from
// being mistaken for a hidden column.
func ResolveSpecialColumnLayout(block BlockObject) SpecialColumnLayout {
	layout := SpecialColumnLayout{
		PhysicalAddr: InvalidSpecialColumnPosition,
		CommitTS:     InvalidSpecialColumnPosition,
		Abort:        InvalidSpecialColumnPosition,
	}
	metaColumnCount := block.GetMetaColumnCount()
	if metaColumnCount == 0 {
		return layout
	}

	maxSeqnum := block.GetMaxSeqnum()
	if maxSeqnum >= SEQNUM_UPPER {
		return layout
	}
	pos := maxSeqnum + 1
	if pos < metaColumnCount &&
		block.ColumnMeta(pos).DataType() == uint8(types.T_Rowid) {
		layout.PhysicalAddr = pos
		pos++
	}

	commitPos := pos
	if commitPos >= metaColumnCount ||
		block.ColumnMeta(commitPos).DataType() != uint8(types.T_TS) {
		return layout
	}
	layout.CommitTS = commitPos

	abortPos := commitPos + 1
	if abortPos < metaColumnCount &&
		block.ColumnMeta(abortPos).DataType() == uint8(types.T_Rowid) {
		layout.PhysicalAddr = abortPos
		abortPos++
	}
	if abortPos < metaColumnCount &&
		block.ColumnMeta(abortPos).DataType() == uint8(types.T_bool) {
		layout.Abort = abortPos
	}
	return layout
}

var (
	TombstoneSeqnums_CN_Created         = []uint16{0, 1}
	TombstoneSeqnums_CN_Created_PhyAddr = []uint16{0, 1, SEQNUM_ROWID}
	TombstoneSeqnums_DN_Created         = []uint16{0, 1, TombstoneAttr_CommitTs_SeqNum}
	TombstoneSeqnums_DN_Created_PhyAddr = []uint16{0, 1, TombstoneAttr_CommitTs_SeqNum, SEQNUM_ROWID}

	TombstoneColumns_CN_Created         = []int{0, 1}
	TombstoneColumns_CN_Created_PhyAddr = []int{0, 1, SEQNUM_ROWID}
	TombstoneColumns_TN_Created         = []int{0, 1, TombstoneAttr_CommitTs_SeqNum}
	TombstoneColumns_TN_Created_PhyAddr = []int{0, 1, TombstoneAttr_CommitTs_SeqNum, SEQNUM_ROWID}

	TombstoneAttrs_CN_Created         = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr}
	TombstoneAttrs_CN_Created_PhyAddr = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr, PhysicalAddr_Attr}
	TombstoneAttrs_TN_Created         = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr, TombstoneAttr_CommitTs_Attr}
	TombstoneAttrs_TN_Created_PhyAddr = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr, TombstoneAttr_CommitTs_Attr, PhysicalAddr_Attr}
)

const ZoneMapSize = index.ZMSize

func IsPhysicalAddr(attr string) bool {
	return attr == PhysicalAddr_Attr
}

func normalizeTombstoneHiddenColumns(hidden HiddenColumnSelection) HiddenColumnSelection {
	// Abort is part of appendable MVCC metadata and is never meaningful without
	// the commit timestamp that defines the row's visibility interval.
	if hidden&HiddenColumnSelection_Abort != 0 {
		hidden |= HiddenColumnSelection_CommitTS
	}
	return hidden
}

func GetTombstoneAttrs(hidden HiddenColumnSelection) []string {
	hidden = normalizeTombstoneHiddenColumns(hidden)
	var attrs []string
	if hidden&HiddenColumnSelection_PhysicalAddr != 0 &&
		hidden&HiddenColumnSelection_CommitTS != 0 {
		attrs = TombstoneAttrs_TN_Created_PhyAddr
	} else if hidden&HiddenColumnSelection_PhysicalAddr != 0 {
		attrs = TombstoneAttrs_CN_Created_PhyAddr
	} else if hidden&HiddenColumnSelection_CommitTS != 0 {
		attrs = TombstoneAttrs_TN_Created
	} else {
		attrs = TombstoneAttrs_CN_Created
	}
	attrs = slices.Clone(attrs)
	if hidden&HiddenColumnSelection_Abort != 0 {
		attrs = append(attrs, TombstoneAttr_Abort_Attr)
	}
	return attrs
}

func GetTombstoneSeqnums(hidden HiddenColumnSelection) []uint16 {
	hidden = normalizeTombstoneHiddenColumns(hidden)
	var seqnums []uint16
	if hidden&HiddenColumnSelection_PhysicalAddr != 0 &&
		hidden&HiddenColumnSelection_CommitTS != 0 {
		seqnums = TombstoneSeqnums_DN_Created_PhyAddr
	} else if hidden&HiddenColumnSelection_PhysicalAddr != 0 {
		seqnums = TombstoneSeqnums_CN_Created_PhyAddr
	} else if hidden&HiddenColumnSelection_CommitTS != 0 {
		seqnums = TombstoneSeqnums_DN_Created
	} else {
		seqnums = TombstoneSeqnums_CN_Created
	}
	seqnums = slices.Clone(seqnums)
	if hidden&HiddenColumnSelection_Abort != 0 {
		seqnums = append(seqnums, SEQNUM_ABORT)
	}
	return seqnums
}

func GetTombstoneSchema(
	pk types.Type, hidden HiddenColumnSelection,
) (attrs []string, attrTypes []types.Type) {
	attrs = GetTombstoneAttrs(hidden)
	attrTypes = GetTombstoneTypes(pk, hidden)
	return
}
func GetTombstoneTypes(
	pk types.Type, hidden HiddenColumnSelection,
) []types.Type {
	hidden = normalizeTombstoneHiddenColumns(hidden)
	var typs []types.Type
	if hidden&HiddenColumnSelection_PhysicalAddr != 0 &&
		hidden&HiddenColumnSelection_CommitTS != 0 {
		typs = []types.Type{
			RowidType,
			pk,
			TSType,
			RowidType,
		}
	} else if hidden&HiddenColumnSelection_PhysicalAddr != 0 {
		typs = []types.Type{
			RowidType,
			pk,
			RowidType,
		}
	} else if hidden&HiddenColumnSelection_CommitTS != 0 {
		typs = []types.Type{
			RowidType,
			pk,
			TSType,
		}
	} else {
		typs = []types.Type{
			RowidType,
			pk,
		}
	}
	if hidden&HiddenColumnSelection_Abort != 0 {
		typs = append(typs, types.T_bool.ToType())
	}
	return typs
}

func MustGetPhysicalColumnPosition(seqnums []uint16, colTypes []types.Type) int {
	for i, seqnum := range seqnums {
		if seqnum == SEQNUM_ROWID {
			if colTypes[i] != RowidType {
				panic(fmt.Sprintf("rowid column should be rowid type but got %s", colTypes[i]))
			}
			return i
		}
	}
	return -1
}
