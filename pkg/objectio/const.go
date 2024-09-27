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
	DefaultRowid_Attr    = "__mo_rowid"
	DefaultCommitTS_Attr = "__mo_%1_commit_time"
	DefaultAbort_Attr    = "__mo_%1_abort"

	TombstoneAttr_Rowid_Attr    = "__mo_%1_delete_rowid"
	TombstoneAttr_PK_Attr       = "__mo_%1_pk_val"
	TombstoneAttr_CommitTs_Attr = DefaultCommitTS_Attr
	TombstoneAttr_Abort_Attr    = DefaultAbort_Attr
)

var (
	TombstoneSeqnums_CN_Created = []uint16{0, 1}
	TombstoneSeqnums_DN_Created = []uint16{0, 1, TombstoneAttr_CommitTs_SeqNum}

	TombstoneColumns_CN_Created = []int{0, 1}
	TombstoneColumns_TN_Created = []int{0, 1, TombstoneAttr_CommitTs_SeqNum}

	TombstoneAttrs_CN_Created = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr}
	TombstoneAttrs_TN_Created = []string{TombstoneAttr_Rowid_Attr, TombstoneAttr_PK_Attr, TombstoneAttr_CommitTs_Attr}
)

const ZoneMapSize = index.ZMSize

func GetTombstoneCommitTSAttrIdx(columnCnt uint16) uint16 {
	if columnCnt == 3 {
		return TombstoneAttr_NA_CommitTs_Idx
	} else if columnCnt == 4 {
		return TombstoneAttr_A_CommitTs_Idx
	}
	panic(fmt.Sprintf("invalid tombstone column count %d", columnCnt))
}

func GetTombstoneSchema(pk types.Type, withHidden bool) (attrs []string, attrTypes []types.Type) {
	if withHidden {
		attrs = TombstoneAttrs_TN_Created
	} else {
		attrs = TombstoneAttrs_CN_Created
	}
	attrTypes = GetTombstoneTypes(pk, withHidden)
	return
}
func GetTombstoneTypes(pk types.Type, withHidden bool) []types.Type {
	if withHidden {
		return []types.Type{
			RowidType,
			pk,
			TSType,
		}
	}
	return []types.Type{
		RowidType,
		pk,
	}
}
