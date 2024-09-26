// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package api

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	OpMethodName = map[OpCode]string{
		OpCode_OpPing:             "Ping",
		OpCode_OpFlush:            "Flush",
		OpCode_OpCheckpoint:       "Checkpoint",
		OpCode_OpInspect:          "Inspect",
		OpCode_OpAddFaultPoint:    "AddFaultPoint",
		OpCode_OpBackup:           "Backup",
		OpCode_OpTraceSpan:        "TraceSpan",
		OpCode_OpGlobalCheckpoint: "GlobalCheckpoint",
		OpCode_OpInterceptCommit:  "InterceptCommit",
		OpCode_OpCommitMerge:      "CommitMerge",
		OpCode_OpDiskDiskCleaner:  "DiskCleaner",
	}
)

func MustMarshalTblExtra(info *SchemaExtra) []byte {
	if info == nil {
		return []byte{}
	}
	data, err := info.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func MustUnmarshalTblExtra(data []byte) *SchemaExtra {
	info := &SchemaExtra{}
	if len(data) == 0 {
		return info
	}
	if err := info.Unmarshal(data); err != nil {
		panic(err)
	}
	return info
}

func NewUpdateConstraintReq(did, tid uint64, cstr string) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_UpdateConstraint,
		Operation: &AlterTableReq_UpdateCstr{
			&AlterTableConstraint{Constraints: []byte(cstr)},
		},
	}
}

func NewUpdateCommentReq(did, tid uint64, comment string) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_UpdateComment,
		Operation: &AlterTableReq_UpdateComment{
			&AlterTableComment{Comment: comment},
		},
	}
}

func NewRenameTableReq(did, tid uint64, old, new string) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_RenameTable,
		Operation: &AlterTableReq_RenameTable{
			&AlterTableRenameTable{OldName: old, NewName: new},
		},
	}
}

func NewAddColumnReq(did, tid uint64, name string, typ *plan.Type, insertAt int32) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_AddColumn,
		Operation: &AlterTableReq_AddColumn{
			&AlterTableAddColumn{
				Column: &plan.ColDef{
					Name:       strings.ToLower(name),
					OriginName: name,
					Typ:        *typ,
					Default: &plan.Default{
						NullAbility:  true,
						Expr:         nil,
						OriginString: "",
					},
				},
				InsertPosition: insertAt,
			},
		},
	}
}

func NewRemoveColumnReq(did, tid uint64, idx, seqnum uint32) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_DropColumn,
		Operation: &AlterTableReq_DropColumn{
			&AlterTableDropColumn{
				LogicalIdx:  idx,
				SequenceNum: seqnum,
			},
		},
	}
}

func NewAddPartitionReq(did, tid uint64, partitionDef *plan.PartitionByDef) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_AddPartition,
		Operation: &AlterTableReq_AddPartition{
			AddPartition: &AlterTableAddPartition{
				PartitionDef: partitionDef,
			},
		},
	}
}

func NewRenameColumnReq(did, tid uint64, oldname, newname string, seqnum uint32) *AlterTableReq {
	return &AlterTableReq{
		DbId:    did,
		TableId: tid,
		Kind:    AlterKind_RenameColumn,
		Operation: &AlterTableReq_RenameCol{
			&AlterTableRenameCol{
				OldName:     oldname,
				NewName:     newname,
				SequenceNum: seqnum,
			},
		},
	}
}
func (m *SyncLogTailReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *SyncLogTailReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *SyncLogTailResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *SyncLogTailResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *TNStringResponse) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *TNStringResponse) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *PrecommitWriteCmd) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *PrecommitWriteCmd) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *MergeCommitEntry) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *MergeCommitEntry) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *CheckpointResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CheckpointResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

// To reduce memory consumption

type TransferMaps []TransferMap
type TransferMap map[uint32]TransferDestPos
type TransferDestPos struct {
	ObjIdx uint8
	BlkIdx uint16
	RowIdx uint32
}
