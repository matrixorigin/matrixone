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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

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
					Name: name,
					Typ:  typ,
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

func (m *PrecommitWriteCmd) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *PrecommitWriteCmd) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}
