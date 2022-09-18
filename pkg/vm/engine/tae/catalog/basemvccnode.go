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

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type NodeOp uint16

const (
	NOpNoop NodeOp = iota
	NOpCreate
	NOpDelete
	NOpCustomizedStart
)

type EntryMVCCNode struct {
	CreatedAt, DeletedAt     types.TS
	HasCreateOp, HasDeleteOp bool
	CommittedOpCnt           int
	TotalOp                  int
}

func NewEntryMVCCNode() *EntryMVCCNode {
	return &EntryMVCCNode{}
}

func (un *EntryMVCCNode) HasDropped() bool {
	if !un.DeletedAt.IsEmpty() {
		return true
	}
	return un.HasDeleteOp
}

func (un *EntryMVCCNode) GetCreatedAt() types.TS {
	return un.CreatedAt
}

func (un *EntryMVCCNode) GetDeletedAt() types.TS {
	return un.DeletedAt
}

func (un *EntryMVCCNode) IsCreating() bool {
	return un.CreatedAt.IsEmpty()
}

func (un *EntryMVCCNode) Clone() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt:      un.CreatedAt,
		DeletedAt:      un.DeletedAt,
		HasCreateOp:    un.HasCreateOp,
		HasDeleteOp:    un.HasDeleteOp,
		TotalOp:        un.TotalOp,
		CommittedOpCnt: un.CommittedOpCnt,
	}
}

func (un *EntryMVCCNode) CloneData() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) AddOp(op NodeOp) {
	switch op {
	case NOpCreate:
		un.HasCreateOp = true
	case NOpDelete:
		un.HasDeleteOp = true
	}
	un.TotalOp++
}

func (un *EntryMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &un.CreatedAt); err != nil {
		return
	}
	n += 12
	if err = binary.Read(r, binary.BigEndian, &un.DeletedAt); err != nil {
		return
	}
	n += 12
	var hasCreateOp, hasDeleteOp uint8
	if err = binary.Read(r, binary.BigEndian, &hasCreateOp); err != nil {
		return
	}
	n += 1
	if hasCreateOp == 1 {
		un.HasCreateOp = true
	}
	if err = binary.Read(r, binary.BigEndian, &hasDeleteOp); err != nil {
		return
	}
	n += 1
	if hasDeleteOp == 1 {
		un.HasDeleteOp = true
	}
	return
}
func (un *EntryMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, un.CreatedAt); err != nil {
		return
	}
	n += 12
	if err = binary.Write(w, binary.BigEndian, un.DeletedAt); err != nil {
		return
	}
	n += 12
	var hasCreateOp, hasDeleteOp uint8
	if un.HasCreateOp {
		hasCreateOp = 1
	}
	if err = binary.Write(w, binary.BigEndian, hasCreateOp); err != nil {
		return
	}
	n += 1
	if un.HasDeleteOp {
		hasDeleteOp = 1
	}
	if err = binary.Write(w, binary.BigEndian, hasDeleteOp); err != nil {
		return
	}
	n += 1
	return
}
func (un *EntryMVCCNode) PrepareCommit() (err error) {
	return nil
}
func (un *EntryMVCCNode) String() string {
	return fmt.Sprintf("CreatedAt=%v,DeletedAt=%v", un.CreatedAt, un.DeletedAt)
}
func (un *EntryMVCCNode) IsLastOp() bool {
	return un.CommittedOpCnt == un.TotalOp-1
}
func (un *EntryMVCCNode) ApplyCommit(ts types.TS) (err error) {
	if un.HasCreateOp {
		un.CreatedAt = ts
	}
	if un.HasDeleteOp {
		un.DeletedAt = ts
	}
	return nil
}

func (un *EntryMVCCNode) ReplayCommit(ts types.TS) (err error) {
	if un.HasCreateOp {
		un.CreatedAt = ts
	}
	if un.HasDeleteOp {
		un.DeletedAt = ts
	}
	return nil
}
