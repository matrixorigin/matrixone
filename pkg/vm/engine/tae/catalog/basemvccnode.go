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
	CreatedAt, DeletedAt types.TS
	NodeOp               []NodeOp
	CommittedOpCnt       int
}

func NewEntryMVCCNode() *EntryMVCCNode {
	return &EntryMVCCNode{
		NodeOp: make([]NodeOp, 0),
	}
}

func (un *EntryMVCCNode) HasDropped() bool {
	if !un.DeletedAt.IsEmpty() {
		return true
	}
	for _, ntype := range un.NodeOp {
		if ntype == NOpDelete {
			return true
		}
	}
	return false
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
	ntypes := make([]NodeOp, len(un.NodeOp))
	copy(ntypes, un.NodeOp)
	return &EntryMVCCNode{
		CreatedAt:      un.CreatedAt,
		DeletedAt:      un.DeletedAt,
		NodeOp:         ntypes,
		CommittedOpCnt: un.CommittedOpCnt,
	}
}

func (un *EntryMVCCNode) CloneData() *EntryMVCCNode {
	ntypes := make([]NodeOp, len(un.NodeOp))
	copy(ntypes, un.NodeOp)
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) AddOp(op NodeOp) {
	un.NodeOp = append(un.NodeOp, op)
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
	length := uint8(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	un.NodeOp = make([]NodeOp, length)
	n += 1
	for i := 0; i < int(length); i++ {
		ntype := NOpNoop
		if err = binary.Read(r, binary.BigEndian, &ntype); err != nil {
			return
		}
		n += 2
		un.NodeOp[i] = ntype
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
	length := uint8(len(un.NodeOp))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 1
	for _, ntype := range un.NodeOp {
		if err = binary.Write(w, binary.BigEndian, &ntype); err != nil {
			return
		}
		n += 2
	}
	return
}
func (un *EntryMVCCNode) PrepareCommit() (err error) {
	return nil
}
func (un *EntryMVCCNode) String() string {
	return fmt.Sprintf("CreatedAt=%v,DeletedAt=%v,Ops=%v", un.CreatedAt, un.DeletedAt, un.NodeOp)
}
func (un *EntryMVCCNode) IsLastOp() bool {
	return un.CommittedOpCnt == len(un.NodeOp)-1
}
func (un *EntryMVCCNode) ApplyCommit(ts types.TS) (err error) {
	ntype := un.NodeOp[un.CommittedOpCnt]
	switch ntype {
	case NOpCreate:
		un.CreatedAt = ts
	case NOpDelete:
		un.DeletedAt = ts
	}
	return nil
}

func (un *EntryMVCCNode) ReplayCommit(ts types.TS) (err error) {
	for _, ntype := range un.NodeOp {
		switch ntype {
		case NOpCreate:
			un.CreatedAt = ts
		case NOpDelete:
			un.DeletedAt = ts
		}
	}
	return nil
}
