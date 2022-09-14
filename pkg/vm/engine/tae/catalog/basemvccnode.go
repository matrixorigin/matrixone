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
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type EntryMVCCNode struct {
	CreatedAt, DeletedAt types.TS
	Deleted              bool
}

func (un *EntryMVCCNode) HasDropped() bool {
	return un.Deleted
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
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
		Deleted:   un.Deleted,
	}
}

func (un *EntryMVCCNode) ApplyDeleteLocked() (err error) {
	if un.Deleted {
		panic("cannot apply delete to deleted node")
	}
	un.Deleted = true
	return
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
	if !un.DeletedAt.IsEmpty() {
		un.Deleted = true
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
	return
}
func (un *EntryMVCCNode) PrepareCommit() (err error) {
	return nil
}
func (un *EntryMVCCNode) ApplyCommit(ts types.TS) (err error) {
	if un.CreatedAt.IsEmpty() {
		un.CreatedAt = ts
	}
	if un.Deleted {
		un.DeletedAt = ts
	}
	return nil
}

func (un *EntryMVCCNode) ReplayCommit(ts types.TS) (err error) {
	if un.CreatedAt.IsEmpty() {
		un.CreatedAt = ts
	} else {
		un.Deleted=true
		un.DeletedAt = ts
	}
	return nil
}