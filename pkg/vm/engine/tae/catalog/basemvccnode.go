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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type EntryMVCCNode struct {
	CreatedAt, DeletedAt types.TS
}

// Dropped committed
func (un *EntryMVCCNode) HasDropCommitted() bool {
	return !un.DeletedAt.IsEmpty() && un.DeletedAt != txnif.UncommitTS
}

// Dropped committed or uncommitted
func (un *EntryMVCCNode) HasDropIntent() bool {
	return !un.DeletedAt.IsEmpty()
}

func (un *EntryMVCCNode) GetCreatedAt() types.TS {
	return un.CreatedAt
}

func (un *EntryMVCCNode) GetDeletedAt() types.TS {
	return un.DeletedAt
}

func (un *EntryMVCCNode) IsCreating() bool {
	return un.CreatedAt.Equal(txnif.UncommitTS)
}

func (un *EntryMVCCNode) Clone() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) CloneData() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) Delete() {
	un.DeletedAt = txnif.UncommitTS
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
func (un *EntryMVCCNode) String() string {
	return fmt.Sprintf("[C@%s,D@%s]", un.CreatedAt.ToString(), un.DeletedAt.ToString())
}
func (un *EntryMVCCNode) ApplyCommit(ts types.TS) (err error) {
	if un.CreatedAt == txnif.UncommitTS {
		un.CreatedAt = ts
	}
	if un.DeletedAt == txnif.UncommitTS {
		un.DeletedAt = ts
	}
	return nil
}
