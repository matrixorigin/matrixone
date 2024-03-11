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
	"io"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type EmptyMVCCNode struct{}

func NewEmptyEmptyMVCCNode() *EmptyMVCCNode {
	return &EmptyMVCCNode{}
}

func (e *EmptyMVCCNode) CloneAll() *EmptyMVCCNode {
	node := &EmptyMVCCNode{}
	return node
}

func (e *EmptyMVCCNode) CloneData() *EmptyMVCCNode {
	return &EmptyMVCCNode{}
}

func (e *EmptyMVCCNode) String() string {
	return ""
}

// for create drop in one txn
func (e *EmptyMVCCNode) Update(vun *EmptyMVCCNode)           {}
func (e *EmptyMVCCNode) IdempotentUpdate(vun *EmptyMVCCNode) {}

func (e *EmptyMVCCNode) WriteTo(w io.Writer) (n int64, err error) { return }

func (e *EmptyMVCCNode) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) { return }

type DBNode struct {
	acInfo    accessInfo
	name      string
	datType   string
	createSql string
}

func (node *DBNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	if node.name, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if sn, err = node.acInfo.ReadFrom(r); err != nil {
		return
	}
	n += sn
	if node.createSql, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if node.datType, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	return
}

func (node *DBNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if sn, err = objectio.WriteString(node.name, w); err != nil {
		return
	}
	n += sn
	if sn, err = node.acInfo.WriteTo(w); err != nil {
		return
	}
	n += sn
	if sn, err = objectio.WriteString(node.createSql, w); err != nil {
		return
	}
	n += sn
	if sn, err = objectio.WriteString(node.datType, w); err != nil {
		return
	}
	n += sn
	return
}

// only used in ut test
func (node *DBNode) TestSetAccId(id uint32) {
	node.acInfo.TenantID = id
}
