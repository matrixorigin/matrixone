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

package txnimpl

import (
	"context"
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const MaxNodeRows = 10000

type InsertNode interface {
	Close() error
	Append(data *containers.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	IsPersisted() bool
	PrintDeletes() string
	GetColumnDataByIds([]int, *mpool.MPool) (*containers.BlockView, error)
	GetColumnDataById(context.Context, int, *mpool.MPool) (*containers.ColumnView, error)
	Prefetch(idxes []uint16) error
	FillBlockView(view *containers.BlockView, colIdxes []int, mp *mpool.MPool) (err error)
	FillColumnView(*containers.ColumnView, *mpool.MPool) error
	Window(start, end uint32) (*containers.Batch, error)
	WindowColumn(start, end uint32, pos int) (containers.Vector, error)
	Rows() uint32
	GetValue(col int, row uint32) (any, bool, error)
	MakeCommand(uint32) (txnif.TxnCmd, error)
	AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) *appendInfo
	GetAppends() []*appendInfo
	GetTxn() txnif.AsyncTxn
}

type appendInfo struct {
	seq              uint32
	srcOff, srcLen   uint32
	dest             common.ID
	destOff, destLen uint32
}

const (
	AppendInfoSize int64 = int64(unsafe.Sizeof(appendInfo{}))
)

func EncodeAppendInfo(info *appendInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(info)), AppendInfoSize)
}

func (info *appendInfo) GetDest() *common.ID {
	return &info.dest
}
func (info *appendInfo) GetSrcOff() uint32 {
	return info.srcOff
}
func (info *appendInfo) GetSrcLen() uint32 {
	return info.srcLen
}
func (info *appendInfo) GetDestOff() uint32 {
	return info.destOff
}
func (info *appendInfo) GetDestLen() uint32 {
	return info.destLen
}
func (info *appendInfo) Desc() string {
	return info.dest.BlockString()
}
func (info *appendInfo) String() string {
	s := fmt.Sprintf("[From=[%d:%d];To=%s[%d:%d]]",
		info.srcOff, info.srcLen+info.srcOff, info.dest.BlockString(), info.destOff, info.destLen+info.destOff)
	return s
}
func (info *appendInfo) WriteTo(w io.Writer) (n int64, err error) {
	_, err = w.Write(EncodeAppendInfo(info))
	n = AppendInfoSize
	return
}
func (info *appendInfo) ReadFrom(r io.Reader) (n int64, err error) {
	_, err = r.Read(EncodeAppendInfo(info))
	n = AppendInfoSize
	return
}

type baseNode struct {
	meta  *catalog.ObjectEntry
	table *txnTable
}

func newBaseNode(
	tbl *txnTable,
	meta *catalog.ObjectEntry,
) *baseNode {
	return &baseNode{
		meta:  meta,
		table: tbl,
	}
}

func (n *baseNode) IsPersisted() bool {
	return n.meta.HasPersistedData()
}

func (n *baseNode) GetTxn() txnif.AsyncTxn {
	return n.table.store.txn
}
