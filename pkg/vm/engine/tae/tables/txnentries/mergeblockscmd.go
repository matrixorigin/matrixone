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

package txnentries

import (
	"bytes"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type mergeBlocksCmd struct {
	txnbase.BaseCmd
	tid         uint64
	droppedSegs []*common.ID
	createdSegs []*common.ID
	droppedBlks []*common.ID
	createdBlks []*common.ID
	mapping     []uint32
	fromAddr    []uint32
	toAddr      []uint32
	txn         txnif.AsyncTxn
	id          uint32
}

func newMergeBlocksCmd(
	tid uint64,
	droppedSegs, createdSegs, droppedBlks, createdBlks []*common.ID,
	mapping, fromAddr, toAddr []uint32,
	txn txnif.AsyncTxn,
	id uint32) *mergeBlocksCmd {
	return &mergeBlocksCmd{
		tid:         tid,
		droppedSegs: droppedSegs,
		createdSegs: createdSegs,
		droppedBlks: droppedBlks,
		createdBlks: createdBlks,
		mapping:     mapping,
		fromAddr:    fromAddr,
		toAddr:      toAddr,
		txn:         txn,
		id:          id,
	}
}

func (cmd *mergeBlocksCmd) GetType() int16 { return CmdMergeBlocks }
func (cmd *mergeBlocksCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := CmdMergeBlocks
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}

	if _, err = w.Write(types.EncodeUint32(&cmd.id)); err != nil {
		return
	}

	droppedSegsLength := uint32(len(cmd.droppedSegs))
	if _, err = w.Write(types.EncodeUint32(&droppedSegsLength)); err != nil {
		return
	}
	n = 2 + 4 + 4
	var sn int64
	for _, seg := range cmd.droppedSegs {
		if _, err = w.Write(common.EncodeID(seg)); err != nil {
			return
		}
		n += common.IDSize
	}

	createdSegsLength := uint32(len(cmd.createdSegs))
	if _, err = w.Write(types.EncodeUint32(&createdSegsLength)); err != nil {
		return
	}
	n += 4
	for _, seg := range cmd.createdSegs {
		if _, err = w.Write(common.EncodeID(seg)); err != nil {
			return
		}
		n += common.IDSize
	}

	droppedBlksLength := uint32(len(cmd.droppedBlks))
	if _, err = w.Write(types.EncodeUint32(&droppedBlksLength)); err != nil {
		return
	}
	n += 4
	for _, blk := range cmd.droppedBlks {
		if _, err = w.Write(common.EncodeID(blk)); err != nil {
			return
		}
		n += common.IDSize
	}

	createdBlksLength := uint32(len(cmd.createdBlks))
	if _, err = w.Write(types.EncodeUint32(&createdBlksLength)); err != nil {
		return
	}
	n += 4
	for _, blk := range cmd.createdBlks {
		if _, err = w.Write(common.EncodeID(blk)); err != nil {
			return
		}
		n += common.IDSize
	}

	buf := types.EncodeSlice[uint32](cmd.toAddr)
	if sn, err = common.WriteBytes(buf, w); err != nil {
		return
	}
	n += sn
	buf = types.EncodeSlice[uint32](cmd.fromAddr)
	if sn, err = common.WriteBytes(buf, w); err != nil {
		return
	}
	n += sn

	return
}
func (cmd *mergeBlocksCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint32(&cmd.id)); err != nil {
		return
	}
	n = 4
	dropSegmentLength := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&dropSegmentLength)); err != nil {
		return
	}
	var sn int64
	n += 4
	cmd.droppedSegs = make([]*common.ID, dropSegmentLength)
	for i := 0; i < int(dropSegmentLength); i++ {
		id := &common.ID{}
		if _, err = r.Read(common.EncodeID(id)); err != nil {
			return
		}
		n += common.IDSize
		cmd.droppedSegs[i] = id
	}

	createSegmentLength := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&createSegmentLength)); err != nil {
		return
	}
	n += 4
	cmd.createdSegs = make([]*common.ID, createSegmentLength)
	for i := 0; i < int(createSegmentLength); i++ {
		id := &common.ID{}
		if _, err = r.Read(common.EncodeID(id)); err != nil {
			return
		}
		n += common.IDSize
		cmd.createdSegs[i] = id
	}

	dropBlkLength := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&dropBlkLength)); err != nil {
		return
	}
	n += 4
	cmd.droppedBlks = make([]*common.ID, dropBlkLength)
	for i := 0; i < int(dropBlkLength); i++ {
		id := &common.ID{}
		if _, err = r.Read(common.EncodeID(id)); err != nil {
			return
		}
		n += common.IDSize
		cmd.droppedBlks[i] = id
	}
	createBlkLength := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&createBlkLength)); err != nil {
		return
	}
	n += 4
	cmd.createdBlks = make([]*common.ID, createBlkLength)
	for i := 0; i < int(createBlkLength); i++ {
		id := &common.ID{}
		if _, err = r.Read(common.EncodeID(id)); err != nil {
			return
		}
		n += common.IDSize
		cmd.createdBlks[i] = id
	}
	var buf []byte
	if buf, sn, err = common.ReadBytes(r); err != nil {
		return
	}
	n += sn
	cmd.toAddr = types.DecodeSlice[uint32](buf)
	if buf, sn, err = common.ReadBytes(r); err != nil {
		return
	}
	n += sn
	cmd.fromAddr = types.DecodeSlice[uint32](buf)

	return
}
func (cmd *mergeBlocksCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *mergeBlocksCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}

func (cmd *mergeBlocksCmd) Desc() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedBlks {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdBlks {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ]", s)
	return s
}

func (cmd *mergeBlocksCmd) String() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedBlks {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdBlks {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ]", s)
	return s
}
func (cmd *mergeBlocksCmd) VerboseString() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedBlks {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdBlks {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ];FromFormat=%v;ToFormat=%v", s, cmd.fromAddr, cmd.toAddr)
	return s
}
func (cmd *mergeBlocksCmd) ApplyCommit()                  {}
func (cmd *mergeBlocksCmd) ApplyRollback()                {}
func (cmd *mergeBlocksCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
