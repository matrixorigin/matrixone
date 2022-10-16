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
	"encoding/binary"
	"fmt"
	"io"

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

func newMergeBlocksCmd(tid uint64, droppedSegs, createdSegs, droppedBlks, createdBlks []*common.ID, mapping, fromAddr, toAddr []uint32, txn txnif.AsyncTxn, id uint32) *mergeBlocksCmd {
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

func WriteSegID(w io.Writer, id *common.ID) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, id.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, id.SegmentID); err != nil {
		return
	}
	n = 8 + 8
	return
}

func ReadSegID(r io.Reader, id *common.ID) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &id.SegmentID); err != nil {
		return
	}
	n = 8 + 8
	return
}

func WriteBlkID(w io.Writer, id *common.ID) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, id.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, id.SegmentID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, id.BlockID); err != nil {
		return
	}
	n = 8 + 8 + 8
	return
}

func ReadBlkID(r io.Reader, id *common.ID) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &id.SegmentID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &id.BlockID); err != nil {
		return
	}
	n = 8 + 8 + 8
	return
}

func WriteUint32Array(w io.Writer, array []uint32) (n int64, err error) {
	length := uint32(len(array))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n = 4
	for _, i := range array {
		if err = binary.Write(w, binary.BigEndian, i); err != nil {
			return
		}
		n += 4
	}
	return
}

func ReadUint32Array(r io.Reader) (array []uint32, n int64, err error) {
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n = 4
	array = make([]uint32, length)
	for i := 0; i < int(length); i++ {
		if err = binary.Read(r, binary.BigEndian, &array[i]); err != nil {
			return
		}
		n += 4
	}
	return
}

func (cmd *mergeBlocksCmd) GetType() int16 { return CmdMergeBlocks }
func (cmd *mergeBlocksCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, CmdMergeBlocks); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, cmd.id); err != nil {
		return
	}

	droppedSegsLength := uint32(len(cmd.droppedSegs))
	if err = binary.Write(w, binary.BigEndian, droppedSegsLength); err != nil {
		return
	}
	n = 2 + 4 + 4
	var sn int64
	for _, seg := range cmd.droppedSegs {
		if sn, err = WriteSegID(w, seg); err != nil {
			return
		}
		n += sn
	}

	createdSegsLength := uint32(len(cmd.createdSegs))
	if err = binary.Write(w, binary.BigEndian, createdSegsLength); err != nil {
		return
	}
	n += 4
	for _, seg := range cmd.createdSegs {
		if sn, err = WriteSegID(w, seg); err != nil {
			return
		}
		n += sn
	}

	droppedBlksLength := uint32(len(cmd.droppedBlks))
	if err = binary.Write(w, binary.BigEndian, droppedBlksLength); err != nil {
		return
	}
	n += 4
	for _, blk := range cmd.droppedBlks {
		if sn, err = WriteBlkID(w, blk); err != nil {
			return
		}
		n += sn
	}

	createdBlksLength := uint32(len(cmd.createdBlks))
	if err = binary.Write(w, binary.BigEndian, createdBlksLength); err != nil {
		return
	}
	n += 4
	for _, blk := range cmd.createdBlks {
		if sn, err = WriteBlkID(w, blk); err != nil {
			return
		}
		n += sn
	}

	if sn, err = WriteUint32Array(w, cmd.mapping); err != nil {
		return
	}
	n += sn
	if sn, err = WriteUint32Array(w, cmd.toAddr); err != nil {
		return
	}
	n += sn
	if sn, err = WriteUint32Array(w, cmd.fromAddr); err != nil {
		return
	}
	n += sn
	return
}
func (cmd *mergeBlocksCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.id); err != nil {
		return
	}
	n = 4
	dropSegmentLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &dropSegmentLength); err != nil {
		return
	}
	var sn int64
	n += 4
	cmd.droppedSegs = make([]*common.ID, dropSegmentLength)
	for i := 0; i < int(dropSegmentLength); i++ {
		id := &common.ID{}
		if sn, err = ReadSegID(r, id); err != nil {
			return
		}
		n += sn
		cmd.droppedSegs[i] = id
	}

	createSegmentLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &createSegmentLength); err != nil {
		return
	}
	n += 4
	cmd.createdSegs = make([]*common.ID, createSegmentLength)
	for i := 0; i < int(createSegmentLength); i++ {
		id := &common.ID{}
		if sn, err = ReadSegID(r, id); err != nil {
			return
		}
		cmd.createdSegs[i] = id
		n += sn
	}

	dropBlkLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &dropBlkLength); err != nil {
		return
	}
	n += 4
	cmd.droppedBlks = make([]*common.ID, dropBlkLength)
	for i := 0; i < int(dropBlkLength); i++ {
		id := &common.ID{}
		if sn, err = ReadBlkID(r, id); err != nil {
			return
		}
		cmd.droppedBlks[i] = id
		n += sn
	}
	createBlkLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &createBlkLength); err != nil {
		return
	}
	n += 4
	cmd.createdBlks = make([]*common.ID, createBlkLength)
	for i := 0; i < int(createBlkLength); i++ {
		id := &common.ID{}
		if sn, err = ReadBlkID(r, id); err != nil {
			return
		}
		cmd.createdBlks[i] = id
		n += sn
	}

	if cmd.mapping, sn, err = ReadUint32Array(r); err != nil {
		return
	}
	n += sn
	if cmd.toAddr, sn, err = ReadUint32Array(r); err != nil {
		return
	}
	n += sn
	if cmd.fromAddr, sn, err = ReadUint32Array(r); err != nil {
		return
	}
	n += sn
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
	s = fmt.Sprintf("%s ];FromFormat=%v;ToFormat=%v;Mapping=%v", s, cmd.fromAddr, cmd.toAddr, cmd.mapping)
	return s
}
func (cmd *mergeBlocksCmd) ApplyCommit()                  {}
func (cmd *mergeBlocksCmd) ApplyRollback()                {}
func (cmd *mergeBlocksCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
