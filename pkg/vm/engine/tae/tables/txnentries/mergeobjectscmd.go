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

type mergeObjectsCmd struct {
	txnbase.BaseCmd
	tid         uint64
	droppedObjs []*common.ID
	createdObjs []*common.ID
	droppedBlks []*common.ID
	createdBlks []*common.ID
	txn         txnif.AsyncTxn
	id          uint32
}

func newMergeBlocksCmd(
	tid uint64,
	droppedObjs, createdObjs, droppedBlks, createdBlks []*common.ID,
	txn txnif.AsyncTxn,
	id uint32) *mergeObjectsCmd {
	return &mergeObjectsCmd{
		tid:         tid,
		droppedObjs: droppedObjs,
		createdObjs: createdObjs,
		droppedBlks: droppedBlks,
		createdBlks: createdBlks,
		txn:         txn,
		id:          id,
	}
}

func (cmd *mergeObjectsCmd) GetType() uint16 { return IOET_WALTxnCommand_Merge }

func (cmd *mergeObjectsCmd) WriteTo(w io.Writer) (n int64, err error) {
	typ := IOET_WALTxnCommand_Merge
	if _, err = w.Write(types.EncodeUint16(&typ)); err != nil {
		return
	}
	n = 2
	ver := IOET_WALTxnCommand_Merge_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n = 2
	return
}
func (cmd *mergeObjectsCmd) ReadFrom(r io.Reader) (n int64, err error) {
	return
}
func (cmd *mergeObjectsCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *mergeObjectsCmd) UnmarshalBinary(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}

func (cmd *mergeObjectsCmd) Desc() string {
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

func (cmd *mergeObjectsCmd) String() string {
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
func (cmd *mergeObjectsCmd) VerboseString() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedBlks {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdBlks {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ]", s)
	return s
}
func (cmd *mergeObjectsCmd) ApplyCommit()                  {}
func (cmd *mergeObjectsCmd) ApplyRollback()                {}
func (cmd *mergeObjectsCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
