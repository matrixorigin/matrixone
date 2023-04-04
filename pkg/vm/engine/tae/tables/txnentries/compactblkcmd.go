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

type compactBlockCmd struct {
	txnbase.BaseCmd
	from *common.ID
	to   *common.ID
	txn  txnif.AsyncTxn
	id   uint32
}

func newCompactBlockCmd(from, to *common.ID, txn txnif.AsyncTxn, id uint32) *compactBlockCmd {
	return &compactBlockCmd{
		txn:  txn,
		from: from,
		to:   to,
		id:   id,
	}
}
func (cmd *compactBlockCmd) GetType() int16 { return CmdCompactBlock }
func (cmd *compactBlockCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := CmdCompactBlock
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&cmd.id)); err != nil {
		return
	}
	if _, err = w.Write(common.EncodeID(cmd.from)); err != nil {
		return
	}
	if _, err = w.Write(common.EncodeID(cmd.to)); err != nil {
		return
	}
	n = 2 + 4 + 2*common.IDSize
	return
}
func (cmd *compactBlockCmd) ReadFrom(r io.Reader) (n int64, err error) {
	cmd.from = &common.ID{}
	cmd.to = &common.ID{}
	if _, err = r.Read(types.EncodeUint32(&cmd.id)); err != nil {
		return
	}
	if _, err = r.Read(common.EncodeID(cmd.from)); err != nil {
		return
	}
	if _, err = r.Read(common.EncodeID(cmd.to)); err != nil {
		return
	}
	n = 4 + 2*common.IDSize
	return
}
func (cmd *compactBlockCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *compactBlockCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
func (cmd *compactBlockCmd) Desc() string {
	return fmt.Sprintf("CmdName=CPCT;CSN=%d;From=%s;To=%s", cmd.id, cmd.from.BlockString(), cmd.to.BlockString())
}
func (cmd *compactBlockCmd) String() string {
	return fmt.Sprintf("CmdName=CPCT;CSN=%d;From=%s;To=%s", cmd.id, cmd.from.BlockString(), cmd.to.BlockString())
}
func (cmd *compactBlockCmd) VerboseString() string {
	return fmt.Sprintf("CmdName=CPCT;CSN=%d;From=%s;To=%s", cmd.id, cmd.from.BlockString(), cmd.to.BlockString())
}
func (cmd *compactBlockCmd) ApplyCommit()                  {}
func (cmd *compactBlockCmd) ApplyRollback()                {}
func (cmd *compactBlockCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
