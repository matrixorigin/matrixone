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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	CmdAppend int16 = txnbase.CmdCustomized + iota
)

func init() {
	txnif.RegisterCmdFactory(CmdAppend, func(int16) txnif.TxnCmd {
		return NewEmptyAppendCmd()
	})
}

type AppendCmd struct {
	*txnbase.BaseCustomizedCmd
	*txnbase.ComposedCmd
	Infos []*appendInfo
	Ts    types.TS
	Node  InsertNode
}

func NewEmptyAppendCmd() *AppendCmd {
	cmd := &AppendCmd{
		ComposedCmd: txnbase.NewComposedCmd(),
	}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)
	return cmd
}

func NewAppendCmd(id uint32, node InsertNode) *AppendCmd {
	impl := &AppendCmd{
		ComposedCmd: txnbase.NewComposedCmd(),
		Node:        node,
		Infos:       node.GetAppends(),
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}
func (c *AppendCmd) Desc() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%d;Dests=[", c.ID, c.Ts)
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s %s", s, info.Desc())
	}
	s = fmt.Sprintf("%s];\n%s", s, c.ComposedCmd.ToDesc("\t\t"))
	return s
}
func (c *AppendCmd) String() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%d;Dests=[", c.ID, c.Ts)
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s%s", s, info.String())
	}
	s = fmt.Sprintf("%s];\n%s", s, c.ComposedCmd.ToString("\t\t"))
	return s
}
func (c *AppendCmd) VerboseString() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%d;Dests=", c.ID, c.Ts)
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s%s", s, info.String())
	}
	s = fmt.Sprintf("%s];\n%s", s, c.ComposedCmd.ToVerboseString("\t\t"))
	return s
}
func (c *AppendCmd) Close()         {}
func (c *AppendCmd) GetType() int16 { return CmdAppend }
func (c *AppendCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(c.Infos))); err != nil {
		return
	}
	var sn int64
	n = 10
	for _, info := range c.Infos {
		if sn, err = info.WriteTo(w); err != nil {
			return
		}
		n += sn
	}
	sn, err = c.ComposedCmd.WriteTo(w)
	n += sn
	if err != nil {
		return n, err
	}
	if err = binary.Write(w, binary.BigEndian, c.Node.GetTxn().GetPrepareTS()); err != nil {
		return
	}
	n += 16
	return
}

func (c *AppendCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	var sn int64
	n = 8
	c.Infos = make([]*appendInfo, length)
	for i := 0; i < int(length); i++ {
		c.Infos[i] = &appendInfo{dest: &common.ID{}}
		if sn, err = c.Infos[i].ReadFrom(r); err != nil {
			return
		}
		n += sn
	}
	cc, sn, err := txnbase.BuildCommandFrom(r)
	c.ComposedCmd = cc.(*txnbase.ComposedCmd)
	n += sn
	if err != nil {
		return n, err
	}
	if err = binary.Read(r, binary.BigEndian, &c.Ts); err != nil {
		return
	}
	n += 16
	return
}

func (c *AppendCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *AppendCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}
