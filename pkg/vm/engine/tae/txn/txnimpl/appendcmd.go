package txnimpl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	CmdAppend int16 = txnbase.CmdCustomized + iota
	CmdUpdate
	CmdDelete
)

func init() {
	txnif.RegisterCmdFactory(CmdAppend, func(int16) txnif.TxnCmd {
		return NewEmptyAppendCmd()
	})
}

type AppendCmd struct {
	*txnbase.BaseCustomizedCmd
	txnbase.ComposedCmd
	Node InsertNode
}

func NewEmptyAppendCmd() *AppendCmd {
	return NewAppendCmd(0, nil)
}

func NewAppendCmd(id uint32, node InsertNode) *AppendCmd {
	impl := &AppendCmd{
		ComposedCmd: *txnbase.NewComposedCmd(),
		Node:        node,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (c *AppendCmd) String() string {
	s := fmt.Sprintf("AppendCmd: ID=%d", c.ID)
	s = fmt.Sprintf("%s\n%s", s, c.ComposedCmd.ToString("\t"))
	return s
}

func (e *AppendCmd) GetType() int16 { return CmdAppend }
func (c *AppendCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	err = c.ComposedCmd.WriteTo(w)
	return err
}

func (c *AppendCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	err = c.ComposedCmd.ReadFrom(r)
	return
}

func (c *AppendCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *AppendCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := c.ReadFrom(bbuf)
	return err
}
