package updates

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func init() {
	txnif.RegisterCmdFactory(txnbase.CmdUpdate, func(int16) txnif.TxnCmd {
		return NewEmptyUpdateCmd()
	})
}

type UpdateCmd struct {
	*txnbase.BaseCustomizedCmd
	updates *BlockUpdates
}

func NewEmptyUpdateCmd() *UpdateCmd {
	updates := NewEmptyBlockUpdates()
	return NewUpdateCmd(0, updates)
}

func NewUpdateCmd(id uint32, updates *BlockUpdates) *UpdateCmd {
	impl := &UpdateCmd{
		updates: updates,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (c *UpdateCmd) String() string {
	return ""
}

func (c *UpdateCmd) GetType() int16 { return txnbase.CmdUpdate }

func (c *UpdateCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	err = c.updates.WriteTo(w)
	return
}

func (c *UpdateCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	err = c.updates.ReadFrom(r)
	return
}

func (c *UpdateCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *UpdateCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	return c.ReadFrom(bbuf)
}
