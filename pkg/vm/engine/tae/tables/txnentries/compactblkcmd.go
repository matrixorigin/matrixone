package txnentries

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type compactBlockCmd struct {
	txnbase.BaseCustomizedCmd
	cmdType int16
}

func NewTxnBlockCmd(id uint32, cmdType int16) *compactBlockCmd {
	cmd := &compactBlockCmd{
		cmdType: cmdType,
	}
	cmd.BaseCustomizedCmd = *txnbase.NewBaseCustomizedCmd(id, cmd)
	return cmd
}

func (cmd *compactBlockCmd) GetType() int16                   { return cmd.cmdType }
func (cmd *compactBlockCmd) WriteTo(w io.Writer) (err error)  { return }
func (cmd *compactBlockCmd) ReadFrom(r io.Reader) (err error) { return }
func (cmd *compactBlockCmd) Marshal() (buf []byte, err error) { return }
func (cmd *compactBlockCmd) Unmarshal(buf []byte) (err error) { return }
func (cmd *compactBlockCmd) String() string                   { return "" }
