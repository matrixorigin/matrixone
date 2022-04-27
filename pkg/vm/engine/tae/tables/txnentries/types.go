package txnentries

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"

const (
	CmdCompactBlock int16 = 0x0200
)

func init() {
	txnif.RegisterCmdFactory(CmdCompactBlock, func(int16) txnif.TxnCmd {
		return new(compactBlockCmd)
	})
}
