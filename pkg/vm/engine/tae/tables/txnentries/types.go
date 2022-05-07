package txnentries

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"

const (
	CmdCompactBlock int16 = 0x0200 + iota
	CmdMergeBlocks
)

func init() {
	txnif.RegisterCmdFactory(CmdCompactBlock, func(int16) txnif.TxnCmd {
		return new(compactBlockCmd)
	})
	txnif.RegisterCmdFactory(CmdMergeBlocks, func(int16) txnif.TxnCmd {
		return new(mergeBlocksCmd)
	})
}
