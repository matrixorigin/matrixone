package txnif

const (
	UncommitTS = ^uint64(0)
)

const (
	TxnStateActive int32 = iota
	TxnStateCommitting
	TxnStateRollbacking
	TxnStateCommitted
	TxnStateRollbacked
)

func TxnStrState(state int32) string {
	switch state {
	case TxnStateActive:
		return "Active"
	case TxnStateCommitting:
		return "Committing"
	case TxnStateRollbacking:
		return "Rollbacking"
	case TxnStateCommitted:
		return "Committed"
	case TxnStateRollbacked:
		return "Rollbacked"
	}
	panic("state not support")
}
