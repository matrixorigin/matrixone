package rpc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnSender is used to send transaction requests to the DN nodes.
type TxnSender interface {
	// Send send request to the specified DN node, and wait for response synchronously.
	// For any reason, if no response is received, the internal will keep retrying until
	// the Context times out.
	Send(context.Context, []txn.TxnRequest) ([]txn.TxnResponse, error)
	// Close the txn sender
	Close() error
}
