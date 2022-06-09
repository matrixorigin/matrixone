package util

import (
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/txn/pb"
	"go.uber.org/zap"
)

// TxnIDField returns a txn id field
func TxnIDField(meta pb.TxnMeta) zap.Field {
	return zap.String("txn-id", hex.EncodeToString(meta.ID))
}
