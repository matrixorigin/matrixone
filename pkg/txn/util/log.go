package util

import (
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

// TxnIDField returns a txn id field
func TxnIDField(meta txn.TxnMeta) zap.Field {
	return zap.String("txn-id", hex.EncodeToString(meta.ID))
}
