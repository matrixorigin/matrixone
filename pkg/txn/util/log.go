package util

import (
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

// TxnIDField returns a txn id field
func TxnIDField(txnMeta txn.TxnMeta) zap.Field {
	return TxnIDFieldWithID(txnMeta.ID)
}

// TxnIDFieldWithID returns a txn id field
func TxnIDFieldWithID(id []byte) zap.Field {
	return zap.String("txn-id", hex.EncodeToString(id))
}

// TxnDNShardField returns a dn shard zap field
func TxnDNShardField(dn metadata.DNShard) zap.Field {
	return zap.String("dn-shard", dn.DebugString())
}

// TxnField returns a txn zap field
func TxnField(txnMeta txn.TxnMeta) zap.Field {
	return zap.String("txn", txnMeta.DebugString())
}
