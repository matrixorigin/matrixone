package metadata

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"

type Wal = wal.ShardAwareWal

// type dbWal struct {
// 	impl Wal
// 	id   uint64
// }

// func (db *dbWal) GetCheckpointId() uint64 {
// 	return db.impl.GetShardCheckpointId(db.id)
// }

// func (db *dbWal) GetCurrSeqNum() uint64 {
// 	return db.impl.GetShardCurrSeqNum(db.id)
// }

// func (db *dbWal) Log(wal.Payload) (*Entry, error)
