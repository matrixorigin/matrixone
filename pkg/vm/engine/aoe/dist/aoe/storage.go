package aoe

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe"
	store "matrixone/pkg/vm/engine/aoe/storage"
	adb "matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync/atomic"
)

// Storage memory storage
type Storage struct {
	db *adb.DB
	stats stats.Stats
}

// NewStorage returns pebble kv store on a default options
func NewStorage(dir string) (*Storage, error) {
	return NewStorageWithOptions(dir, &store.Options{})
}

// NewStorageWithOptions returns badger kv store
func NewStorageWithOptions(dir string, opts *store.Options) (*Storage, error) {
	db, err := adb.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) Stats() stats.Stats {
	return s.stats
}


func (s *Storage) Append(tabletName string, bat *batch.Batch, index *md.LogIndex) error  {
	size := 0
	for _, vec := range bat.Vecs{
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(bat.Vecs[0].Length()))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	return s.db.Append(tabletName, bat, index)
}

func (s *Storage) Relation(tabletName string) (*adb.Relation, error) {
	return s.db.Relation(tabletName)
}

func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.db.GetSnapshot(ctx)
}

func (s *Storage) CreateTable(info *aoe.TabletInfo, index *md.LogIndex) (uint64, error) {
	return s.db.CreateTable(info)
}

func (s *Storage) DropTable(name string, index *md.LogIndex) (uint64, error) {
	return s.db.DropTable(name)
}

// RemovedShardData remove shard data
func (s *Storage) RemovedShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error {
	panic("implement me")
}

func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	panic("implement me")
}

func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	panic("implement me")
}

func (s *Storage) ApplySnapshot(path string) error {
	panic("implement me")
}