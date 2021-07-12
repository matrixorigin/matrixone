package aoe

import (
	"github.com/matrixorigin/matrixcube/storage/stats"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe"
	store "matrixone/pkg/vm/engine/aoe/storage"
	adb "matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
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

func (s *Storage) Write(tableName string, bat *batch.Batch, index *md.LogIndex) error  {
	size := 0
	for _, vec := range bat.Vecs{
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	return s.db.Append(tableName, bat, index)
}

func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.db.GetSnapshot(ctx)
}

func (s *Storage) CreateTable(info *aoe.TabletInfo, index *md.LogIndex) (id uint64, err error) {
	return s.db.CreateTable(info)
}




// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
// returns the current bytes in [start,end), and the founded key
func SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error){
	panic("not implemented")
}

// CreateSnapshot create a snapshot file under the giving path
func CreateSnapshot(path string, start, end []byte) error {
	panic("not implemented")
}

// ApplySnapshot apply a snapshot file from giving path
func ApplySnapshot(path string) error {
	panic("not implemented")
}