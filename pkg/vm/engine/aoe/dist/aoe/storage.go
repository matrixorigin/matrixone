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
	"os"
	"sync/atomic"
)

// Storage memory storage
type Storage struct {
	DB    *adb.DB
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
		DB: db,
	}, nil
}

func (s *Storage) Stats() stats.Stats {
	return s.stats
}

func (s *Storage) Append(tabletName string, bat *batch.Batch, index *md.LogIndex) error {
	size := 0
	for _, vec := range bat.Vecs {
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(bat.Vecs[0].Length()))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	return s.DB.Append(dbi.AppendCtx{
		OpIndex:   index.ID,
		TableName: tabletName,
		Data:      bat,
	})
}

func (s *Storage) Relation(tabletName string) (*adb.Relation, error) {
	return s.DB.Relation(tabletName)
}

func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.DB.GetSnapshot(ctx)
}

func (s *Storage) GetSegmentIds(ctx dbi.GetSegmentsCtx) (ids adb.IDS) {
	return s.DB.GetSegmentIds(ctx)
}

func (s *Storage) GetSegmentedId(prefix string) (index uint64, err error) {
	return s.DB.GetSegmentedId(dbi.GetSegmentedIdCtx{
		Matchers: []*dbi.StringMatcher{
			{
				Type:    dbi.MTPrefix,
				Pattern: prefix,
			},
		},
	})
}

func (s *Storage) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (uint64, error) {
	return s.DB.CreateTable(info, ctx)
}

func (s *Storage) DropTable(ctx dbi.DropTableCtx) (uint64, error) {
	return s.DB.DropTable(ctx)
}

func (s *Storage) TableIDs() (ids []uint64, err error) {
	return s.DB.TableIDs()
}

func (s *Storage) TableNames() (ids []string) {
	return s.DB.TableNames()
}

// RemovedShardData remove shard data
func (s *Storage) RemovedShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error {
	return nil
}

func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	return 0, 0, nil, err

}

func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	if _, err := os.Stat(path); err != nil {
		os.MkdirAll(path, os.ModeDir)
	}
	return nil
}

func (s *Storage) ApplySnapshot(path string) error {
	return nil
}

func (s *Storage) Stop() error {
	return s.DB.Close()
}
