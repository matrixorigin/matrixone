// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aoe

import (
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	store "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"

	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

// Storage memory storage
type Storage struct {
	DB    *aoedb.DB
	stats stats.Stats
}

func (s *Storage) Sync() error {
	//TODO: implement me
	return nil
}

func (s *Storage) RemoveShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error {
	//TODO: implement me
	return nil
}

// NewStorage returns pebble kv store on a default options
func NewStorage(dir string) (*Storage, error) {
	return NewStorageWithOptions(dir, &store.Options{})
}

// NewStorageWithOptions returns badger kv store
func NewStorageWithOptions(dir string, opts *store.Options) (*Storage, error) {
	db, err := aoedb.OpenWithWalBroker(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Storage{
		DB: db,
	}, nil
}

//Stats returns the stats of the Storage
func (s *Storage) Stats() stats.Stats {
	return s.stats
}

//Append appends batch in the table
func (s *Storage) Append(tabletName string, bat *batch.Batch, shardId uint64, logIdx uint64, logOffset, logSize int) error {
	size := 0
	for _, vec := range bat.Vecs {
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(bat.Vecs[0].Length()))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	err := s.DB.Append(dbi.AppendCtx{
		ShardId:   shardId,
		OpIndex:   logIdx,
		OpOffset:  logOffset,
		OpSize:    logSize,
		TableName: tabletName,
		Data:      bat,
	})
	return err
}

//Relation  returns a relation of the db and the table
func (s *Storage) Relation(shardId uint64, tabletName string) (*aoedb.Relation, error) {
	return s.DB.Relation(shardId, tabletName)
}

//GetSnapshot gets the snapshot from the table.
//If there's no segment, it returns an empty snapshot.
func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.DB.GetSnapshot(ctx)
}

//GetSegmentIds returns the ids of segments of the table
func (s *Storage) GetSegmentIds(ctx dbi.GetSegmentsCtx) (ids dbi.IDS) {
	return s.DB.GetSegmentIds(ctx)
}

//GetShardPesistedId returns the smallest segmente id among the tables starts with prefix
func (s *Storage) GetShardPesistedId(shardId uint64) uint64 {
	return s.DB.GetShardCheckpointId(shardId)
}

//CreateTable creates a table in the storage.
//It returns the id of the created table.
//If the storage is closed, it panics.
func (s *Storage) CreateTable(info *aoe.TableInfo, ctx dbi.TableOpCtx) (uint64, error) {
	return s.DB.CreateTable(info, ctx)
}

//DropTable drops the table in the storage.
//If the storage is closed, it panics.
func (s *Storage) DropTable(ctx dbi.DropTableCtx) (uint64, error) {
	return s.DB.DropTable(ctx)
}

//TableIDs returns the ids of all the tables in the storage.
func (s *Storage) TableIDs(shardId uint64) (ids []uint64, err error) {
	return s.DB.TableIDs(shardId)
}

//TableIDs returns the names of all the tables in the storage.
func (s *Storage) TableNames(shardId uint64) (ids []string) {
	return s.DB.TableNames(shardId)
}

//TODO
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	return 0, 0, nil, err

}

//TODO
func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	if _, err := os.Stat(path); err != nil {
		os.MkdirAll(path, os.FileMode(0755))
	}
	return nil
}

//TODO
func (s *Storage) ApplySnapshot(path string) error {
	return nil
}

//Close closes the storage.
func (s *Storage) Close() error {
	return s.DB.Close()
}
