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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe"
	store "matrixone/pkg/vm/engine/aoe/storage"
	adb "matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

// Storage memory storage
type Storage struct {
	DB    *adb.DB
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
	db, err := adb.Open(dir, opts)
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
func (s *Storage) Append(tabletName string, bat *batch.Batch, logIdx uint64, logOffset, logSize int) error {
	size := 0
	for _, vec := range bat.Vecs {
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(bat.Vecs[0].Length()))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	return s.DB.Append(dbi.AppendCtx{
		OpIndex:   logIdx,
		OpOffset:  logOffset,
		OpSize:    logSize,
		TableName: tabletName,
		Data:      bat,
	})
}

//Relation  returns a relation of the db and the table
func (s *Storage) Relation(tabletName string) (*adb.Relation, error) {
	return s.DB.Relation(tabletName)
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

//GetSegmentedId returns the smallest segmente id among the tables starts with prefix
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
func (s *Storage) TableIDs() (ids []uint64, err error) {
	return s.DB.TableIDs()
}

//TableIDs returns the names of all the tables in the storage.
func (s *Storage) TableNames() (ids []string) {
	return s.DB.TableNames()
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
