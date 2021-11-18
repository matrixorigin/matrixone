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

package metadata

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

var (
	AddressNotFoundErr = errors.New("address not found")
)

type SSWriter interface {
	PrepareWrite() error
	CommitWrite() error
}

type SSLoader interface {
	Preprocess(Processor) error
	PrepareLoad() error
	CommitLoad() error
	GetIndex() uint64
	GetShardId() uint64
	Addresses() *Addresses
}

type Snapshoter interface {
	SSWriter
	SSLoader
}

func MakeMetaSnapshotName(shardId, index uint64) string {
	return fmt.Sprintf("%d-%d-%d.meta", shardId, index, time.Now().UTC().UnixMicro())
}

type Addresses struct {
	Database map[uint64]uint64
	Table    map[uint64]uint64
	Segment  map[uint64]uint64
	Block    map[uint64]uint64
	ShardId  map[uint64]uint64
}

func (m *Addresses) GetBlkAddr(addr *common.ID) (*common.ID, error) {
	var ok bool
	naddr := new(common.ID)
	naddr.TableID, ok = m.Table[addr.TableID]
	if !ok {
		return nil, AddressNotFoundErr
	}
	naddr.SegmentID, ok = m.Segment[addr.SegmentID]
	if !ok {
		return nil, AddressNotFoundErr
	}
	naddr.BlockID, ok = m.Segment[addr.BlockID]
	if !ok {
		return nil, AddressNotFoundErr
	}
	return naddr, nil
}

func (m *Addresses) GetSegAddr(addr *common.ID) (*common.ID, error) {
	var ok bool
	naddr := new(common.ID)
	naddr.TableID, ok = m.Table[addr.TableID]
	if !ok {
		return nil, AddressNotFoundErr
	}
	naddr.SegmentID, ok = m.Segment[addr.SegmentID]
	if !ok {
		return nil, AddressNotFoundErr
	}
	return naddr, nil
}

func (m *Addresses) GetTableAddr(addr uint64) (uint64, error) {
	tid, ok := m.Table[addr]
	if !ok {
		return 0, AddressNotFoundErr
	}
	return tid, nil
}

func (m *Addresses) GetDBAddr(addr uint64) (uint64, error) {
	id, ok := m.Table[addr]
	if !ok {
		return 0, AddressNotFoundErr
	}
	return id, nil
}

func (m *Addresses) RewriteSegmentFile(filename string) (string, error) {
	arr := strings.Split(strings.TrimSuffix(filename, ".seg"), "_")
	tableId, err := strconv.Atoi(arr[0])
	if err != nil {
		return "", err
	}
	arr[0] = strconv.Itoa(int(m.Table[uint64(tableId)]))
	segId, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", err
	}
	arr[1] = strconv.Itoa(int(m.Segment[uint64(segId)]))
	return arr[0] + "_" + arr[1] + ".seg", nil
}

func (m *Addresses) RewriteBlockFile(filename string) (string, error) {
	arr := strings.Split(strings.TrimSuffix(filename, ".blk"), "_")
	tableId, err := strconv.Atoi(arr[0])
	if err != nil {
		return "", err
	}
	arr[0] = strconv.Itoa(int(m.Table[uint64(tableId)]))
	segId, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", err
	}
	arr[1] = strconv.Itoa(int(m.Segment[uint64(segId)]))
	blkId, err := strconv.Atoi(arr[2])
	if err != nil {
		return "", err
	}
	arr[2] = strconv.Itoa(int(m.Block[uint64(blkId)]))
	return arr[0] + "_" + arr[1] + "_" + arr[2] + ".blk", nil
}

type dbSnapshoter struct {
	dir       string
	name      string
	view      *databaseLogEntry
	index     uint64
	tranId    uint64
	db        *Database
	catalog   *Catalog
	addresses *Addresses
}

func NewDBSSWriter(db *Database, dir string, index uint64) *dbSnapshoter {
	ss := &dbSnapshoter{
		db:    db,
		dir:   dir,
		index: index,
	}
	return ss
}

func NewDBSSLoader(catalog *Catalog, name string) *dbSnapshoter {
	ss := &dbSnapshoter{
		catalog: catalog,
		name:    name,
	}
	return ss
}

func (ss *dbSnapshoter) PrepareWrite() error {
	ss.view = ss.db.View(ss.index)
	ss.view.Id = ss.db.Id
	return nil
}

func (ss *dbSnapshoter) CommitWrite() error {
	ss.name = filepath.Join(ss.dir, MakeMetaSnapshotName(ss.db.GetShardId(), ss.index))
	f, err := os.Create(ss.name)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ss.view.Marshal()
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	logutil.Infof("%s | Shard SS | Flushed", f.Name())
	return err
}

func (ss *dbSnapshoter) ReAllocId(allocator *Sequence, view *Database) error {
	ss.tranId = allocator.NextUncommitId()
	processor := newReAllocIdProcessor(allocator, ss.tranId)
	ss.addresses = processor.trace
	processor.OnDatabase(view)
	err := view.RecurLoopLocked(processor)
	if err != nil {
		return err
	}
	tableSet := make(map[uint64]*Table)
	for _, table := range ss.view.Database.TableSet {
		tableSet[table.Id] = table
	}
	ss.view.Database.TableSet = tableSet
	return err
}

func (ss *dbSnapshoter) PrepareLoad() error {
	f, err := os.OpenFile(ss.name, os.O_RDONLY, 666)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	mnode := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(mnode)
	ss.view = &databaseLogEntry{}
	if _, err = f.Read(mnode.Buf[:size]); err != nil {
		return err
	}
	if err = ss.view.Unmarshal(mnode.Buf[:size]); err != nil {
		return err
	}
	ss.view.Database.Catalog = ss.catalog
	if err = ss.ReAllocId(&ss.catalog.Sequence, ss.view.Database); err != nil {
		return err
	}
	if err = ss.view.Database.rebuild(true, false); err != nil {
		return err
	}
	ss.view.Database.InitWal(ss.view.LogRange.Range.Right)
	// for src, dest := range ss.addresses.Segment {
	// 	logutil.Infof("map segment %d------->%d", src, dest)
	// }
	// for src, dest := range ss.addresses.Block {
	// 	logutil.Infof("map block %d------->%d", src, dest)
	// }
	return nil
}

func (ss *dbSnapshoter) Preprocess(processor Processor) error {
	return ss.view.Database.RecurLoopLocked(processor)
}

func (ss *dbSnapshoter) GetIndex() uint64 {
	return ss.view.LogRange.Range.Right
}

func (ss *dbSnapshoter) GetShardId() uint64 {
	return ss.view.LogRange.ShardId
}

func (ss *dbSnapshoter) CommitLoad() error {
	return ss.catalog.SimpleReplaceDatabase(ss.view, ss.tranId)
}

func (ss *dbSnapshoter) View() *databaseLogEntry {
	return ss.view
}

func (ss *dbSnapshoter) Addresses() *Addresses {
	return ss.addresses
}
