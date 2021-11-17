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

package db

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type SSWriter interface {
	io.Closer
	metadata.SSWriter
	GetIndex() uint64
}

type SSLoader = metadata.SSLoader

type Snapshoter interface {
	SSWriter
	SSLoader
}

type ssWriter struct {
	mwriter metadata.SSWriter
	meta    *metadata.Database
	data    map[uint64]iface.ITableData
	tables  *table.Tables
	dir     string
	index   uint64
}

type ssLoader struct {
	mloader  metadata.SSLoader
	src      string
	database *metadata.Database
	tables   *table.Tables
	index    uint64
}

func NewDBSSWriter(database *metadata.Database, dir string, tables *table.Tables) *ssWriter {
	w := &ssWriter{
		data:   make(map[uint64]iface.ITableData),
		tables: tables,
		dir:    dir,
		meta:   database,
	}
	return w
}

func NewDBSSLoader(database *metadata.Database, tables *table.Tables, src string) *ssLoader {
	ss := &ssLoader{
		database: database,
		src:      src,
		tables:   tables,
	}
	return ss
}

func (ss *ssWriter) Close() error {
	for _, t := range ss.data {
		t.Unref()
	}
	return nil
}

func (ss *ssWriter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == ss.meta {
		data.Ref()
		ss.data[data.GetID()] = data
	}
	return nil
}

func (ss *ssWriter) validatePrepare() error {
	// TODO
	return nil
}

func (ss *ssWriter) GetIndex() uint64 {
	return ss.index
}

func (ss *ssWriter) PrepareWrite() error {
	ss.tables.RLock()
	ss.index = ss.meta.GetCheckpointId()
	err := ss.tables.ForTablesLocked(ss.onTableData)
	ss.tables.RUnlock()
	if err != nil {
		return err
	}
	ss.mwriter = metadata.NewDBSSWriter(ss.meta, ss.dir, ss.index)
	if err = ss.mwriter.PrepareWrite(); err != nil {
		return err
	}
	return ss.validatePrepare()
}

func (ss *ssWriter) CommitWrite() error {
	err := ss.mwriter.CommitWrite()
	if err != nil {
		return err
	}
	for _, t := range ss.data {
		if err = CopyTableFn(t, ss.dir); err != nil {
			break
		}
	}
	return err
}

func (ss *ssLoader) validateMetaLoader() error {
	// if ss.index != ss.mloader.GetIndex() {
	// 	return errors.New(fmt.Sprintf("index mismatch: %d, %d", ss.index, ss.mloader.GetIndex()))
	// }
	if ss.database.GetShardId() != ss.mloader.GetShardId() {
		return errors.New(fmt.Sprintf("shard id mismatch: %d, %d", ss.database.ShardId, ss.mloader.GetShardId()))
	}
	return nil
}

func (ss *ssLoader) prepareData(tblks, blks, segs []string) error {
	var err error
	blkMapFn := ss.mloader.Addresses().GetBlkAddr
	segMapFn := ss.mloader.Addresses().GetSegAddr
	destDir := ss.database.Catalog.Cfg.Dir
	for _, tblk := range tblks {
		if err = CopyTBlockFileToDestDir(tblk, ss.src, destDir, blkMapFn); err != nil {
			if err == metadata.AddressNotFoundErr {
				logutil.Warnf("%s cannot be used", tblk)
				err = nil
				continue
			}
			return err
		}
	}
	for _, blk := range blks {
		if err = CopyBlockFileToDestDir(blk, ss.src, destDir, blkMapFn); err != nil {
			if err == metadata.AddressNotFoundErr {
				logutil.Warnf("%s cannot be used", blk)
				err = nil
				continue
			}
			return err
		}
	}
	for _, seg := range segs {
		if err = CopySegmentFileToDestDir(seg, ss.src, destDir, segMapFn); err != nil {
			if err == metadata.AddressNotFoundErr {
				logutil.Warnf("%s cannot be used", seg)
				err = nil
				continue
			}
			return err
		}
	}

	return err
}

func (ss *ssLoader) PrepareLoad() error {
	metas, tblks, blks, segs, err := ScanMigrationDir(ss.src)
	if err != nil {
		return err
	}
	if len(metas) != 1 {
		return errors.New("invalid meta data to apply")
	}

	ss.mloader = metadata.NewDBSSLoader(ss.database.Catalog, filepath.Join(ss.src, metas[0]))
	err = ss.mloader.PrepareLoad()
	if err != nil {
		return err
	}
	if err = ss.validateMetaLoader(); err != nil {
		return err
	}

	if err = ss.prepareData(tblks, blks, segs); err != nil {
		return err
	}

	return err
}

func (ss *ssLoader) CommitLoad() error {
	return ss.mloader.CommitLoad()
}
