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
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

// createSnapshot creates a snapshot of the specified shard, and stores it at
// the given path. It returns the raft log index of the snapshot and any error
// if exists. The detailed procedure is as follows:
//
// 1. Get a persisted view of shard
//	 1.1. Use `GetShardCheckpointId` to get the persistent raft log index
//	 1.2. Get the persisted view of shard using `SSWriter.PrepareWrite`
// 2. Collect related segment / block files, and pin them from being removed
//   2.1. Call `checkAndPin` with the view above, if nothing wrong comes up, go
//        to the next stage.
//   2.2. If returned `files` is nil, that means some inconsistency between
//        our previous view and the current underlying files has been there.
//        But that's not a frequent event, so we just retry for a maximum
//        times(10 by default), if failed still, report that to upper level.
//        Each time we fetch a newer raft log index to increase our success
//        rate.
// 3. Write metadata of snapshot to the given path
//	 3.1. Call `SSWriter.CommitWrite` to commit and persist metadata
// **In fact, the next 2 steps are done earlier due to the wrong behaviour of Ref**
// 4. Pin and collect related files
//	 4.1. For all the table entries, get the `TableData`
//	 4.2. Collect all segment/block files within those tables via FsManager and ref them
// 5. Hard-link all files to the given path
//	 5.1. Use `Link` syscall to create the link
//   5.2. Remember to Unref the files
func (d *DB) createSnapshot(dbName string, path string) (idx uint64, err error) {
	// Preparations for snapshotting
	if err := d.Closed.Load(); err != nil {
		return 0, errors.New("aoe already closed")
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return
	}
	if _, err := os.Stat(path); err != nil {
		if err = os.MkdirAll(path, os.FileMode(0755)); err != nil {
			return 0, err
		}
	}

	shardId := database.GetShardId()

	// Get the PersistentLogIndex for the shard
	retId := d.GetShardCheckpointId(shardId)
	// Get the view for shard on PersistentLogIndex
	ssWriter := metadata.NewDBSSWriter(database, path, retId)
	if err := ssWriter.PrepareWrite(); err != nil {
		return 0, err
	}

	// Collect related files on disk, and pin them from being deleted
	files, err := d.checkAndPin(path, ssWriter.View().Database.TableSet)
	if err != nil {
		return 0, err
	}
	retry := 0
	for files == nil {
		retry++
		if retry == MaxRetryCreateSnapshot {
			return 0, errors.New("failed to create snapshot, retry later")
		}
		retId = d.GetShardCheckpointId(shardId)
		logutil.Infof("retry create snapshot on log index: %d", retId)
		ssWriter = metadata.NewDBSSWriter(database, path, retId)
		if err = ssWriter.PrepareWrite(); err != nil {
			return 0, err
		}

		files, err = d.checkAndPin(path, ssWriter.View().Database.TableSet)
		if err != nil {
			return 0, err
		}
	}

	// Write the view (shard metadata) to the given path
	if err = ssWriter.CommitWrite(); err != nil {
		return 0, err
	}

	// Currently, the reference of segment/block files doesn't work
	// as we expected, we would refactor here to hard-link all files
	// together after ref them later. But now we simply hard-link them
	// one by one in a for-loop instead of referencing.

	// Hard-link all files to the given path

	//for _, file := range files {
	//	oldName := file.Stat().Name()
	//	newName := filepath.Join(path, filepath.Base(oldName))
	//	if err := os.Link(oldName, newName); err != nil {
	//		return 0, err
	//	}
	//	file.Unref()
	//}

	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	if err = f.Sync(); err != nil {
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}

	return retId, nil
}

func (d *DB) checkAndPin(path string, tables map[uint64]*metadata.Table) ([]base.IBaseFile, error) {
	files := make([]base.IBaseFile, 0)
	for _, tbl := range tables {
		data, err := d.Store.DataTables.WeakRefTable(tbl.Id)
		if err != nil {
			return nil, err
		}

		// Notice: there could probably be such a scenario: in snapshot metadata, we got an
		// unsorted segment a.k.a. several blocks, but the underlying file has been already
		// upgraded to a sorted segment file. Only when segment upgrade happens between the
		// snapshot's view and the current view could this case comes up. We resolve the case
		// by simply checking the consistency of metadata and data files before pinning files,
		// and if check fails, just retry. It should work because: 1) upgrading is a really
		// low-frequency operation compared to others like append. 2) each time we get a new
		// persistent log index for snapshotting, and the time spent on pinning is little.
		// 3) snapshotting is also a low-frequency operation, sometimes never happens.
		for _, seg := range tbl.SegmentSet {
			s := data.StrongRefSegment(seg.Id)
			if s == nil {
				return nil, errors.New(fmt.Sprintf("segment %d not exists", seg.Id))
			}
			sf := s.GetSegmentFile()

			if segFile, ok := sf.(*dataio.SortedSegmentFile); ok {
				if !seg.IsSortedLocked() {
					for _, file := range files {
						file.Unref()
					}
					s.Unref()
					infos, err := ioutil.ReadDir(path)
					if err != nil {
						return nil, err
					}
					for _, info := range infos {
						if err = os.Remove(filepath.Join(path, info.Name())); err != nil {
							return nil, err
						}
					}
					return nil, nil
				}
				segFile.Ref()
				oldName := segFile.Stat().Name()
				newName := filepath.Join(path, filepath.Base(oldName))
				if err = os.Link(oldName, newName); err != nil {
					return nil, err
				}
				files = append(files, segFile)
			} else if segFile, ok := sf.(*dataio.UnsortedSegmentFile); ok {
				segFile.RLock()
				for _, file := range segFile.Blocks {
					bid := file.(*dataio.BlockFile).ID.BlockID
					flag := false
					for _, blk := range seg.BlockSet {
						if blk.Id == bid {
							flag = true
							break
						}
					}
					if !flag {
						continue
					}
					file.Ref()
					oldName := file.Stat().Name()
					newName := filepath.Join(path, filepath.Base(oldName))
					if err := os.Link(oldName, newName); err != nil {
						return nil, err
					}
					files = append(files, file)
				}
				segFile.RUnlock()
			} else {
				return nil, errors.New("unexpected error: not a segment file")
			}
			s.Unref()
		}
	}
	return files, nil
}

func (d *DB) applySnapshot(dbName string, path string) error {
	if err := d.Closed.Load(); err != nil {
		return errors.New("aoe already closed")
	}

	catalog := d.Store.Catalog

	database, err := catalog.SimpleGetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	shardId := database.GetShardId()
	var ssmeta string
	var segfiles []string
	var blkfiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".meta") {
			ssmeta = filepath.Join(path, file.Name())
		} else if strings.HasSuffix(file.Name(), ".seg") {
			segfiles = append(segfiles, filepath.Join(path, file.Name()))
		} else if strings.HasSuffix(file.Name(), ".blk") {
			blkfiles = append(blkfiles, filepath.Join(path, file.Name()))
		}
	}
	if len(ssmeta) == 0 {
		return errors.New("metadata of snapshot not found")
	}
	arr := strings.Split(filepath.Base(ssmeta), "-")
	if len(arr) != 3 {
		return errors.New("invalid metadata file name")
	}
	if shard, err := strconv.Atoi(arr[0]); err != nil || uint64(shard) != shardId {
		return errors.New("shardId mismatch with the local snapshot meta")
	}

	ssReader := metadata.NewDBSSLoader(catalog, ssmeta)
	if err = ssReader.PrepareLoad(); err != nil {
		return err
	}

	// Link files
	mapping := ssReader.Mapping()
	for _, segFile := range segfiles {
		oldName := segFile
		rewritten, err := mapping.RewriteSegmentFile(filepath.Base(oldName))
		if err != nil {
			return err
		}
		newName := filepath.Join(filepath.Join(d.Dir, "data"), rewritten)
		if err = os.Link(oldName, newName); err != nil {
			return err
		}
		//logutil.Infof("old: %s => new: %s", oldName, newName)
	}
	for _, blkFile := range blkfiles {
		oldName := blkFile
		rewritten, err := mapping.RewriteBlockFile(filepath.Base(oldName))
		if err != nil {
			return err
		}
		newName := filepath.Join(filepath.Join(d.Dir, "data"), rewritten)
		if err = os.Link(oldName, newName); err != nil {
			return err
		}
		//logutil.Infof("old: %s => new: %s", oldName, newName)
	}

	tbls := ssReader.View().Database.TableSet
	data := d.Store.DataTables
	for _, tbl := range tbls {
		// tbl.Catalog = d.Store.Catalog
		// tbl.IdIndex = make(map[uint64]int)
		tb, err := data.RegisterTable(tbl)
		if err != nil {
			return err
		}
		for _, seg := range tbl.SegmentSet {
			// tbl.IdIndex[seg.Id] = i
			// seg.Table = tbl
			// seg.Catalog = d.Store.Catalog
			// seg.IdIndex = make(map[uint64]int)
			sg, err := tb.RegisterSegment(seg)
			if err != nil {
				return err
			}
			for _, blk := range seg.BlockSet {
				// seg.IdIndex[blk.Id] = i
				// blk.Segment = seg
				// blk.IndiceMemo = metadata.NewIndiceMemo(blk)
				if _, err = sg.RegisterBlock(blk); err != nil {
					return err
				}
			}
		}
	}

	// check if there are segments which could be upgraded
	unsortedSegs := make([]*metadata.Segment, 0)
	for _, tbl := range tbls {
		for _, seg := range tbl.SegmentSet {
			if seg.IsSortedLocked() {
				continue
			}
			unsortedSegs = append(unsortedSegs, seg)
		}
	}

	// TODO: fill the logic
	if err = ssReader.CommitLoad(); err != nil {
		return err
	}
	return nil
}
