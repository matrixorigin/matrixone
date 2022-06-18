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

package segmentio

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

var SegmentFactory file.SegmentFactory

func init() {
	SegmentFactory = new(segmentFactory)
}

type segmentFactory struct{}

func (factory *segmentFactory) Build(dir string, id uint64) file.Segment {
	baseName := factory.EncodeName(id)
	name := path.Join(dir, baseName)
	return openSegment(name, id)
}

func (factory *segmentFactory) EncodeName(id uint64) string {
	return fmt.Sprintf("%d.seg", id)
}

func (factory *segmentFactory) DecodeName(name string) (id uint64, err error) {
	trimmed := strings.TrimSuffix(name, ".seg")
	if trimmed == name {
		err = fmt.Errorf("%w: %s", file.ErrInvalidName, name)
		return
	}
	id, err = strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		err = fmt.Errorf("%w: %s", file.ErrInvalidName, name)
	}
	return
}

type segmentFile struct {
	sync.RWMutex
	common.RefHelper
	id     *common.ID
	ts     uint64
	blocks map[uint64]*blockFile
	name   string
	driver *Driver
}

func openSegment(name string, id uint64) *segmentFile {
	sf := &segmentFile{
		blocks: make(map[uint64]*blockFile),
		name:   name,
	}
	sf.driver = &Driver{}
	err := sf.driver.Open(sf.name)
	if err != nil {
		panic(any(err.Error()))
	}
	sf.driver.Mount()
	sf.id = &common.ID{
		SegmentID: id,
	}
	sf.Ref()
	sf.OnZeroCB = sf.close
	return sf
}

func (sf *segmentFile) Name() string { return sf.name }

func (sf *segmentFile) RemoveBlock(id uint64) {
	sf.Lock()
	defer sf.Unlock()
	block := sf.blocks[id]
	if block == nil {
		return
	}
	delete(sf.blocks, id)
}

func (sf *segmentFile) replayInfo(stat *fileStat, file *DriverFile) {
	meta := file.GetInode()
	stat.size = meta.GetFileSize()
	stat.originSize = meta.GetOriginSize()
	stat.algo = meta.GetAlgo()
	stat.name = file.GetName()
}

func (sf *segmentFile) getFileTs(name string) (ts uint64,err error) {
	tmpName := strings.Split(name, ".")
	fileName := strings.Split(tmpName[0], "_")
	if len(fileName) > 2 {
		ts, err = strconv.ParseUint(fileName[2], 10, 64)
		if err != nil {
			return 0,err
		}
	}
	return ts, nil
}

func (sf *segmentFile) Replay(colCnt int, indexCnt map[int]int, cache *bytes.Buffer) error {
	err := sf.driver.Replay(cache)
	if err != nil {
		return err
	}
	nodes := sf.driver.GetNodes()
	sf.Lock()
	defer sf.Unlock()
	for name, file := range nodes {
		tmpName := strings.Split(name, ".")
		fileName := strings.Split(tmpName[0], "_")
		if len(fileName) < 2 {
			continue
		}
		id, err := strconv.ParseUint(fileName[1], 10, 32)
		if err != nil {
			return err
		}
		bf := sf.blocks[id]
		if bf == nil {
			bf = replayBlock(id, sf, colCnt, indexCnt)
			sf.blocks[id] = bf
		}
		col, err := strconv.ParseUint(fileName[0], 10, 32)
		if err != nil {
			return err
		}
		var ts uint64 = 0
		if len(fileName) > 2 {
			ts, err = strconv.ParseUint(fileName[2], 10, 64)
			if err != nil {
				return err
			}
		}
		switch tmpName[1] {
		case "blk":
			if ts == 0 {
				bf.columns[col].ts = 0
				bf.columns[col].data.file[0] = file
				sf.replayInfo(bf.columns[col].data.stat, file)
				break
			}
			if bf.columns[col].ts < ts {
				bf.columns[col].ts = ts
				bf.columns[col].data.file[0] = file
				sf.replayInfo(bf.columns[col].data.stat, file)
			}
			if bf.ts <= ts {
				bf.ts = ts
				bf.rows = file.GetInode().GetRows()
			}
		case "update":
			if bf.ts <= ts {
				bf.ts = ts
			}
			if bf.columns[col].updates.file[0] == nil {
				bf.columns[col].updates.file[0] = file
				sf.replayInfo(bf.columns[col].updates.stat, file)
				break
			}
			updateTs, err := sf.getFileTs(bf.columns[col].updates.file[0].name)
			if err != nil {
				return err
			}
			if updateTs < ts {
				bf.columns[col].ts = ts
				bf.columns[col].updates.file[0] = file
				sf.replayInfo(bf.columns[col].updates.stat, file)
			}
		case "del":
			if bf.ts <= ts {
				bf.ts = ts
			}
			if bf.deletes.file[0] == nil {
				bf.deletes.file[0] = file
				sf.replayInfo(bf.deletes.stat, file)
				break
			}
			delTs, err := sf.getFileTs(bf.deletes.file[0].name)
			if err != nil {
				return err
			}
			if delTs < ts {
				bf.deletes.file[0] = file
				sf.replayInfo(bf.deletes.stat, file)
			}
		case "idx":
			if ts == 0 && len(fileName) < 3 {
				bf.indexMeta.file[0] = file
				sf.replayInfo(bf.indexMeta.stat, file)
				break
			}
			bf.columns[col].indexes[ts].dataFile.file[0] = file
			sf.replayInfo(bf.columns[col].indexes[ts].dataFile.stat, file)
		default:
			panic(any("No Support"))
		}

	}
	return nil
}

func (sf *segmentFile) Fingerprint() *common.ID { return sf.id }
func (sf *segmentFile) Close() error            { return nil }

func (sf *segmentFile) close() {
	sf.Destroy()
}
func (sf *segmentFile) Destroy() {
	logutil.Infof("Destroying Driver %d", sf.id.SegmentID)
	sf.RLock()
	blocks := sf.blocks
	sf.RUnlock()
	for _, block := range blocks {
		if err := block.Destroy(); err != nil {
			panic(err)
		}
	}
	sf.driver.Unmount()
	sf.driver.Destroy()
}

func (sf *segmentFile) OpenBlock(id uint64, colCnt int, indexCnt map[int]int) (block file.Block, err error) {
	sf.Lock()
	defer sf.Unlock()
	bf := sf.blocks[id]
	if bf == nil {
		bf = newBlock(id, sf, colCnt, indexCnt)
		sf.blocks[id] = bf
	}
	block = bf
	return
}

func (sf *segmentFile) WriteTS(ts uint64) error {
	sf.ts = ts
	return nil
}

func (sf *segmentFile) ReadTS() uint64 {
	return sf.ts
}

func (sf *segmentFile) String() string {
	s := fmt.Sprintf("SegmentFile[%d][\"%s\"][TS=%d][BCnt=%d]", sf.id, sf.name, sf.ts, len(sf.blocks))
	return s
}

func (sf *segmentFile) GetSegmentFile() *Driver {
	return sf.driver
}

func (sf *segmentFile) Sync() error {
	return sf.driver.Sync()
}
