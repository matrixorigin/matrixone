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
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

const BLOCK_SUFFIX = "blk"
const UPDATE_SUFFIX = "update"
const INDEX_SUFFIX = "idx"
const DELETE_SUFFIX = "del"

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
	ts     types.TS
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
	sf.id = &common.ID{
		SegmentID: id,
	}
	err = sf.Replay()
	if err != nil {
		panic(any(err.Error()))
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

func setFile(files *[]*DriverFile, file *DriverFile) {
	if len(*files) > 1 {
		panic(any("driver file err"))
	}
	if len(*files) == 1 {
		(*files)[0].Unref()
		(*files)[0] = file
		return
	}
	*files = append(*files, file)
}

func getBlock(id uint64, seg *segmentFile) *blockFile {
	bf := &blockFile{
		seg:     seg,
		id:      id,
		columns: make([]*columnBlock, 0),
	}
	bf.deletes = newDeletes(bf)
	bf.indexMeta = newIndex(&columnBlock{block: bf}).dataFile
	bf.OnZeroCB = bf.close
	bf.Ref()
	return bf
}

func getColumnBlock(col int, block *blockFile) *columnBlock {
	cb := &columnBlock{
		block:   block,
		indexes: make([]*indexFile, 0),
		col:     col,
	}
	cb.updates = newUpdates(cb)
	cb.data = newData(cb)
	cb.OnZeroCB = cb.close
	cb.Ref()
	return cb
}

func getFileTs(name string) (ts types.TS, err error) {
	tmpName, _ := getFileName(name)
	fileName := strings.Split(tmpName, "_")
	if len(fileName) > 2 {
		//ts, err = strconv.ParseUint(fileName[2], 10, 64)
		//if err != nil {
		//return 0, err
		//}
		ts = types.StringToTS(fileName[2])
		return ts, nil
	}
	return ts, nil
}

func getFileName(str string) (name, suffix string) {
	file := strings.Split(str, ".")
	suffix = file[len(file)-1]
	file = strings.Split(str, fmt.Sprintf(".%s", suffix))
	name = file[0]
	return
}

func (sf *segmentFile) Replay() error {
	nodes := sf.driver.GetNodes()
	sf.Lock()
	defer sf.Unlock()
	for name, file := range nodes {
		fileName, suffix := getFileName(name)
		info := strings.Split(fileName, "_")
		if len(info) < 2 {
			//logfile
			continue
		}
		id, err := strconv.ParseUint(info[1], 10, 32)
		if err != nil {
			return err
		}
		bf := sf.blocks[id]
		if bf == nil {
			bf = getBlock(id, sf)
			sf.blocks[id] = bf
		}
		col, err := strconv.ParseUint(info[0], 10, 32)
		if err != nil {
			return err
		}
		//var ts uint64 = 0
		var ts types.TS
		var idx uint64
		if len(info) > 2 {
			//ts, err = strconv.ParseUint(info[2], 10, 64)
			//if err != nil {
			//	return err
			//}
			if suffix == INDEX_SUFFIX {
				idx, err = strconv.ParseUint(info[2], 10, 64)
				if err != nil {
					return err
				}
			} else {
				ts = types.StringToTS(info[2])
			}
		}
		if file.snode.GetCols() > uint32(len(bf.columns)) && suffix != INDEX_SUFFIX {
			bf.AddColumn(int(file.snode.GetCols()))
		}
		switch suffix {
		case BLOCK_SUFFIX:
			if file.snode.GetIdxs() > uint32(len(bf.columns[col].indexes)) {
				bf.columns[col].AddIndex(int(file.snode.GetIdxs()))
			}
			if len(bf.columns[col].data.file) == 0 || bf.columns[col].ts.Less(ts) {
				bf.columns[col].ts = ts
				setFile(&bf.columns[col].data.file, file)
				sf.replayInfo(bf.columns[col].data.stat, file)
			}
			if bf.ts.LessEq(ts) {
				bf.ts = ts
				bf.rows = file.GetInode().GetRows()
			}
		case UPDATE_SUFFIX:
			if bf.ts.LessEq(ts) {
				bf.ts = ts
			}
			//var updateTs uint64 = 0
			var updateTs types.TS
			if len(bf.columns[col].updates.file) > 0 {
				updateTs, err = getFileTs(bf.columns[col].updates.file[0].name)
				if err != nil {
					return err
				}
			}
			if len(bf.columns[col].updates.file) == 0 || updateTs.Less(ts) {
				setFile(&bf.columns[col].updates.file, file)
				sf.replayInfo(bf.columns[col].updates.stat, file)
			}
		case DELETE_SUFFIX:
			if bf.ts.LessEq(ts) {
				bf.ts = ts
			}
			//var delTs uint64 = 0
			var delTs types.TS
			if len(bf.deletes.file) > 0 {
				delTs, err = getFileTs(bf.deletes.file[0].name)
				if err != nil {
					return err
				}
			}
			if len(bf.deletes.file) == 0 || delTs.Less(ts) {
				setFile(&bf.deletes.file, file)
				sf.replayInfo(bf.deletes.stat, file)
			}
		case INDEX_SUFFIX:
			if idx == 0 && len(info) < 3 {
				setFile(&bf.indexMeta.file, file)
				sf.replayInfo(bf.indexMeta.stat, file)
				break
			}
			if int(col) > len(bf.columns)-1 {
				bf.AddColumn(int(col + 1))
			}
			if file.snode.GetIdxs() > uint32(len(bf.columns[col].indexes)) {
				bf.columns[col].AddIndex(int(file.snode.GetIdxs()))
			}
			setFile(&bf.columns[col].indexes[idx].dataFile.file, file)
			sf.replayInfo(bf.columns[col].indexes[idx].dataFile.stat, file)
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
			panic(any(err))
		}
	}
	sf.driver.Unmount()
	sf.driver.Destroy()
	sf.driver = nil
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

func (sf *segmentFile) WriteTS(ts types.TS) error {
	sf.ts = ts
	return nil
}

func (sf *segmentFile) ReadTS() types.TS {
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
