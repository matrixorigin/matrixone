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

package mockio

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	return mockFS.OpenFile(name, id)
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
}

func (sf *segmentFile) Replay(colCnt int, indexCnt map[int]int, cache *bytes.Buffer) error {
	panic(any("implement me"))
}

func newSegmentFile(name string, id uint64) *segmentFile {
	sf := &segmentFile{
		blocks: make(map[uint64]*blockFile),
		name:   name,
	}
	sf.id = &common.ID{
		SegmentID: id,
	}
	sf.Ref()
	sf.OnZeroCB = sf.close
	return sf
}

func (sf *segmentFile) Fingerprint() *common.ID { return sf.id }
func (sf *segmentFile) Close() error            { return nil }

func (sf *segmentFile) close() {
	sf.Destroy()
}
func (sf *segmentFile) Destroy() {
	// for _, block := range sf.blocks {
	// 	block.Unref()
	// }

	logutil.Infof("Destroying Segment %d", sf.id.SegmentID)
	_ = mockFS.RemoveFile(sf.id.SegmentID)
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

func (sf *segmentFile) Name() string { return sf.name }
func (sf *segmentFile) RemoveBlock(id uint64) {
	sf.Lock()
	defer sf.Unlock()
	delete(sf.blocks, id)
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
