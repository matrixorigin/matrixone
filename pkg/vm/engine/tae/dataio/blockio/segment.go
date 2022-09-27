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

package blockio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

type ObjectFactory struct {
	Fs *objectio.ObjectFS
}

func NewObjectFactory(dirname string) file.SegmentFactory {
	serviceDir := path.Join(dirname, "data")
	service := objectio.TmpNewFileservice(path.Join(dirname, "data"))
	SegmentFactory := &ObjectFactory{
		Fs: objectio.NewObjectFS(service, serviceDir),
	}
	return SegmentFactory
}

func (factory *ObjectFactory) EncodeName(id uint64) string {
	return ""
}

func (factory *ObjectFactory) DecodeName(name string) (uint64, error) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, moerr.NewInternalError("blockio: segment name is illegal")
	}
	id, err := DecodeSegName(name)
	if err != nil {
		return 0, err
	}
	return id.SegmentID, err
}

func (factory *ObjectFactory) Build(dir string, id, tid uint64, fs *objectio.ObjectFS) file.Segment {
	return openSegment(dir, id, tid, fs)
}

type segmentFile struct {
	sync.RWMutex
	common.RefHelper
	id     *common.ID
	blocks map[uint64]*blockFile
	name   string
	fs     *objectio.ObjectFS
}

func (sf *segmentFile) GetFs() *objectio.ObjectFS {
	return sf.fs
}

func openSegment(name string, id, tid uint64, fs *objectio.ObjectFS) *segmentFile {
	sf := &segmentFile{
		blocks: make(map[uint64]*blockFile),
		name:   name,
	}
	sf.fs = fs
	sf.id = &common.ID{
		TableID:   tid,
		SegmentID: id,
	}
	sf.Ref()
	sf.OnZeroCB = sf.close
	return sf
}

func (sf *segmentFile) Name() string { return sf.name }

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
	name := path.Join(sf.fs.Dir, EncodeSegName(sf.id))
	os.Remove(name)
	sf.fs = nil
}

func (sf *segmentFile) OpenBlock(id uint64, colCnt int) (block file.Block, err error) {
	sf.Lock()
	defer sf.Unlock()
	bf := sf.blocks[id]
	if bf == nil {
		bf = newBlock(id, sf, colCnt)
		sf.blocks[id] = bf
	}
	block = bf
	return
}

func (sf *segmentFile) String() string {
	s := fmt.Sprintf("SegmentFile[%d][\"%s\"][BCnt=%d]", sf.id, sf.name, len(sf.blocks))
	return s
}

func (sf *segmentFile) Sync() error {
	return nil
}
