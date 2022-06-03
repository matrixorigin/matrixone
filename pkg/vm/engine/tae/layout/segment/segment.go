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

package segment

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/pierrec/lz4"
	"os"
	"sync"
)

const INODE_NUM = 4096
const INODE_SIZE = 512
const BLOCK_SIZE = 4096
const SIZE = 4 * 1024 * 1024 * 1024
const LOG_START = 2 * INODE_SIZE
const DATA_START = LOG_START + INODE_SIZE*INODE_NUM
const DATA_SIZE = SIZE - DATA_START
const LOG_SIZE = DATA_START - LOG_START
const HOLE_SIZE = 512 * INODE_SIZE
const MAGIC = 0xFFFFFFFF

type SuperBlock struct {
	version   uint64
	blockSize uint32
	inodeSize uint32
	colCnt    uint32
	lognode   *Inode
	state     StateType
}

type Segment struct {
	mutex     sync.Mutex
	segFile   *os.File
	lastInode uint64
	super     SuperBlock
	nodes     map[string]*BlockFile
	log       *Log
	allocator Allocator
	name      string
}

func (s *Segment) Init(name string) error {
	var (
		err     error
		sbuffer bytes.Buffer
	)
	s.super = SuperBlock{
		version:   1,
		blockSize: BLOCK_SIZE,
		inodeSize: INODE_SIZE,
	}
	log := &Inode{
		magic: MAGIC,
		inode: 1,
		size:  0,
		state: RESIDENT,
	}
	s.name = name
	s.super.lognode = log
	segmentFile, err := os.Create(name)
	if err != nil {
		return err
	}
	s.segFile = segmentFile
	err = s.segFile.Truncate(DATA_START)

	if err != nil {
		return err
	}
	/*header := make([]byte, 32)
	copy(header, encoding.EncodeUint64(sb.version))*/
	err = binary.Write(&sbuffer, binary.BigEndian, s.super.version)
	if err != nil {
		return err
	}
	if err = binary.Write(&sbuffer, binary.BigEndian, uint8(compress.Lz4)); err != nil {
		return err
	}
	if err = binary.Write(&sbuffer, binary.BigEndian, s.super.blockSize); err != nil {
		return err
	}
	if err = binary.Write(&sbuffer, binary.BigEndian, s.super.colCnt); err != nil {
		return err
	}

	cbufLen := (s.super.blockSize - (uint32(sbuffer.Len()) % s.super.blockSize)) + uint32(sbuffer.Len())

	if cbufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, cbufLen-uint32(sbuffer.Len()))
		if err = binary.Write(&sbuffer, binary.BigEndian, zero); err != nil {
			return err
		}
	}

	if _, err := s.segFile.Write(sbuffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (s *Segment) Open(name string) (err error) {
	s.segFile, err = os.OpenFile(name, os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) Mount() {
	s.lastInode = 1
	var seq uint64
	seq = 0
	s.nodes = make(map[string]*BlockFile, INODE_NUM)
	logFile := &BlockFile{
		snode:   s.super.lognode,
		name:    "logfile",
		segment: s,
	}
	s.log = &Log{}
	s.log.logFile = logFile
	s.log.offset = LOG_START + s.log.logFile.snode.size
	s.log.seq = seq + 1
	s.nodes[logFile.name] = s.log.logFile
	s.allocator = NewBitmapAllocator(DATA_SIZE, s.GetPageSize())
	s.log.allocator = NewBitmapAllocator(LOG_SIZE, s.GetInodeSize())
}

func (s *Segment) Unmount() {
	logutil.Infof("Unmount Segment: %v", s.name)
}

func (s *Segment) Destroy() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.segFile.Close()
	if err != nil {
		panic(any(err.Error()))
	}
	logutil.Infof(" %s | SegmentFile | Destroying", s.name)
	err = os.Remove(s.name)
	if err != nil {
		panic(any(err.Error()))
	}
	s.segFile = nil
}

func (s *Segment) Replay(cache *bytes.Buffer) error {
	s.super = SuperBlock{
		version:   1,
		blockSize: BLOCK_SIZE,
		inodeSize: INODE_SIZE,
	}
	log := &Inode{
		magic: MAGIC,
		inode: 1,
		size:  0,
		state: RESIDENT,
	}
	s.super.lognode = log
	s.Mount()
	err := s.log.Replay(cache)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) NewBlockFile(fname string) *BlockFile {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	file := s.nodes[fname]
	var ino *Inode
	if file == nil {
		ino = &Inode{
			magic:      MAGIC,
			inode:      s.lastInode + 1,
			size:       0,
			extents:    make([]Extent, 0),
			logExtents: Extent{},
			state:      RESIDENT,
			algo:       compress.Lz4,
			seq:        0,
		}
	}
	file = &BlockFile{
		snode:   ino,
		name:    fname,
		segment: s,
	}
	s.nodes[file.name] = file
	s.lastInode += 1
	return file
}

func (s *Segment) Append(fd *BlockFile, pl []byte) (err error) {
	buf := pl
	if fd.snode.algo == compress.Lz4 {
		colSize := len(pl)
		buf = make([]byte, lz4.CompressBlockBound(colSize))
		if buf, err = compress.Compress(pl, buf, compress.Lz4); err != nil {
			return err
		}
	}
	offset, allocated := s.allocator.Allocate(uint64(len(buf)))
	if allocated == 0 {
		//panic(any("no space"))
		panic(any("no space"))
	}
	err = fd.Append(DATA_START+offset, buf, uint32(len(pl)))
	if err != nil {
		return err
	}
	err = s.log.Append(fd)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) Update(fd *BlockFile, pl []byte, fOffset uint64) error {
	offset, _ := s.allocator.Allocate(uint64(len(pl)))
	free, err := fd.Update(DATA_START+offset, pl, uint32(fOffset))
	if err != nil {
		return err
	}
	for _, ext := range free {
		s.allocator.Free(ext.offset-DATA_START, ext.length)
	}
	err = s.log.Append(fd)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) ReleaseFile(fd *BlockFile) {
	if s.segFile == nil {
		return
	}
	err := s.log.RemoveInode(fd)
	if err != nil {
		panic(any(err.Error()))
	}
	s.mutex.Lock()
	delete(s.nodes, fd.name)
	s.mutex.Unlock()
	s.Free(fd)
	fd = nil
}

func (s *Segment) Free(fd *BlockFile) {
	fd.snode.mutex.Lock()
	defer fd.snode.mutex.Unlock()
	for _, ext := range fd.snode.extents {
		s.allocator.Free(ext.offset-DATA_START, ext.length)
	}
	fd.snode.extents = []Extent{}
}

func (s *Segment) GetPageSize() uint32 {
	return s.super.blockSize
}

func (s *Segment) GetInodeSize() uint32 {
	return s.super.inodeSize
}

func (s *Segment) Sync() error {
	return s.segFile.Sync()
}

func (s *Segment) GetName() string {
	return s.name
}

func (s *Segment) GetNodes() map[string]*BlockFile {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.nodes
}
