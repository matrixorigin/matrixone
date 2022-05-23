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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"unsafe"
)

type Log struct {
	logFile   *BlockFile
	seq       uint64
	offset    uint64
	allocator Allocator
}

func (l *Log) readInode(cache *bytes.Buffer, file *BlockFile) (n int, err error) {
	var nameLen uint32
	var extentLen uint64
	n = 0
	if err = binary.Read(cache, binary.BigEndian, &file.snode.magic); err != nil {
		return
	}
	if file.snode.magic != MAGIC {
		return 0, nil
	}
	n += int(unsafe.Sizeof(file.snode.magic))
	if err = binary.Read(cache, binary.BigEndian, &file.snode.inode); err != nil {
		return
	}
	n += int(unsafe.Sizeof(file.snode.inode))
	if err = binary.Read(cache, binary.BigEndian, &nameLen); err != nil {
		return
	}
	n += int(unsafe.Sizeof(nameLen))
	name := make([]byte, nameLen)
	if err = binary.Read(cache, binary.BigEndian, name); err != nil {
		return
	}
	n += len(name)
	file.name = string(name)
	if err = binary.Read(cache, binary.BigEndian, &file.snode.seq); err != nil {
		return
	}
	n += int(unsafe.Sizeof(file.snode.seq))
	if err = binary.Read(cache, binary.BigEndian, &file.snode.algo); err != nil {
		return
	}
	n += int(unsafe.Sizeof(file.snode.algo))
	if err = binary.Read(cache, binary.BigEndian, &file.snode.state); err != nil {
		return
	}
	n += int(unsafe.Sizeof(file.snode.state))
	if err = binary.Read(cache, binary.BigEndian, &file.snode.size); err != nil {
		return
	}
	n += int(unsafe.Sizeof(file.snode.size))
	if err = binary.Read(cache, binary.BigEndian, &extentLen); err != nil {
		return
	}
	n += int(unsafe.Sizeof(extentLen))
	file.snode.extents = make([]Extent, extentLen)
	for i := 0; i < int(extentLen); i++ {
		if err = binary.Read(cache, binary.BigEndian, &file.snode.extents[i].typ); err != nil {
			return
		}
		n += int(unsafe.Sizeof(file.snode.extents[i].typ))
		if err = binary.Read(cache, binary.BigEndian, &file.snode.extents[i].offset); err != nil {
			return
		}
		n += int(unsafe.Sizeof(file.snode.extents[i].offset))
		if err = binary.Read(cache, binary.BigEndian, &file.snode.extents[i].length); err != nil {
			return
		}
		n += int(unsafe.Sizeof(file.snode.extents[i].length))
		if err = binary.Read(cache, binary.BigEndian, &file.snode.extents[i].data.offset); err != nil {
			return
		}
		n += int(unsafe.Sizeof(file.snode.extents[i].data.offset))
		if err = binary.Read(cache, binary.BigEndian, &file.snode.extents[i].data.length); err != nil {
			return
		}
		n += int(unsafe.Sizeof(file.snode.extents[i].data.length))
	}
	return
}

func (l *Log) Replay(cache *bytes.Buffer) error {
	n, err := l.logFile.segment.segFile.ReadAt(cache.Bytes(), LOG_START)
	if err != nil {
		return err
	}
	if n != cache.Len() {
		panic(any("Replay read error"))
	}
	l.logFile.segment.mutex.Lock()
	defer l.logFile.segment.mutex.Unlock()
	l.logFile.segment.lastInode = 1
	l.logFile.name = "logfile"
	l.logFile.segment.nodes[l.logFile.name] = l.logFile
	magicLen := uint32(unsafe.Sizeof(l.logFile.snode.magic))
	for {
		file := &BlockFile{
			snode:   &Inode{},
			segment: l.logFile.segment,
		}
		n, err = l.readInode(cache, file)
		if err != nil {
			return err
		}
		if n == 0 {
			if int(l.logFile.segment.super.blockSize-magicLen) == cache.Len() {
				break
			}
			cache = bytes.NewBuffer(cache.Bytes()[l.logFile.segment.super.blockSize-magicLen:])
			continue
		}
		seekLen := l.logFile.segment.super.blockSize - (uint32(n) % l.logFile.segment.super.blockSize)
		if int(seekLen) == cache.Len() {
			break
		}
		cache = bytes.NewBuffer(cache.Bytes()[seekLen:])
		block := l.logFile.segment.nodes[file.name]
		if (block == nil || block.snode.seq < file.snode.seq) &&
			file.snode.state == RESIDENT {
			extents := file.GetExtents()
			for _, extent := range *extents {
				l.logFile.segment.allocator.CheckAllocations(
					extent.offset-DATA_START, extent.length)
			}
			l.logFile.segment.nodes[file.name] = file
		}
		if block == nil {
			l.logFile.segment.lastInode++
		} else {
			logutil.Infof("block: %v seq: %d is overwritten", block.name, block.snode.seq)
		}
	}
	return nil
}

func (l *Log) RemoveInode(file *BlockFile) error {
	file.snode.state = REMOVE
	err := l.Append(file)
	if err != nil {
		return err
	}
	l.allocator.Free(file.snode.logExtents.offset, file.snode.logExtents.length)
	return nil
}

func (l *Log) Append(file *BlockFile) error {
	var (
		err     error
		ibuffer bytes.Buffer
	)
	segment := l.logFile.segment
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.magic); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.inode); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, uint32(len([]byte(file.name)))); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, []byte(file.name)); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.seq); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.algo); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.state); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, file.snode.size); err != nil {
		return err
	}
	if err = binary.Write(&ibuffer, binary.BigEndian, uint64(len(file.snode.extents))); err != nil {
		return err
	}
	file.snode.mutex.RLock()
	extents := file.snode.extents
	file.snode.mutex.RUnlock()
	for _, ext := range extents {
		if err = binary.Write(&ibuffer, binary.BigEndian, ext.typ); err != nil {
			return err
		}
		if err = binary.Write(&ibuffer, binary.BigEndian, ext.offset); err != nil {
			return err
		}
		if err = binary.Write(&ibuffer, binary.BigEndian, ext.length); err != nil {
			return err
		}
		if err = binary.Write(&ibuffer, binary.BigEndian, ext.data.offset); err != nil {
			return err
		}
		if err = binary.Write(&ibuffer, binary.BigEndian, ext.data.length); err != nil {
			return err
		}
	}
	ibufLen := (segment.super.blockSize - (uint32(ibuffer.Len()) % segment.super.blockSize)) + uint32(ibuffer.Len())
	offset, allocated := l.allocator.Allocate(uint64(ibufLen))
	if n, err := segment.segFile.WriteAt(ibuffer.Bytes(), int64(offset+LOG_START)); err != nil || n != ibuffer.Len() {
		return err
	}
	l.allocator.Free(file.snode.logExtents.offset, file.snode.logExtents.length)
	file.snode.logExtents.offset = uint32(offset)
	file.snode.logExtents.length = uint32(allocated)
	return nil
}
