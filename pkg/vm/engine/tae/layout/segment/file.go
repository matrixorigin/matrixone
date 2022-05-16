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
	"io"
)

type BlockFile struct {
	snode   *Inode
	name    string
	segment *Segment
}

func (b *BlockFile) GetSegement() *Segment {
	return b.segment
}

func (b *BlockFile) GetInode() uint64 {
	return b.snode.inode
}

func (b *BlockFile) GetFileSize() int64 {
	b.snode.mutex.Lock()
	var size int64 = 0
	for _, ext := range b.snode.extents {
		size += int64(ext.GetData().GetLength())
	}
	b.snode.mutex.Unlock()
	return size
}

func (b *BlockFile) GetName() string {
	return b.name
}

func (b *BlockFile) Append(offset uint64, data []byte) error {
	cbufLen := uint32(p2roundup(uint64(len(data)), uint64(b.segment.super.blockSize)))
	_, err := b.segment.segFile.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}
	b.snode.mutex.Lock()
	b.snode.extents = append(b.snode.extents, Extent{
		typ:    APPEND,
		offset: uint32(offset),
		length: cbufLen,
		data:   entry{offset: 0, length: uint32(len(data))},
	})
	/*if len(b.snode.extents) > 0 &&
		b.snode.extents[len(b.snode.extents)-1].End() == uint32(offset) {
		b.snode.extents[len(b.snode.extents)-1].data = append(b.snode.extents[len(b.snode.extents)-1].data, entry{
			offset: b.snode.extents[len(b.snode.extents)-1].length,
			length: uint32(len(data)),
		})
		b.snode.extents[len(b.snode.extents)-1].length += cbufLen
	} else {
		rel := make([]entry, 1)
		rel[0] = entry{offset: 0, length: uint32(len(data))}
		b.snode.extents = append(b.snode.extents, Extent{
			typ:    APPEND,
			offset: uint32(offset),
			length: cbufLen,
			data:   rel,
		})
	}*/
	b.snode.mutex.Unlock()
	b.snode.size += uint64(cbufLen)
	return nil
}

func extentsInsert(extents *[]Extent, idx int, vals []Extent) {
	rear := make([]Extent, 0)
	rear = append(rear, (*extents)[idx:]...)
	front := append((*extents)[:idx], vals...)
	*extents = append(front, rear...)
}
func extentsRemove(extents *[]Extent, vals []Extent) {
	for _, ext := range vals {
		for i, e := range *extents {
			if e.offset == ext.offset &&
				e.length == ext.length {
				*extents = append((*extents)[:i], (*extents)[i+1:]...)
			}
		}
	}
}

func (b *BlockFile) repairExtent(offset, fOffset, length uint32) []Extent {
	num := 0
	b.snode.mutex.Lock()
	defer b.snode.mutex.Unlock()
	for _, extent := range b.snode.extents {
		if fOffset >= extent.length {
			fOffset -= extent.length
		} else {
			break
		}
		num++
	}
	free := make([]Extent, 0)
	remove := make([]Extent, 0)
	ext := b.snode.extents[num]
	var remaining uint32 = 0
	if ext.length > fOffset+length {
		remaining = ext.length - fOffset - length
	}
	oldOff := b.snode.extents[num].offset
	if fOffset == 0 && ext.length-fOffset-length == 0 {
		b.snode.extents[num].typ = UPDATE
		b.snode.extents[num].offset = offset
		free = append(free, Extent{
			offset: oldOff,
			length: length,
		})
		return free
	}
	vals := make([]Extent, 1)
	vals[0].offset = offset
	vals[0].length = length
	vals[0].typ = UPDATE
	if remaining > 0 {
		b.snode.extents[num].length = fOffset
		vals = append(vals, Extent{
			offset: ext.End() - remaining,
			length: remaining,
		})
	}
	freeLength := length
	idx := num
	for {
		if freeLength == 0 || idx == len(b.snode.extents) {
			break
		}
		e := &b.snode.extents[idx]
		if idx == num {
			b.snode.extents[num].length = fOffset
			xLen := ext.length - fOffset
			if xLen > freeLength {
				xLen = freeLength
			}
			free = append(free, Extent{
				offset: e.offset + fOffset,
				length: xLen,
			})
			freeLength -= xLen
			if freeLength == 0 {
				break
			}
			idx++
			continue
		}
		xLen := e.length
		if xLen > freeLength {
			xLen = freeLength
			free = append(free, Extent{
				offset: e.offset,
				length: xLen,
			})
			e.offset += xLen
			e.length -= xLen
		} else {
			free = append(free, Extent{
				offset: e.offset,
				length: xLen,
			})
			remove = append(remove, b.snode.extents[idx])
			//b.snode.extents = append(b.snode.extents[:idx], b.snode.extents[idx+1:]...)
		}

		freeLength -= xLen
		idx++
	}
	if len(remove) > 0 {
		extentsRemove(&b.snode.extents, remove)
	}
	extentsInsert(&b.snode.extents, num+1, vals)
	return free
}

func (b *BlockFile) Update(offset uint64, data []byte, fOffset uint32) ([]Extent, error) {
	var (
		err     error
		sbuffer bytes.Buffer
	)
	if err = binary.Write(&sbuffer, binary.BigEndian, data); err != nil {
		return nil, err
	}
	cbufLen := uint32(p2roundup(uint64(sbuffer.Len()), uint64(b.segment.super.blockSize)))
	//if cbufLen > uint32(sbuffer.Len()) {
	//zero := make([]byte, cbufLen-uint32(sbuffer.Len()))
	//binary.Write(&sbuffer, binary.BigEndian, zero)
	//}
	_, err = b.segment.segFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		return nil, err
	}
	logutil.Infof("extents is %d", len(b.snode.extents))
	return b.repairExtent(uint32(offset), fOffset, cbufLen), nil
}

func (b *BlockFile) GetExtents() *[]Extent {
	extents := &b.snode.extents
	return extents
}

func (b *BlockFile) Read(data []byte) (n int, err error) {
	bufLen := len(data)
	if bufLen == 0 {
		return 0, nil
	}
	b.snode.mutex.Lock()
	defer b.snode.mutex.Unlock()
	n = 0
	var boff uint32 = 0
	var roff uint32 = 0
	for _, ext := range b.snode.extents {
		if bufLen == 0 {
			break
		}
		c := data[boff : boff+ext.GetData().GetLength()]
		dataLen, err := b.ReadExtent(roff, ext.GetData().GetLength(), c)
		if err != nil && dataLen != ext.GetData().GetLength() {
			return int(dataLen), err
		}
		n += int(dataLen)
		//entries := ext.GetData()
		/*for _, entry := range entries {
			if bufLen < int(entry.GetLength()) {
				copy(data, c[entry.GetOffset():entry.GetOffset()+uint32(bufLen)])
				bufLen = 0
				break
			}
			bufLen -= int(entry.GetLength())
			copy(data[n:], c[entry.GetOffset():entry.GetOffset()+entry.GetLength()])
			n += int(entry.GetLength())
		}*/
		boff += ext.GetData().GetLength()
		roff += ext.Length()
	}
	return n, nil
}

func (b *BlockFile) ReadExtent(offset, length uint32, data []byte) (uint32, error) {
	remain := uint32(b.snode.size) - offset - length
	num := 0
	for _, extent := range b.snode.extents {
		if offset >= extent.length {
			offset -= extent.length
		} else {
			break
		}
		num++
	}
	var read uint32 = 0
	for {
		buf := data
		readOne := length
		if offset > 0 {
			if b.snode.extents[num].length-offset < length {
				readOne = b.snode.extents[num].length - offset
			}
			offset = 0
		} else if b.snode.extents[num].length < length {
			readOne = b.snode.extents[num].length
		}
		buf = buf[read : read+readOne]
		_, err := b.segment.segFile.ReadAt(buf, int64(b.snode.extents[num].offset)+int64(offset))
		if err != nil && err != io.EOF {
			return 0, err
		}
		read += readOne
		length -= readOne
		num++
		if length == 0 || length == remain {
			return read, nil
		}
	}
}
