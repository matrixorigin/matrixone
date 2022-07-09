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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
)

type DriverFile struct {
	common.RefHelper
	snode  *Inode
	name   string
	driver *Driver
}

func (b *DriverFile) GetSegement() *Driver {
	return b.driver
}

func (b *DriverFile) GetInode() *Inode {
	b.snode.mutex.RLock()
	defer b.snode.mutex.RUnlock()
	return b.snode
}

func (b *DriverFile) SetRows(rows uint32) {
	b.snode.rows = rows
}

func (b *DriverFile) SetCols(cols uint32) {
	b.snode.cols = cols
}

func (b *DriverFile) SetIdxs(idxs uint32) {
	b.snode.idxs = idxs
}

func (b *DriverFile) GetName() string {
	return b.name
}

func (b *DriverFile) Append(offset uint64, data []byte, originSize uint32) (err error) {
	cbufLen := uint32(p2roundup(uint64(len(data)), uint64(b.driver.super.blockSize)))
	_, err = b.driver.segFile.WriteAt(data, int64(offset))
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
	b.snode.size += uint64(len(data))
	b.snode.originSize += uint64(originSize)
	b.snode.seq++
	b.snode.mutex.Unlock()
	return nil
}

func (b *DriverFile) GetExtents() *[]Extent {
	extents := &b.snode.extents
	return extents
}

func (b *DriverFile) Read(data []byte) (n int, err error) {
	bufLen := len(data)
	if bufLen == 0 {
		return 0, nil
	}
	n = 0
	var boff uint32 = 0
	var roff uint32 = 0
	b.snode.mutex.RLock()
	extents := b.snode.extents
	b.snode.mutex.RUnlock()
	for _, ext := range extents {
		if bufLen == 0 {
			break
		}
		buf := data[boff : boff+ext.GetData().GetLength()]
		dataLen, err := b.ReadExtent(roff, ext.GetData().GetLength(), buf)
		if err != nil && dataLen != ext.GetData().GetLength() {
			return int(dataLen), err
		}
		n += int(dataLen)
		boff += ext.GetData().GetLength()
		roff += ext.Length()
	}
	return n, nil
}

func (b *DriverFile) ReadExtent(offset, length uint32, data []byte) (uint32, error) {
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
		_, err := b.driver.segFile.ReadAt(buf, int64(b.snode.extents[num].offset)+int64(offset))
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

func (b *DriverFile) close() {
	b.Destroy()
}

func (b *DriverFile) Destroy() {
	b.driver.ReleaseFile(b)
}
