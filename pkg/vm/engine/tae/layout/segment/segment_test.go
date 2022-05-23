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
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "LAYOUT"
)

func mockData(size uint32) []byte {
	var sbuffer bytes.Buffer
	err := binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", size)))
	if err != nil {
		return nil
	}
	ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
		err = binary.Write(&sbuffer, binary.BigEndian, zero)
		if err != nil {
			return nil
		}
	}
	return sbuffer.Bytes()
}

/*func debugBitmap(b *BitmapAllocator) (info string) {
	if len(b.level1) < 20 {
		// log allocator
		return ""
	}
	info = fmt.Sprintf("level0-")
	for i := 0; i < 200; i++ {
		info = fmt.Sprintf("%v-%x\n", info, b.level0[i])
	}
	info = fmt.Sprintf("%v-level1-", info)
	for i := 0; i < 5; i++ {
		info = fmt.Sprintf("%v-%x\n", info, b.level1[i])
	}
	return info
}*/

func TestBitmapAllocator_Allocate(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "init.seg")
	seg := Segment{}
	err := seg.Init(name)
	assert.Nil(t, err)
	seg.Mount()
	file := seg.NewBlockFile("bitmap")
	file.snode.algo = compress.None
	level0 := seg.allocator.(*BitmapAllocator).level0
	level1 := seg.allocator.(*BitmapAllocator).level1
	for i := 0; i < 20; i++ {
		buffer1 := mockData(1048576)
		assert.NotNil(t, buffer1)
		err = file.segment.Append(file, buffer1)
		assert.Nil(t, err)
		buffer2 := mockData(4096)
		assert.NotNil(t, buffer2)
		err = file.segment.Append(file, buffer2)
		assert.Nil(t, err)
		buffer3 := mockData(5242880)
		assert.NotNil(t, buffer3)
		err = file.segment.Append(file, buffer3)
		assert.Nil(t, err)
	}
	l0pos := uint32(file.snode.originSize) / seg.GetPageSize() / BITS_PER_UNIT
	l1pos := l0pos / BITS_PER_UNITSET

	assert.Equal(t, ALL_UNIT_CLEAR, int(level0[l0pos-1]))
	ret := 0xFFFFFFFFFFF00000 - level0[l0pos]
	assert.Equal(t, 0, int(ret))
	ret = 0xF000000000000000 - level1[l1pos]
	assert.Equal(t, 0, int(ret))

	seg.allocator.Free(8192, 4096)
	//ret = uint64(0x4) - level0[0]
	assert.Equal(t, 4, int(level0[0]))
	//fmt.Printf(debugBitmap(seg.allocator.(*BitmapAllocator)))
}

func TestBitmapAllocator_Free(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "free.seg")
	seg := Segment{}
	err := seg.Init(name)
	assert.Nil(t, err)
	seg.Mount()
	file := seg.NewBlockFile("bitmap")
	file.snode.algo = compress.None
	level0 := seg.allocator.(*BitmapAllocator).level0
	level1 := seg.allocator.(*BitmapAllocator).level1
	buffer1 := mockData(2048000)
	assert.NotNil(t, buffer1)
	err = file.segment.Append(file, buffer1)
	assert.Nil(t, err)
	buffer2 := mockData(49152)
	assert.NotNil(t, buffer2)
	err = file.segment.Append(file, buffer2)
	assert.Nil(t, err)
	buffer3 := mockData(8192)
	assert.NotNil(t, buffer3)
	err = file.segment.Append(file, buffer3)
	assert.Nil(t, err)
	buffer4 := mockData(5242880)
	assert.NotNil(t, buffer4)
	err = file.segment.Append(file, buffer4)
	assert.Nil(t, err)
	l0pos := uint32(file.snode.originSize) / seg.GetPageSize() / BITS_PER_UNIT
	l1pos := l0pos / BITS_PER_UNITSET

	assert.Equal(t, ALL_UNIT_CLEAR, int(level0[l0pos-1]))
	ret := 0xFFFFFFFFFFFFFFFC - level0[l0pos]
	assert.Equal(t, 0, int(ret))
	ret = 0xFFFFFFFFFFFFFFF8 - level1[l1pos]
	assert.Equal(t, 0, int(ret))

	l0pos = 2048000 / seg.GetPageSize() / BITS_PER_UNIT
	seg.allocator.Free(2048000, 49152)
	ret = 0xFFF0000000000000 - level0[l0pos]
	//ret = uint64(0x4) - level0[0]
	assert.Equal(t, 0, int(ret))
	ret = 0xFFFFFFFFFFFFFFF9 - level1[l1pos]
	assert.Equal(t, 0, int(ret))
	seg.allocator.Free(2101248, 4096)
	assert.Equal(t, 2, int(level0[l0pos+1]))
	ret = 0xFFFFFFFFFFFFFFFB - level1[l1pos]
	assert.Equal(t, 0, int(ret))
	buffer5 := mockData(53248)
	assert.NotNil(t, buffer5)
	err = file.segment.Append(file, buffer5)
	assert.Nil(t, err)
	extents := *file.GetExtents()
	offset := extents[len(extents)-1].offset
	size := 2048000 + 8192 + 49152 + 5242880
	assert.Equal(t, size, int(offset-DATA_START))
	buffer6 := mockData(49152)
	assert.NotNil(t, buffer6)
	err = file.segment.Append(file, buffer6)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(level0[l0pos]))
	ret = 0xFFFFFFFFFFFFFFFA - level1[l1pos]
	assert.Equal(t, 0, int(ret))
	buffer7 := mockData(8192)
	assert.NotNil(t, buffer7)
	err = file.segment.Append(file, buffer7)
	assert.Nil(t, err)
	buffer8 := mockData(4096)
	assert.NotNil(t, buffer8)
	err = file.segment.Append(file, buffer8)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(level0[l0pos+1]))
	ret = 0xFFFFFFFFFFFFFFF8 - level1[l1pos]
	assert.Equal(t, 0, int(ret))
	extents = *file.GetExtents()
	offset6 := extents[len(extents)-3].offset
	assert.Equal(t, 2048000, int(offset6-DATA_START))
	offset7 := extents[len(extents)-2].offset
	assert.Equal(t, size+53248, int(offset7-DATA_START))
	offset8 := extents[len(extents)-1].offset
	assert.Equal(t, 2101248, int(offset8-DATA_START))
	assert.Equal(t, 4096, int(extents[len(extents)-1].length))
	//fmt.Printf(debugBitmap(seg.allocator.(*BitmapAllocator)))
}

func TestBlockFile_GetExtents(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "free.seg")
	seg := Segment{}
	err := seg.Init(name)
	assert.Nil(t, err)
	seg.Mount()
	file := seg.NewBlockFile("bitmap")
	file.snode.algo = compress.None
	for i := 0; i < 16; i++ {
		buffer1 := mockData(8388608)
		assert.NotNil(t, buffer1)
		err = file.segment.Append(file, buffer1)
		assert.Nil(t, err)
	}
	for i := 0; i < 10; i++ {
		buffer2 := mockData(4096)
		assert.NotNil(t, buffer2)
		err = file.segment.Append(file, buffer2)
		assert.Nil(t, err)
	}
	buffer3 := mockData(2097152)
	assert.NotNil(t, buffer3)
	err = file.segment.Append(file, buffer3)
	assert.Nil(t, err)

	level0 := seg.allocator.(*BitmapAllocator).level0
	level1 := seg.allocator.(*BitmapAllocator).level1
	l0pos := uint32(file.snode.originSize) / seg.GetPageSize() / BITS_PER_UNIT
	l1pos := l0pos / BITS_PER_UNITSET

	assert.Equal(t, ALL_UNIT_CLEAR, int(level0[l0pos-1]))
	ret := 0xFFFFFFFFFFFFFC00 - level0[l0pos]
	assert.Equal(t, 0, int(ret))
	ret = 0xFFFFFFFFFFFFFFFE - level1[l1pos]
	assert.Equal(t, 0, int(ret))
	extents := *file.GetExtents()
	size := uint32(0)
	for i, extent := range extents {
		size += extent.length
		if i == len(extents)-1 {
			break
		}
		assert.Equal(t, extents[i+1].offset, extent.offset+extent.length)
	}

	assert.Equal(t, size, uint32(file.GetFileSize()))

}

func TestSegment_Replay(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "init.seg")
	seg := Segment{}
	err := seg.Init(name)
	assert.Nil(t, err)
	seg.Mount()
	var file *BlockFile
	for i := 0; i < 10; i++ {
		file = seg.NewBlockFile(fmt.Sprintf("test_%d.blk", i))
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", i)))
		assert.Nil(t, err)
	}
	for _, file = range seg.nodes {
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 10)))
		assert.Nil(t, err)
		buffer1 := mockData(2048000)
		assert.NotNil(t, buffer1)
		err = file.segment.Append(file, buffer1)
		assert.Nil(t, err)
		buffer2 := mockData(49152)
		assert.NotNil(t, buffer2)
		err = file.segment.Append(file, buffer2)
		assert.Nil(t, err)
		buffer3 := mockData(8192)
		assert.NotNil(t, buffer3)
		err = file.segment.Append(file, buffer3)
		assert.Nil(t, err)
		buffer4 := mockData(5242880)
		assert.NotNil(t, buffer4)
		err = file.segment.Append(file, buffer4)
		assert.Nil(t, err)
	}
	level0 := seg.allocator.(*BitmapAllocator).level0
	level1 := seg.allocator.(*BitmapAllocator).level1
	segfile, err := os.OpenFile(name, os.O_RDWR, os.ModePerm)
	assert.Nil(t, err)
	seg1 := Segment{
		name:    name,
		segFile: segfile,
	}
	cache := bytes.NewBuffer(make([]byte, LOG_SIZE))
	err = seg1.Replay(cache)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(seg1.nodes))
	level0_2 := seg1.allocator.(*BitmapAllocator).level0
	level1_2 := seg1.allocator.(*BitmapAllocator).level1
	for i := range level0_2 {
		assert.Equal(t, level0[i], level0_2[i])
	}
	for i := range level1_2 {
		assert.Equal(t, level1[i], level1_2[i])
	}

}

func TestSegment_Init(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "init.seg")
	seg := Segment{}
	err := seg.Init(name)
	assert.Nil(t, err)
	seg.Mount()
	file := seg.NewBlockFile("test")
	/*seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	seg.Update(file, []byte(fmt.Sprintf("this is tests %d", 517)), 4096)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 518)))*/
	for i := 0; i < 3; i++ {
		var sbuffer bytes.Buffer
		err := binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", 515)))
		assert.Nil(t, err)
		var size uint32 = 262144
		ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
		if ibufLen > uint32(sbuffer.Len()) {
			zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
			err = binary.Write(&sbuffer, binary.BigEndian, zero)
			assert.Nil(t, err)
		}
		err = seg.Append(file, sbuffer.Bytes())
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
		assert.Nil(t, err)
	}
	var sbuffer bytes.Buffer
	err = binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", 515)))
	assert.Nil(t, err)
	var size uint32 = 262144
	ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
		err = binary.Write(&sbuffer, binary.BigEndian, zero)
		assert.Nil(t, err)
	}
	err = seg.Update(file, sbuffer.Bytes(), 16384)
	assert.Nil(t, err)
	b := bytes.NewBuffer(make([]byte, 1<<20))
	_, err = file.ReadExtent(0, uint32(file.snode.size), b.Bytes())
	assert.Nil(t, err)
	buf := b.Bytes()
	buf = buf[16384 : 16384+17]
	logutil.Infof("%v", string(buf))
	//seg.Update(file, []byte(fmt.Sprintf("this is tests %d", 517)), 8192)
	//seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	//seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	/*seg.Free(file, 1)
	seg.Free(file, 40)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))*/
}
