// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

// FileWithChecksum maps file contents to blocks with checksum
type FileWithChecksum[T FileLike] struct {
	ctx              context.Context
	underlying       T
	blockSize        int
	blockContentSize int
	contentOffset    int64
	perfCounterSets  []*perfcounter.CounterSet
}

const (
	_ChecksumSize     = crc32.Size
	_DefaultBlockSize = 2048
	_BlockContentSize = _DefaultBlockSize - _ChecksumSize
	_BlockSize        = _BlockContentSize + _ChecksumSize
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

func NewFileWithChecksum[T FileLike](
	ctx context.Context,
	underlying T,
	blockContentSize int,
	perfCounterSets []*perfcounter.CounterSet,
) *FileWithChecksum[T] {
	return &FileWithChecksum[T]{
		ctx:              ctx,
		underlying:       underlying,
		blockSize:        blockContentSize + _ChecksumSize,
		blockContentSize: blockContentSize,
		perfCounterSets:  perfCounterSets,
	}
}

func NewFileWithChecksumOSFile(
	ctx context.Context,
	underlying *os.File,
	blockContentSize int,
	perfCounterSets []*perfcounter.CounterSet,
) (*FileWithChecksum[*os.File], PutBack[*FileWithChecksum[*os.File]]) {
	var f *FileWithChecksum[*os.File]
	put := fileWithChecksumPoolOSFile.Get(&f)
	f.ctx = ctx
	f.underlying = underlying
	f.blockSize = blockContentSize + _ChecksumSize
	f.blockContentSize = blockContentSize
	f.perfCounterSets = perfCounterSets
	return f, put
}

var fileWithChecksumPoolOSFile = NewPool(
	1024,
	func() *FileWithChecksum[*os.File] {
		return new(FileWithChecksum[*os.File])
	},
	func(f *FileWithChecksum[*os.File]) {
		*f = emptyFileWithChecksumOSFile
	},
	nil,
)

var emptyFileWithChecksumOSFile FileWithChecksum[*os.File]

var _ FileLike = new(FileWithChecksum[*os.File])

func (f *FileWithChecksum[T]) ReadAt(buf []byte, offset int64) (n int, err error) {
	defer func() {
		perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
			c.FileService.FileWithChecksum.Read.Add(int64(n))
		}, f.perfCounterSets...)
	}()

	for len(buf) > 0 {

		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)
		var data []byte
		var putback PutBack[[]byte]
		data, putback, err = f.readBlock(blockOffset)
		if err != nil && err != io.EOF {
			// read error
			putback.Put()
			return
		}

		data = data[offsetInBlock:]
		nBytes := copy(buf, data)
		buf = buf[nBytes:]
		if err == io.EOF && nBytes != len(data) {
			// not fully read
			err = nil
		}
		putback.Put()

		offset += int64(nBytes)
		n += nBytes
		if err == io.EOF && nBytes == 0 {
			// no more data
			break
		}

	}
	return
}

func (f *FileWithChecksum[T]) Read(buf []byte) (n int, err error) {
	n, err = f.ReadAt(buf, f.contentOffset)
	f.contentOffset += int64(n)
	return
}

func (f *FileWithChecksum[T]) WriteAt(buf []byte, offset int64) (n int, err error) {
	defer func() {
		perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
			c.FileService.FileWithChecksum.Write.Add(int64(n))
		}, f.perfCounterSets...)
	}()

	for len(buf) > 0 {

		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)
		data, putback, err := f.readBlock(blockOffset)
		if err != nil && err != io.EOF {
			putback.Put()
			return 0, err
		}

		// extend data
		if len(data[offsetInBlock:]) == 0 {
			nAppend := len(buf)
			if nAppend+len(data) > f.blockContentSize {
				nAppend = f.blockContentSize - len(data)
			}
			data = append(data, make([]byte, nAppend)...)
		}

		// copy to data
		nBytes := copy(data[offsetInBlock:], buf)
		buf = buf[nBytes:]

		checksum := crc32.Checksum(data, crcTable)
		checksumBytes := make([]byte, _ChecksumSize)
		binary.LittleEndian.PutUint32(checksumBytes, checksum)
		if n, err := f.underlying.WriteAt(checksumBytes, blockOffset); err != nil {
			putback.Put()
			return n, err
		} else {
			perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
				c.FileService.FileWithChecksum.UnderlyingWrite.Add(int64(n))
			}, f.perfCounterSets...)
		}

		if n, err := f.underlying.WriteAt(data, blockOffset+_ChecksumSize); err != nil {
			putback.Put()
			return n, err
		} else {
			perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
				c.FileService.FileWithChecksum.UnderlyingWrite.Add(int64(n))
			}, f.perfCounterSets...)
		}

		putback.Put()

		n += nBytes
		offset += int64(nBytes)
	}

	return
}

func (f *FileWithChecksum[T]) Write(buf []byte) (n int, err error) {
	n, err = f.WriteAt(buf, f.contentOffset)
	f.contentOffset += int64(n)
	return
}

func (f *FileWithChecksum[T]) Seek(offset int64, whence int) (int64, error) {

	fileSize, err := f.underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	nBlock := ceilingDiv(fileSize, int64(f.blockSize))
	contentSize := fileSize - _ChecksumSize*nBlock

	switch whence {
	case io.SeekStart:
		f.contentOffset = offset
	case io.SeekCurrent:
		f.contentOffset += offset
	case io.SeekEnd:
		f.contentOffset = contentSize + offset
	}

	if f.contentOffset < 0 {
		f.contentOffset = 0
	}
	if f.contentOffset > contentSize {
		f.contentOffset = contentSize
	}

	return f.contentOffset, nil
}

func (f *FileWithChecksum[T]) contentOffsetToBlockOffset(
	contentOffset int64,
) (
	blockOffset int64,
	offsetInBlock int64,
) {

	nBlock := contentOffset / int64(f.blockContentSize)
	blockOffset += nBlock * int64(f.blockSize)

	offsetInBlock = contentOffset % int64(f.blockContentSize)

	return
}

func (f *FileWithChecksum[T]) readBlock(offset int64) (data []byte, putback PutBack[[]byte], err error) {

	if f.blockSize == _DefaultBlockSize {
		putback = bytesPoolDefaultBlockSize.Get(&data)
	} else {
		data = make([]byte, f.blockSize)
		// putback does not need ptr, bytesPoolDefaultBlockSize put is a no-op
		putback = PutBack[[]byte]{-1, nil, nil}
	}

	n, err := f.underlying.ReadAt(data, offset)
	data = data[:n]
	if err != nil && err != io.EOF {
		return nil, putback, err
	}

	perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
		c.FileService.FileWithChecksum.UnderlyingRead.Add(int64(n))
	}, f.perfCounterSets...)

	if n < _ChecksumSize {
		// empty
		return
	}

	checksum := binary.LittleEndian.Uint32(data[:_ChecksumSize])
	data = data[_ChecksumSize:]

	expectedChecksum := crc32.Checksum(data, crcTable)
	if checksum != expectedChecksum {
		return nil, putback, moerr.NewInternalErrorNoCtx("checksum not match")
	}

	return
}
