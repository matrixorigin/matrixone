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
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"os"
)

// BlockMapper maps file content to blocks with CRC checksum
type BlockMapper[T BlockMappable] struct {
	underlying       T
	blockSize        int
	blockContentSize int
	contentOffset    int64
}

const (
	_ChecksumSize     = 8
	_BlockContentSize = 2048 - _ChecksumSize
)

var (
	crc64Table = crc64.MakeTable(crc64.ECMA)

	ErrChecksumNotMatch = errors.New("checksum not match")
)

type BlockMappable interface {
	io.ReadWriteSeeker
	io.WriterAt
	io.ReaderAt
}

func NewBlockMapper[T BlockMappable](
	underlying T,
	blockContentSize int,
) *BlockMapper[T] {
	return &BlockMapper[T]{
		underlying:       underlying,
		blockSize:        blockContentSize + _ChecksumSize,
		blockContentSize: blockContentSize,
	}
}

var _ BlockMappable = new(BlockMapper[*os.File])

func (b *BlockMapper[T]) ReadAt(buf []byte, offset int64) (n int, err error) {
	for len(buf) > 0 {
		blockOffset, offsetInBlock := b.contentOffsetToBlockOffset(offset)
		var data []byte
		data, err = b.readBlock(blockOffset)
		if err != nil && err != io.EOF {
			// read error
			return
		}
		data = data[offsetInBlock:]
		nBytes := copy(buf, data)
		buf = buf[nBytes:]
		if err == io.EOF && nBytes != len(data) {
			// not fully read
			err = nil
		}
		offset += int64(nBytes)
		n += nBytes
		if err == io.EOF && nBytes == 0 {
			// no more data
			break
		}
	}
	return
}

func (b *BlockMapper[T]) Read(buf []byte) (n int, err error) {
	n, err = b.ReadAt(buf, b.contentOffset)
	b.contentOffset += int64(n)
	return
}

func (b *BlockMapper[T]) WriteAt(buf []byte, offset int64) (n int, err error) {
	for len(buf) > 0 {

		blockOffset, offsetInBlock := b.contentOffsetToBlockOffset(offset)
		data, err := b.readBlock(blockOffset)
		if err != nil && err != io.EOF {
			return 0, err
		}

		if len(data[offsetInBlock:]) == 0 {
			nAppend := len(buf)
			if nAppend+len(data) > b.blockContentSize {
				nAppend = b.blockContentSize - len(data)
			}
			data = append(data, make([]byte, nAppend)...)
		}

		nBytes := copy(data[offsetInBlock:], buf)
		buf = buf[nBytes:]

		checksum := crc64.Checksum(data, crc64Table)
		checksumBytes := make([]byte, _ChecksumSize)
		binary.LittleEndian.PutUint64(checksumBytes, checksum)
		if _, err := b.underlying.WriteAt(checksumBytes, blockOffset); err != nil {
			return n, err
		}

		if _, err := b.underlying.WriteAt(data, blockOffset+_ChecksumSize); err != nil {
			return n, err
		}

		n += nBytes
		offset += int64(nBytes)
	}

	return
}

func (b *BlockMapper[T]) Write(buf []byte) (n int, err error) {
	n, err = b.WriteAt(buf, b.contentOffset)
	b.contentOffset += int64(n)
	return
}

func (b *BlockMapper[T]) Seek(offset int64, whence int) (int64, error) {

	fileSize, err := b.underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	contentSize := fileSize
	nBlock := ceilingDiv(contentSize, int64(b.blockSize))
	contentSize -= _ChecksumSize * nBlock

	switch whence {
	case io.SeekStart:
		b.contentOffset = offset
	case io.SeekCurrent:
		b.contentOffset += offset
	case io.SeekEnd:
		b.contentOffset = contentSize + offset
	}

	if b.contentOffset < 0 {
		b.contentOffset = 0
	}
	if b.contentOffset > contentSize {
		b.contentOffset = contentSize
	}

	return b.contentOffset, nil
}

func (b *BlockMapper[T]) contentOffsetToBlockOffset(
	contentOffset int64,
) (
	blockOffset int64,
	offsetInBlock int64,
) {

	nBlock := contentOffset / int64(b.blockContentSize)
	blockOffset += nBlock * int64(b.blockSize)

	offsetInBlock = contentOffset % int64(b.blockContentSize)

	return
}

func (b *BlockMapper[T]) readBlock(offset int64) (data []byte, err error) {

	data = make([]byte, b.blockSize)
	n, err := b.underlying.ReadAt(data, offset)
	data = data[:n]
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n < _ChecksumSize {
		// empty
		return
	}

	checksum := binary.LittleEndian.Uint64(data[:_ChecksumSize])
	data = data[_ChecksumSize:]

	expectedChecksum := crc64.Checksum(data, crc64Table)
	if checksum != expectedChecksum {
		return nil, ErrChecksumNotMatch
	}

	return
}
