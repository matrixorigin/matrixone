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
type BlockMapper struct {
	file             *os.File
	blockSize        int
	blockContentSize int
	contentOffset    int64
}

const (
	_ChecksumSize = 8
)

var (
	crc64Table = crc64.MakeTable(crc64.ECMA)

	ErrChecksumNotMatch = errors.New("checksum not match")
)

func NewBlockMapper(
	file *os.File,
	blockContentSize int,
) *BlockMapper {
	return &BlockMapper{
		file:             file,
		blockSize:        blockContentSize + _ChecksumSize,
		blockContentSize: blockContentSize,
	}
}

var _ io.ReadWriteSeeker = new(BlockMapper)

func (b *BlockMapper) Read(buf []byte) (n int, err error) {
	blockOffset, offsetInBlock := b.contentOffsetToBlockOffset(b.contentOffset)
	data, err := b.readBlock(blockOffset)
	if err != nil {
		return 0, err
	}
	data = data[offsetInBlock:]
	n = copy(buf, data)
	if len(data) < b.blockContentSize && n == len(data) {
		err = io.EOF
	}
	b.contentOffset += int64(n)
	return
}

func (b *BlockMapper) Write(buf []byte) (n int, err error) {
	for len(buf) > 0 {

		blockOffset, offsetInBlock := b.contentOffsetToBlockOffset(b.contentOffset)
		data, err := b.readBlock(blockOffset)
		if err != nil {
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
		if _, err := b.file.WriteAt(checksumBytes, blockOffset); err != nil {
			return n, err
		}

		if _, err := b.file.WriteAt(data, blockOffset+_ChecksumSize); err != nil {
			return n, err
		}

		n += nBytes
		b.contentOffset += int64(nBytes)
	}

	return
}

func (b *BlockMapper) Seek(offset int64, whence int) (int64, error) {

	fileSize, err := b.file.Seek(0, io.SeekEnd)
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
		b.contentOffset += offset
	}

	if b.contentOffset < 0 {
		b.contentOffset = 0
	}
	if b.contentOffset > contentSize {
		b.contentOffset = contentSize
	}

	return b.contentOffset, nil
}

func (b *BlockMapper) contentOffsetToBlockOffset(
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

func (b *BlockMapper) readBlock(offset int64) ([]byte, error) {
	buf := make([]byte, b.blockSize)
	n, err := b.file.ReadAt(buf, offset)
	if err == io.EOF {
		buf = buf[:n]
	} else if err != nil {
		return nil, err
	}

	if n < _ChecksumSize {
		// empty
		return nil, nil
	}

	data := buf[_ChecksumSize:]
	expectedChecksum := crc64.Checksum(data, crc64Table)

	checksum := binary.LittleEndian.Uint64(buf[:_ChecksumSize])
	if checksum != expectedChecksum {
		return nil, ErrChecksumNotMatch
	}

	return data, nil
}
