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
	"sync"

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

// _ReadCoalesceSize bounds how many bytes of on-disk (checksummed) blocks ReadAt
// pulls per underlying read. The on-disk layout interleaves a 4-byte CRC32 before
// every blockContentSize bytes, so a naive reader does one tiny pread per block
// (e.g. 2KB) — ~N syscalls for an N-block range. Instead we read up to this many
// bytes of contiguous blocks in a single underlying ReadAt, then verify the CRCs
// and de-interleave the payloads in memory.
//
// 128 KiB is the measured knee: a 24MB read drops from ~12k syscalls (2KB blocks)
// to ~190, collapsing the per-2KB syscall overhead from ~12ms to ~0.2ms.
// Throughput is flat from ~64 KiB up to 2 MiB (a benchmark sweep showed <6%
// difference across that range), so we pick the small end of the plateau: it
// matches the typical NVMe MDTS / block-layer max request (a larger pread is just
// split into MDTS-sized device commands by the kernel) and keeps the pooled
// scratch small per concurrent reader. The per-2KB CRC32 + de-interleave copy are
// inherent to the on-disk format and unaffected by this size.
const _ReadCoalesceSize = 128 << 10 // 128 KiB (≈ NVMe MDTS)

var readCoalesceBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, _ReadCoalesceSize)
		return &b
	},
}

func (f *FileWithChecksum[T]) ReadAt(buf []byte, offset int64) (n int, err error) {
	if len(buf) == 0 {
		return 0, nil
	}

	blockSize := int64(f.blockSize)
	blockContentSize := int64(f.blockContentSize)
	// max whole on-disk blocks to pull per underlying read.
	maxBlocks := int64(_ReadCoalesceSize) / blockSize
	if maxBlocks < 1 {
		maxBlocks = 1
	}

	bufp := readCoalesceBufPool.Get().(*[]byte)
	scratch := *bufp
	defer readCoalesceBufPool.Put(bufp)

	for len(buf) > 0 {
		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)

		// whole blocks needed to cover the remaining buf (offsetInBlock only
		// applies to the first block of the run), capped at maxBlocks.
		need := (offsetInBlock + int64(len(buf)) + blockContentSize - 1) / blockContentSize
		if need > maxBlocks {
			need = maxBlocks
		}
		chunkBytes := need * blockSize
		var raw []byte
		if chunkBytes <= int64(len(scratch)) {
			raw = scratch[:chunkBytes]
		} else {
			// blockSize larger than the pooled buffer (uncommon) — one-off alloc.
			raw = make([]byte, chunkBytes)
		}

		rn, rerr := f.underlying.ReadAt(raw, blockOffset)
		if rerr != nil && rerr != io.EOF {
			return n, rerr
		}
		raw = raw[:rn]

		// de-interleave: each on-disk block = [crc32 4B][content]. The last block
		// at EOF may be short (content < blockContentSize) — slice to what's there.
		for len(raw) >= _ChecksumSize && len(buf) > 0 {
			blkLen := int(blockSize)
			if blkLen > len(raw) {
				blkLen = len(raw)
			}
			block := raw[:blkLen]
			content := block[_ChecksumSize:]
			sum := binary.LittleEndian.Uint32(block[:_ChecksumSize])
			if crc32.Checksum(content, crcTable) != sum {
				return n, moerr.NewInternalErrorNoCtx("checksum not match")
			}

			if offsetInBlock > 0 {
				if offsetInBlock >= int64(len(content)) {
					content = content[len(content):]
				} else {
					content = content[offsetInBlock:]
				}
				offsetInBlock = 0
			}

			c := copy(buf, content)
			buf = buf[c:]
			n += c
			offset += int64(c)
			raw = raw[blkLen:]
		}

		if rerr == io.EOF {
			// reached end of file; if buf isn't full, report short read per io.ReaderAt.
			if len(buf) > 0 {
				err = io.EOF
			}
			break
		}
	}
	return n, err
}

func (f *FileWithChecksum[T]) Read(buf []byte) (n int, err error) {
	n, err = f.ReadAt(buf, f.contentOffset)
	f.contentOffset += int64(n)
	return
}

func (f *FileWithChecksum[T]) WriteAt(buf []byte, offset int64) (n int, err error) {
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
		}

		if n, err := f.underlying.WriteAt(data, blockOffset+_ChecksumSize); err != nil {
			putback.Put()
			return n, err
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

	//perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
	//	c.FileService.FileWithChecksum.UnderlyingRead.Add(int64(n))
	//}, f.perfCounterSets...)

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

func (f *FileWithChecksum[T]) dontNeedContentRange(contentOffset int64, contentSize int64) {
	file, ok := any(f.underlying).(*os.File)
	if !ok {
		return
	}

	blockOffset, _ := f.contentOffsetToBlockOffset(contentOffset)
	if contentSize < 0 {
		fadviseDontNeed(file, blockOffset, 0)
		return
	}
	if contentSize == 0 {
		return
	}

	endBlockOffset, endOffsetInBlock := f.contentOffsetToBlockOffset(contentOffset + contentSize)
	if endOffsetInBlock > 0 {
		endBlockOffset += int64(f.blockSize)
	}
	if endBlockOffset <= blockOffset {
		return
	}
	fadviseDontNeed(file, blockOffset, endBlockOffset-blockOffset)
}
