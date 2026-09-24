// Copyright 2026 Matrix Origin
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

package crt

import (
	"bufio"
	"compress/flate"
	"context"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// A zip archive is read as a stream, from its local file headers, the way
// `unzip -` does: a load source is a sequential stream (an object body, a file,
// or the client's bytes for LOAD DATA LOCAL), so the central directory at the
// end of the archive is never needed.
//
// The entry loaded follows the same rule as a tar archive: the first entry, in
// archive order, that is neither a directory nor hidden (its base name starts
// with "."); later entries are ignored.  Deflate entries are self-delimiting.
// A stored entry must carry its size in the local header, which is what zip
// tools write for a seekable output; a stored entry written as a stream (size
// only in the trailing data descriptor) cannot be delimited and is rejected.
// The entry's CRC-32 and size are verified at the end, from the header or the
// data descriptor.

const (
	zipLocalHeaderSig   = 0x04034b50
	zipCentralHeaderSig = 0x02014b50
	zipEndSig           = 0x06054b50
	zipDescriptorSig    = 0x08074b50
	zipZip64ExtraID     = 0x0001

	zipFlagEncrypted  = 0x1
	zipFlagDescriptor = 0x8

	zipMethodStore   = 0
	zipMethodDeflate = 8

	zipMaxUint32 = 0xffffffff
)

type zipLocalHeader struct {
	flags            uint16
	method           uint16
	crc32            uint32
	compressedSize   uint64
	uncompressedSize uint64
	zip64            bool
	name             string
}

func (h *zipLocalHeader) hasDescriptor() bool {
	return h.flags&zipFlagDescriptor != 0
}

// loadable reports whether the entry is the kind a load reads: not a directory
// and not hidden, the rule getTarReader applies to a tar archive.
func (h *zipLocalHeader) loadable() bool {
	return !strings.HasSuffix(h.name, "/") && !strings.HasPrefix(path.Base(h.name), ".")
}

// getZipReader returns the decompressed bytes of the archive's first loadable
// entry and the function that releases its decoder.
func getZipReader(ctx context.Context, r io.Reader) (io.Reader, func() error, error) {
	// bufio.Reader is an io.ByteReader, so flate consumes exactly the
	// compressed bytes and the data descriptor after them stays readable.
	br := bufio.NewReaderSize(r, 64<<10)
	for {
		h, err := readZipLocalHeader(ctx, br)
		if err != nil {
			return nil, nil, err
		}
		if h.flags&zipFlagEncrypted != 0 {
			return nil, nil, moerr.NewInvalidInputf(ctx, "zip entry '%s' is encrypted", h.name)
		}
		if !h.loadable() {
			if err := skipZipEntry(ctx, br, h); err != nil {
				return nil, nil, err
			}
			continue
		}
		return openZipEntry(ctx, br, h)
	}
}

func readZipLocalHeader(ctx context.Context, br *bufio.Reader) (*zipLocalHeader, error) {
	var sig [4]byte
	if _, err := io.ReadFull(br, sig[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, moerr.NewInvalidInput(ctx, "zip archive contains no file to load")
		}
		return nil, err
	}
	switch binary.LittleEndian.Uint32(sig[:]) {
	case zipLocalHeaderSig:
	case zipCentralHeaderSig, zipEndSig:
		return nil, moerr.NewInvalidInput(ctx, "zip archive contains no file to load")
	default:
		return nil, moerr.NewInvalidInput(ctx, "not a zip archive")
	}

	var fixed [26]byte
	if _, err := io.ReadFull(br, fixed[:]); err != nil {
		return nil, zipTruncated(ctx, err)
	}
	h := &zipLocalHeader{
		flags:            binary.LittleEndian.Uint16(fixed[2:]),
		method:           binary.LittleEndian.Uint16(fixed[4:]),
		crc32:            binary.LittleEndian.Uint32(fixed[10:]),
		compressedSize:   uint64(binary.LittleEndian.Uint32(fixed[14:])),
		uncompressedSize: uint64(binary.LittleEndian.Uint32(fixed[18:])),
	}
	nameLen := int(binary.LittleEndian.Uint16(fixed[22:]))
	extraLen := int(binary.LittleEndian.Uint16(fixed[24:]))
	rest := make([]byte, nameLen+extraLen)
	if _, err := io.ReadFull(br, rest); err != nil {
		return nil, zipTruncated(ctx, err)
	}
	h.name = string(rest[:nameLen])

	// A zip64 extra field replaces sizes recorded as 0xffffffff.  In a local
	// header it carries both sizes, uncompressed first.
	for extra := rest[nameLen:]; len(extra) >= 4; {
		id := binary.LittleEndian.Uint16(extra)
		size := int(binary.LittleEndian.Uint16(extra[2:]))
		if 4+size > len(extra) {
			break
		}
		if id == zipZip64ExtraID {
			h.zip64 = true
			field := extra[4 : 4+size]
			if h.uncompressedSize == zipMaxUint32 && len(field) >= 8 {
				h.uncompressedSize = binary.LittleEndian.Uint64(field)
				field = field[8:]
			}
			if h.compressedSize == zipMaxUint32 && len(field) >= 8 {
				h.compressedSize = binary.LittleEndian.Uint64(field)
			}
		}
		extra = extra[4+size:]
	}
	return h, nil
}

// skipZipEntry consumes an entry that is not loaded.
func skipZipEntry(ctx context.Context, br *bufio.Reader, h *zipLocalHeader) error {
	var n int64
	switch {
	case h.method == zipMethodDeflate:
		fl := flate.NewReader(br)
		var err error
		if n, err = io.Copy(io.Discard, fl); err != nil {
			return zipTruncated(ctx, err)
		}
		_ = fl.Close()
	case h.method == zipMethodStore && !h.hasDescriptor():
		if _, err := io.CopyN(io.Discard, br, int64(h.compressedSize)); err != nil {
			return zipTruncated(ctx, err)
		}
	case h.method == zipMethodStore && strings.HasSuffix(h.name, "/"):
		// A directory has no data even when written with a descriptor.
	default:
		return moerr.NewInvalidInputf(ctx,
			"zip entry '%s' cannot be skipped in a stream (method %d); put the file to load first", h.name, h.method)
	}
	if h.hasDescriptor() {
		_, err := readZipDescriptor(ctx, br, h.zip64 || uint64(n) >= zipMaxUint32)
		return err
	}
	return nil
}

func openZipEntry(ctx context.Context, br *bufio.Reader, h *zipLocalHeader) (io.Reader, func() error, error) {
	var (
		data         io.Reader
		closeDecoder func() error
	)
	switch h.method {
	case zipMethodDeflate:
		fl := flate.NewReader(br)
		data, closeDecoder = fl, fl.Close
	case zipMethodStore:
		if h.hasDescriptor() {
			return nil, nil, moerr.NewInvalidInputf(ctx,
				"stored zip entry '%s' has no size in its local header and cannot be read as a stream; compress it with deflate", h.name)
		}
		data = io.LimitReader(br, int64(h.compressedSize))
	default:
		return nil, nil, moerr.NewInvalidInputf(ctx,
			"zip entry '%s' uses compression method %d; only store and deflate are supported", h.name, h.method)
	}
	return &zipEntryReader{ctx: ctx, br: br, h: h, data: data, crc: crc32.NewIEEE()}, closeDecoder, nil
}

// zipEntryReader verifies the entry's CRC-32 and size when its data ends.
type zipEntryReader struct {
	ctx  context.Context
	br   *bufio.Reader
	h    *zipLocalHeader
	data io.Reader
	crc  hash.Hash32
	n    uint64
	err  error
}

func (z *zipEntryReader) Read(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}
	n, err := z.data.Read(p)
	z.crc.Write(p[:n])
	z.n += uint64(n)
	if err == io.EOF {
		err = z.verify()
		if err == nil {
			err = io.EOF
		}
	} else if err == io.ErrUnexpectedEOF {
		err = zipTruncated(z.ctx, err)
	}
	if err != nil {
		z.err = err
	}
	return n, err
}

func (z *zipEntryReader) verify() error {
	wantCRC, wantSize := z.h.crc32, z.h.uncompressedSize
	if z.h.hasDescriptor() {
		// A writer streaming an entry of 4GB or more may use the 8-byte
		// descriptor without a zip64 extra in the local header.
		d, err := readZipDescriptor(z.ctx, z.br, z.h.zip64 || z.n >= zipMaxUint32)
		if err != nil {
			return err
		}
		wantCRC, wantSize = d.crc32, d.uncompressedSize
	}
	if z.h.method == zipMethodStore && z.n != z.h.compressedSize {
		return zipTruncated(z.ctx, io.ErrUnexpectedEOF)
	}
	if z.crc.Sum32() != wantCRC || z.n != wantSize {
		return moerr.NewInvalidInputf(z.ctx, "zip entry '%s' is corrupt: checksum or size mismatch", z.h.name)
	}
	return nil
}

type zipDescriptor struct {
	crc32            uint32
	uncompressedSize uint64
}

// readZipDescriptor reads the data descriptor after an entry's data: an
// optional signature, the CRC-32, then the compressed and uncompressed sizes,
// 8 bytes each for a zip64 entry and 4 otherwise.
func readZipDescriptor(ctx context.Context, br *bufio.Reader, zip64 bool) (zipDescriptor, error) {
	var d zipDescriptor
	var word [4]byte
	if _, err := io.ReadFull(br, word[:]); err != nil {
		return d, zipTruncated(ctx, err)
	}
	if binary.LittleEndian.Uint32(word[:]) == zipDescriptorSig {
		if _, err := io.ReadFull(br, word[:]); err != nil {
			return d, zipTruncated(ctx, err)
		}
	}
	d.crc32 = binary.LittleEndian.Uint32(word[:])
	sizeLen := 4
	if zip64 {
		sizeLen = 8
	}
	sizes := make([]byte, 2*sizeLen)
	if _, err := io.ReadFull(br, sizes); err != nil {
		return d, zipTruncated(ctx, err)
	}
	if zip64 {
		d.uncompressedSize = binary.LittleEndian.Uint64(sizes[8:])
	} else {
		d.uncompressedSize = uint64(binary.LittleEndian.Uint32(sizes[4:]))
	}
	return d, nil
}

func zipTruncated(ctx context.Context, err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return moerr.NewInvalidInput(ctx, "zip archive is truncated")
	}
	return err
}
