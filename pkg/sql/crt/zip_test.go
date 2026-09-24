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
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type zipEntry struct {
	name   string
	data   []byte
	method uint16
	// raw writes the entry with its sizes in the local header (what zip tools
	// write for a seekable output); otherwise archive/zip streams it with a
	// data descriptor.
	raw bool
}

func buildZip(t *testing.T, entries ...zipEntry) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		if e.raw {
			require.Equal(t, uint16(zip.Store), e.method, "raw entries are stored")
			w, err := zw.CreateRaw(&zip.FileHeader{
				Name:               e.name,
				Method:             zip.Store,
				CRC32:              crc32.ChecksumIEEE(e.data),
				CompressedSize64:   uint64(len(e.data)),
				UncompressedSize64: uint64(len(e.data)),
			})
			require.NoError(t, err)
			_, err = w.Write(e.data)
			require.NoError(t, err)
			continue
		}
		w, err := zw.CreateHeader(&zip.FileHeader{Name: e.name, Method: e.method})
		require.NoError(t, err)
		_, err = w.Write(e.data)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func readZip(archive []byte) ([]byte, int, error) {
	src := &closeCounter{Reader: bytes.NewReader(archive)}
	r, err := getUnCompressReader(context.Background(), "", "data.csv.zip", src)
	if err != nil {
		return nil, src.closes, err
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil {
		return got, src.closes, readErr
	}
	return got, src.closes, closeErr
}

func TestZipDeflateAndStored(t *testing.T) {
	payload := bytes.Repeat([]byte("7,delta,2026-09-23 10:00:00\n"), 5000)

	// Deflate, streamed with a data descriptor: sizes and CRC come after.
	got, closes, err := readZip(buildZip(t, zipEntry{name: "part.csv", data: payload, method: zip.Deflate}))
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Equal(t, 1, closes, "the reader owns and closes its source")

	// Stored with sizes in the local header.
	got, _, err = readZip(buildZip(t, zipEntry{name: "part.csv", data: payload, method: zip.Store, raw: true}))
	require.NoError(t, err)
	require.Equal(t, payload, got)

	require.Equal(t, tree.ZIP, GetCompressType("", "s3://b/x/part.CSV.ZIP"))
}

// The loaded entry follows the tar rule: the first entry that is neither a
// directory nor hidden; later entries are ignored.
func TestZipEntrySelection(t *testing.T) {
	archive := buildZip(t,
		zipEntry{name: "export/", method: zip.Store},
		zipEntry{name: "export/.DS_Store", data: []byte("junk junk junk"), method: zip.Deflate},
		zipEntry{name: ".hidden", data: []byte("x"), method: zip.Store, raw: true},
		zipEntry{name: "export/a.csv", data: []byte("1,a\n2,b\n"), method: zip.Deflate},
		zipEntry{name: "export/b.csv", data: []byte("3,c\n"), method: zip.Deflate},
	)
	got, _, err := readZip(archive)
	require.NoError(t, err)
	require.Equal(t, "1,a\n2,b\n", string(got))
}

func TestZipRejections(t *testing.T) {
	// A stored entry streamed with a data descriptor cannot be delimited.
	_, _, err := readZip(buildZip(t, zipEntry{name: "a.csv", data: []byte("1,a\n"), method: zip.Store}))
	require.ErrorContains(t, err, "cannot be read as a stream")

	// Encrypted: set bit 0 of the general purpose flags (offset 6).
	enc := buildZip(t, zipEntry{name: "a.csv", data: []byte("1,a\n"), method: zip.Deflate})
	enc[6] |= 0x1
	_, _, err = readZip(enc)
	require.ErrorContains(t, err, "encrypted")

	// Methods other than store and deflate (offset 8).
	other := buildZip(t, zipEntry{name: "a.csv", data: []byte("1,a\n"), method: zip.Store, raw: true})
	binary.LittleEndian.PutUint16(other[8:], 93)
	_, _, err = readZip(other)
	require.ErrorContains(t, err, "compression method 93")

	_, _, err = readZip(buildZip(t))
	require.ErrorContains(t, err, "no file to load")

	_, _, err = readZip(buildZip(t, zipEntry{name: "only/", method: zip.Store}))
	require.ErrorContains(t, err, "no file to load")

	_, _, err = readZip([]byte("id,name\n1,a\n"))
	require.ErrorContains(t, err, "not a zip archive")
}

func TestZipIntegrity(t *testing.T) {
	payload := []byte("1,alpha\n2,bravo\n3,charlie\n")

	// Corrupt one stored byte: the CRC check fails at the end of the entry.
	stored := buildZip(t, zipEntry{name: "a.csv", data: payload, method: zip.Store, raw: true})
	i := bytes.Index(stored, payload)
	require.Positive(t, i)
	stored[i] ^= 0xff
	_, _, err := readZip(stored)
	require.ErrorContains(t, err, "corrupt")

	// Wrong CRC recorded in a deflate entry's data descriptor.
	deflated := buildZip(t, zipEntry{name: "a.csv", data: payload, method: zip.Deflate})
	d := bytes.Index(deflated, []byte{0x50, 0x4b, 0x07, 0x08})
	require.Positive(t, d)
	deflated[d+4] ^= 0xff
	_, _, err = readZip(deflated)
	require.ErrorContains(t, err, "corrupt")

	// Truncated in the middle of the deflate stream.
	full := buildZip(t, zipEntry{name: "a.csv", data: bytes.Repeat(payload, 1000), method: zip.Deflate})
	_, _, err = readZip(full[:len(full)/3])
	require.ErrorContains(t, err, "truncated")
}

// A local header with sizes in a zip64 extra field, as a zip tool writes for
// an entry over 4GB; the entry itself is small so the test stays cheap.
func TestZipZip64LocalHeader(t *testing.T) {
	payload := []byte("1,zip64\n")
	name := []byte("big.csv")
	extra := make([]byte, 4+16)
	binary.LittleEndian.PutUint16(extra[0:], 0x0001)
	binary.LittleEndian.PutUint16(extra[2:], 16)
	binary.LittleEndian.PutUint64(extra[4:], uint64(len(payload)))
	binary.LittleEndian.PutUint64(extra[12:], uint64(len(payload)))

	var buf bytes.Buffer
	hdr := make([]byte, 30)
	binary.LittleEndian.PutUint32(hdr[0:], 0x04034b50)
	binary.LittleEndian.PutUint16(hdr[4:], 45)
	binary.LittleEndian.PutUint32(hdr[14:], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(hdr[18:], 0xffffffff)
	binary.LittleEndian.PutUint32(hdr[22:], 0xffffffff)
	binary.LittleEndian.PutUint16(hdr[26:], uint16(len(name)))
	binary.LittleEndian.PutUint16(hdr[28:], uint16(len(extra)))
	buf.Write(hdr)
	buf.Write(name)
	buf.Write(extra)
	buf.Write(payload)
	// Nothing after the entry is read, so no central directory is needed.

	got, _, err := readZip(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// buildRawDeflateZip writes one deflate entry with its sizes and CRC in the
// local header and no data descriptor, as Info-ZIP writes to a seekable file.
func buildRawDeflateZip(t *testing.T, name string, payload []byte) []byte {
	var comp bytes.Buffer
	fw, err := flate.NewWriter(&comp, flate.DefaultCompression)
	require.NoError(t, err)
	_, err = fw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, fw.Close())

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.CreateRaw(&zip.FileHeader{
		Name:               name,
		Method:             zip.Deflate,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(comp.Len()),
		UncompressedSize64: uint64(len(payload)),
	})
	require.NoError(t, err)
	_, err = w.Write(comp.Bytes())
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// The compressed size is verified too, from the header or the data
// descriptor: a deflate stream that ends cleanly must still have consumed
// exactly the recorded compressed bytes.
func TestZipCompressedSizeVerified(t *testing.T) {
	payload := bytes.Repeat([]byte("5,echo,2026-09-23\n"), 200)

	archive := buildRawDeflateZip(t, "a.csv", payload)
	got, _, err := readZip(archive)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Header compressed size (offset 18) off by one.
	bad := bytes.Clone(archive)
	binary.LittleEndian.PutUint32(bad[18:], binary.LittleEndian.Uint32(bad[18:])+1)
	_, _, err = readZip(bad)
	require.ErrorContains(t, err, "corrupt")

	// Descriptor compressed size (after signature and CRC) off by one.
	streamed := buildZip(t, zipEntry{name: "a.csv", data: payload, method: zip.Deflate})
	d := bytes.Index(streamed, []byte{0x50, 0x4b, 0x07, 0x08})
	require.Positive(t, d)
	binary.LittleEndian.PutUint32(streamed[d+8:], binary.LittleEndian.Uint32(streamed[d+8:])+1)
	_, _, err = readZip(streamed)
	require.ErrorContains(t, err, "corrupt")
}
