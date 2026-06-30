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
	"bytes"
	"context"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"os"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFileWithChecksumReadAtCoalesce exercises the read-coalescing ReadAt on the
// large / multi-chunk path (reads spanning many on-disk blocks and crossing the
// _ReadCoalesceSize cap), at the production block size (_BlockContentSize=2044)
// and a tiny custom size, over random (offset,length) windows incl. past-EOF.
func TestFileWithChecksumReadAtCoalesce(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	for _, bcs := range []int{_BlockContentSize, 64} {
		dataSize := 5 << 20 // 5 MiB > 2 MiB coalesce cap -> multi-chunk for 2044 blocks
		if bcs == 64 {
			dataSize = 256 << 10
		}
		f, err := os.CreateTemp(tempDir, "*")
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })

		fw := NewFileWithChecksum(ctx, f, bcs, nil)
		src := make([]byte, dataSize)
		_, err = rand.Read(src)
		require.NoError(t, err)
		_, err = fw.WriteAt(src, 0)
		require.NoError(t, err)

		rng := mrand.New(mrand.NewSource(42))
		for it := 0; it < 3000; it++ {
			off := rng.Intn(dataSize + 4096) // sometimes at/past EOF
			length := rng.Intn(3<<20) + 1    // up to 3 MiB -> crosses the 2 MiB cap
			got := make([]byte, length)
			n, rerr := fw.ReadAt(got, int64(off))

			avail := 0
			if off < dataSize {
				if avail = dataSize - off; avail > length {
					avail = length
				}
			}
			start := off
			if start > dataSize {
				start = dataSize
			}
			require.Equalf(t, avail, n, "bcs=%d off=%d len=%d", bcs, off, length)
			require.Equalf(t, src[start:start+avail], got[:n], "bcs=%d off=%d len=%d", bcs, off, length)
			if off+length > dataSize {
				require.ErrorIsf(t, rerr, io.EOF, "bcs=%d off=%d len=%d", bcs, off, length)
			} else {
				require.NoErrorf(t, rerr, "bcs=%d off=%d len=%d", bcs, off, length)
			}
		}
	}
}

func TestFileWithChecksumOffsets(t *testing.T) {
	ctx := context.Background()
	f := NewFileWithChecksum[*os.File](ctx, nil, 64, nil)

	blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(0)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(1)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize))
	assert.Equal(t, int64(f.blockSize), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize) + 1)
	assert.Equal(t, int64(f.blockSize), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize)*2 + 1)
	assert.Equal(t, int64(f.blockSize*2), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize)*3 + 1)
	assert.Equal(t, int64(f.blockSize*3), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)
}

func TestFileWithChecksum(t *testing.T) {
	blockContentSize := 8
	tempDir := t.TempDir()

	testFileWithChecksum(
		t,
		blockContentSize,
		func() FileLike {
			f, err := os.CreateTemp(tempDir, "*")
			assert.Nil(t, err)
			t.Cleanup(func() {
				f.Close()
			})
			return f
		},
	)
}

func testFileWithChecksum(
	t *testing.T,
	blockContentSize int,
	newUnderlying func() FileLike,
) {

	for i := 0; i < blockContentSize*4; i++ {

		underlying := newUnderlying()
		ctx := context.Background()
		fileWithChecksum := NewFileWithChecksum(ctx, underlying, blockContentSize, nil)

		check := func(data []byte) {
			// check content
			pos, err := fileWithChecksum.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(0), pos)
			content, err := io.ReadAll(fileWithChecksum)
			assert.Nil(t, err)
			assert.Equal(t, data, content)

			// seek
			n, err := fileWithChecksum.Seek(0, io.SeekEnd)
			assert.Nil(t, err)
			assert.Equal(t, int64(len(data)), n)

			// iotest
			pos, err = fileWithChecksum.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(0), pos)
			err = iotest.TestReader(fileWithChecksum, data)
			if err != nil {
				t.Logf("%s", err)
			}
			assert.Nil(t, err)
		}

		// random bytes
		data := make([]byte, i)
		_, err := rand.Read(data)
		assert.Nil(t, err)

		// write
		n, err := fileWithChecksum.Write(data)
		assert.Nil(t, err)
		assert.Equal(t, i, n)

		// underlying size
		underlyingSize, err := underlying.Seek(0, io.SeekEnd)
		assert.Nil(t, err)
		expectedSize := len(data) / blockContentSize * (blockContentSize + _ChecksumSize)
		mod := len(data) % blockContentSize
		if mod != 0 {
			expectedSize += _ChecksumSize + mod
		}
		assert.Equal(t, expectedSize, int(underlyingSize))

		check(data)

		for j := 0; j < len(data); j++ {

			// seek and write random bytes
			_, err = rand.Read(data[j:])
			assert.Nil(t, err)
			pos, err := fileWithChecksum.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			n, err = fileWithChecksum.Write(data[j:])
			assert.Nil(t, err)
			assert.Equal(t, len(data[j:]), n)

			// seek and read
			pos, err = fileWithChecksum.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			content, err := io.ReadAll(fileWithChecksum)
			assert.Nil(t, err)
			assert.Equal(t, data[j:], content)

			check(data)

		}

	}
}

func TestMultiLayerFileWithChecksum(t *testing.T) {
	blockContentSize := 8
	tempDir := t.TempDir()

	testFileWithChecksum(
		t,
		blockContentSize,
		func() FileLike {
			f, err := os.CreateTemp(tempDir, "*")
			assert.Nil(t, err)
			t.Cleanup(func() {
				f.Close()
			})
			ctx := context.Background()
			f2 := NewFileWithChecksum(ctx, f, blockContentSize, nil)
			f3 := NewFileWithChecksum(ctx, f2, blockContentSize, nil)
			f4 := NewFileWithChecksum(ctx, f3, blockContentSize, nil)
			return f4
		},
	)
}

func BenchmarkFileWithChecksumRead(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	f2 := NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	_, err = f2.Write(bytes.Repeat([]byte("a"), 65536))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = f2.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(io.Discard, f2)
		if err != nil {
			b.Fatal(err)
		}
		if n != 65536 {
			b.Fatal()
		}
	}
}

func BenchmarkFileWithChecksumWrite(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	content := bytes.Repeat([]byte("a"), 65536)
	f2 := NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	_, err = f2.Write(content)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = f2.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatal(err)
		}
		n, err := f2.Write(content)
		if err != nil {
			b.Fatal(err)
		}
		if n != 65536 {
			b.Fatal()
		}
	}
}

// readAtPerBlock mimics the pre-coalescing ReadAt: one underlying ReadAt (syscall)
// per on-disk block. Used only to benchmark the speedup of the coalesced path.
func (f *FileWithChecksum[T]) readAtPerBlock(buf []byte, offset int64) (n int, err error) {
	for len(buf) > 0 {
		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)
		data, putback, rerr := f.readBlock(blockOffset)
		if rerr != nil && rerr != io.EOF {
			putback.Put()
			return n, rerr
		}
		data = data[offsetInBlock:]
		c := copy(buf, data)
		buf = buf[c:]
		putback.Put()
		offset += int64(c)
		n += c
		if rerr == io.EOF {
			break
		}
	}
	return n, err
}

// TestFileWithoutChecksum exercises the passthrough (noChecksum) mode: the
// on-disk bytes must be raw (no CRC32 framing, no size inflation) and the
// positional read/write/seek semantics must round-trip.
func TestFileWithoutChecksum(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	// identity offset mapping
	idf := NewFileWithoutChecksum[*os.File](ctx, nil, _BlockContentSize, nil)
	for _, off := range []int64{0, 1, 2044, 2048, 1 << 20} {
		blockOffset, offsetInBlock := idf.contentOffsetToBlockOffset(off)
		assert.Equal(t, off, blockOffset)
		assert.Equal(t, int64(0), offsetInBlock)
	}

	f, err := os.CreateTemp(tempDir, "*")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	fw := NewFileWithoutChecksum(ctx, f, _BlockContentSize, nil)

	// write across many "blocks" worth of content
	src := make([]byte, 5000)
	_, err = rand.Read(src)
	require.NoError(t, err)
	n, err := fw.Write(src)
	require.NoError(t, err)
	require.Equal(t, len(src), n)

	// raw invariant: underlying file size == bytes written (no CRC inflation)
	info, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(len(src)), info.Size())

	// raw invariant: on-disk bytes are exactly what we wrote
	onDisk, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.True(t, bytes.Equal(src, onDisk))

	// Seek(SeekEnd) reports the true file size in raw mode
	end, err := fw.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(src)), end)

	// random ReadAt round-trips against the source
	rng := mrand.New(mrand.NewSource(7))
	for it := 0; it < 500; it++ {
		off := rng.Intn(len(src))
		length := rng.Intn(len(src)-off) + 1
		got := make([]byte, length)
		rn, rerr := fw.ReadAt(got, int64(off))
		require.NoError(t, rerr)
		require.Equal(t, length, rn)
		require.True(t, bytes.Equal(src[off:off+length], got))
	}

	// WriteAt at an arbitrary offset overwrites in place (no framing)
	patch := []byte("PATCHED-IN-PLACE")
	_, err = fw.WriteAt(patch, 100)
	require.NoError(t, err)
	got := make([]byte, len(patch))
	_, err = fw.ReadAt(got, 100)
	require.NoError(t, err)
	require.True(t, bytes.Equal(patch, got))
	info, err = f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(len(src)), info.Size()) // overwrite, not append
}

// BenchmarkChecksumVsRaw measures the read/write throughput of the checksummed
// (DISK) vs raw passthrough (DISK-V2) format at the production block size.
func BenchmarkChecksumVsRaw(b *testing.B) {
	ctx := context.Background()
	const dataSize = 24 << 20 // 24 MiB ~ one column block

	makeFW := func(b *testing.B, raw bool) *FileWithChecksum[*os.File] {
		f, err := os.CreateTemp(b.TempDir(), "*")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { f.Close() })
		if raw {
			return NewFileWithoutChecksum(ctx, f, _BlockContentSize, nil)
		}
		return NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	}

	src := make([]byte, dataSize)
	rand.Read(src)

	for _, raw := range []bool{false, true} {
		mode := "checksum"
		if raw {
			mode = "raw"
		}

		b.Run(mode+"/ReadAt", func(b *testing.B) {
			fw := makeFW(b, raw)
			if _, err := fw.WriteAt(src, 0); err != nil {
				b.Fatal(err)
			}
			out := make([]byte, dataSize)
			b.SetBytes(dataSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := fw.ReadAt(out, 0); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(mode+"/WriteAt", func(b *testing.B) {
			fw := makeFW(b, raw)
			b.SetBytes(dataSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := fw.WriteAt(src, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFileWithChecksumReadAt(b *testing.B) {
	ctx := context.Background()
	f, err := os.CreateTemp(b.TempDir(), "*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	fw := NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	const dataSize = 24 << 20 // 24 MiB ~ one column block
	src := make([]byte, dataSize)
	rand.Read(src)
	if _, err := fw.WriteAt(src, 0); err != nil {
		b.Fatal(err)
	}
	out := make([]byte, dataSize)

	b.Run("coalesced", func(b *testing.B) {
		b.SetBytes(dataSize)
		for i := 0; i < b.N; i++ {
			fw.ReadAt(out, 0)
		}
	})
	b.Run("perblock", func(b *testing.B) {
		b.SetBytes(dataSize)
		for i := 0; i < b.N; i++ {
			fw.readAtPerBlock(out, 0)
		}
	})
}
