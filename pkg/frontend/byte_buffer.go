package frontend

import (
	"fmt"
	"io"
)

const (
	defaultMinGrowSize      = 256
	defaultIOCopyBufferSize = 1024 * 4
)

type Allocator interface {
	// Alloc allocate a []byte with len(data) >= size, and the returned []byte cannot
	// be expanded in use.
	Alloc(capacity int) []byte
	// Free free the allocated memory
	Free([]byte)
}

type Slice struct {
	from, to int // [from, to)
	buf      *ByteBuf
}

type ByteBuf struct {
	buf                     []byte // buf data, auto +/- size
	readerIndex             int
	writerIndex             int
	markedIndex             int
	alloc                   Allocator
	minGrowSize             int
	ioCopyBufferSize        int
	disableCompactAfterGrow bool
}

func NewByteBuf(capacity int, allocator Allocator) *ByteBuf {
	b := &ByteBuf{
		buf:                     nil,
		readerIndex:             0,
		writerIndex:             0,
		markedIndex:             0,
		alloc:                   allocator,
		minGrowSize:             defaultMinGrowSize,
		ioCopyBufferSize:        defaultIOCopyBufferSize,
		disableCompactAfterGrow: false,
	}
	b.buf = b.alloc.Alloc(capacity)
	return b
}

// Close close the ByteBuf
func (b *ByteBuf) Close() {
	b.alloc.Free(b.buf)
	b.buf = nil
}

// Reset reset to reuse.
func (b *ByteBuf) Reset() {
	b.readerIndex = 0
	b.writerIndex = 0
	b.markedIndex = 0
}

// SetReadIndex set the reader index. The data in the [readIndex, writeIndex] that can be read.
func (b *ByteBuf) SetReadIndex(readIndex int) {
	if readIndex < 0 || readIndex > b.writerIndex {
		panic(fmt.Sprintf("invalid readIndex %d, writeIndex %d", readIndex, b.writerIndex))
	}

	b.readerIndex = readIndex
}

// GetReadIndex returns the read index
func (b *ByteBuf) GetReadIndex() int {
	return b.readerIndex
}

// SetWriteIndex set the write index. The data can write into range [writeIndex, len(buf)).
// Note, since the underlying buf will expand, the previously held writeIndex will become
// invalid, and in most cases this method should use SetWriteIndexByOffset instead.
func (b *ByteBuf) SetWriteIndex(writeIndex int) {
	if writeIndex < b.readerIndex || writeIndex > b.capacity() {
		panic(fmt.Sprintf("invalid writeIndex %d, capacity %d, readIndex %d",
			writeIndex, b.capacity(), b.readerIndex))
	}

	b.writerIndex = writeIndex
}

// GetWriteIndex get the write index
func (b *ByteBuf) GetWriteIndex() int {
	return b.writerIndex
}

// GetWriteOffset returns the offset of the current writeIndex relative to the ReadIndex.
// GetWriteIndex returns an absolute position, which will fail when the underlying buf is
// expanded.
func (b *ByteBuf) GetWriteOffset() int {
	return b.Readable()
}

// SetWriteIndexByOffset Use writeOffset to reset writeIndex, since offset is a relative
// position to readIndex, so it won't affect correctness when the underlying buf is expanded.
func (b *ByteBuf) SetWriteIndexByOffset(writeOffset int) {
	b.SetWriteIndex(b.readerIndex + writeOffset)
}

// SetMarkIndex mark data in range [readIndex, markIndex)
func (b *ByteBuf) SetMarkIndex(markIndex int) {
	if markIndex > b.writerIndex || markIndex <= b.readerIndex {
		panic(fmt.Sprintf("invalid markIndex %d, readIndex %d, writeIndex %d",
			markIndex, b.readerIndex, b.writerIndex))
	}
	b.markedIndex = markIndex
}

// GetMarkIndex returns the markIndex.
func (b *ByteBuf) GetMarkIndex() int {
	return b.markedIndex
}

// ClearMark clear mark index
func (b *ByteBuf) ClearMark() {
	b.markedIndex = 0
}

// GetMarkedDataLen returns len of marked data
func (b *ByteBuf) GetMarkedDataLen() int {
	return b.markedIndex - b.readerIndex
}

// Skip skip [readIndex, readIndex+n).
func (b *ByteBuf) Skip(n int) {
	if n > b.Readable() {
		panic(fmt.Sprintf("invalid skip %d", n))
	}
	b.readerIndex += n
}

// Slice returns a read only bytebuf slice. ByteBuf may be continuously written to, causing the
// internal buf to reapply, thus invalidating the sliced data in buf[s:e]. Slice only records the
// starting location of the data, and it is safe to read the data when it is certain that the ByteBuf
// will not be written to.
func (b *ByteBuf) Slice(from, to int) Slice {
	if from >= to || to > b.writerIndex {
		panic(fmt.Sprintf("invalid slice by range [%d, %d), writeIndex %d",
			from, to, b.writerIndex))
	}
	return Slice{from, to, b}
}

// RawSlice returns raw buf in range [from, to).  This method requires special care, as the ByteBuf may
// free the internal []byte after the data is written again, causing the slice to fail.
func (b *ByteBuf) RawSlice(from, to int) []byte {
	if from >= to || to > b.writerIndex {
		panic(fmt.Sprintf("invalid slice by range [%d, %d), writeIndex %d",
			from, to, b.writerIndex))
	}
	return b.buf[from:to]
}

// RawBuf returns raw buf. This method requires special care, as the ByteBuf may free the internal []byte
// after the data is written again, causing the slice to fail.
func (b *ByteBuf) RawBuf() []byte {
	return b.buf
}

// Readable return the number of bytes that can be read.
func (b *ByteBuf) Readable() int {
	return b.writerIndex - b.readerIndex
}

// ReadByte read a byte from buf
func (b *ByteBuf) ReadByte() (byte, error) {
	if b.Readable() == 0 {
		return 0, io.EOF
	}

	v := b.buf[b.readerIndex]
	b.readerIndex++
	return v, nil
}

// MustReadByte is similar to ReadByte, buf panic if error retrurned
func (b *ByteBuf) MustReadByte() byte {
	v, err := b.ReadByte()
	if err != nil {
		panic(err)
	}
	return v
}

// ReadBytes read bytes from buf. It's will copy the data to a new byte array.
func (b *ByteBuf) ReadBytes(n int) (readed int, data []byte) {
	readed = n
	if readed > b.Readable() {
		readed = b.Readable()
	}
	if readed == 0 {
		return
	}

	data = make([]byte, readed)
	copy(data, b.buf[b.readerIndex:b.readerIndex+readed])
	b.readerIndex += readed
	return
}

// ReadMarkedData returns [readIndex, markIndex) data
func (b *ByteBuf) ReadMarkedData() []byte {
	_, data := b.ReadBytes(b.GetMarkedDataLen())
	b.ClearMark()
	return data
}

// ReadAll read all readable bytes.
func (b *ByteBuf) ReadAll() (readed int, data []byte) {
	return b.ReadBytes(b.Readable())
}

// PeekN is similar to ReadBytes, but keep readIndex not changed.
func (b *ByteBuf) PeekN(offset, bytes int) []byte {
	if b.Readable() < bytes {
		panic(fmt.Sprintf("peek bytes %d, but readable is %d",
			bytes, b.Readable()))
	}

	start := b.readerIndex + offset
	return b.buf[start : start+bytes]
}

// Writeable return how many bytes can be wirte into buf
func (b *ByteBuf) Writeable() int {
	return b.capacity() - b.writerIndex
}

// MustWrite is similar to Write, but panic if encounter an error.
func (b *ByteBuf) MustWrite(value []byte) {
	if _, err := b.Write(value); err != nil {
		panic(err)
	}
}

// WriteByte write a byte value into buf.
func (b *ByteBuf) WriteByte(v byte) error {
	b.Grow(1)
	b.buf[b.writerIndex] = v
	b.writerIndex++
	return nil
}

// MustWriteByte is similar to WriteByte, but panic if has any error
func (b *ByteBuf) MustWriteByte(v byte) {
	if err := b.WriteByte(v); err != nil {
		panic(err)
	}
}

// Grow grow buf size
func (b *ByteBuf) Grow(n int) {
	if free := b.Writeable(); free < n {
		current := b.capacity()

		if !b.disableCompactAfterGrow {
			current -= b.readerIndex
		}

		step := current / 2
		if step < b.minGrowSize {
			step = b.minGrowSize
		}

		size := current + (n - free)
		target := current
		for {
			if target > size {
				break
			}

			target += step
		}

		newBuf := b.alloc.Alloc(target)
		if b.disableCompactAfterGrow {
			copy(newBuf, b.buf)
		} else {
			offset := b.writerIndex - b.readerIndex
			copy(newBuf, b.buf[b.readerIndex:b.writerIndex])
			b.readerIndex = 0
			b.writerIndex = offset
		}

		b.alloc.Free(b.buf)
		b.buf = newBuf
	}
}

// Write implemented io.Writer interface
func (b *ByteBuf) Write(src []byte) (int, error) {
	n := len(src)
	b.Grow(n)
	copy(b.buf[b.writerIndex:], src)
	b.writerIndex += n
	return n, nil
}

// WriteTo implemented io.WriterTo interface
func (b *ByteBuf) WriteTo(dst io.Writer) (int64, error) {
	n := b.Readable()
	if n == 0 {
		return 0, io.EOF
	}
	if err := WriteTo(b.buf[b.readerIndex:b.writerIndex], dst, b.ioCopyBufferSize); err != nil {
		return 0, err
	}
	b.readerIndex = b.writerIndex
	return int64(n), nil
}

// Read implemented io.Reader interface. return n, nil or 0, io.EOF is successful
func (b *ByteBuf) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	n := b.Readable()
	if n == 0 {
		return 0, io.EOF
	}
	if n > len(dst) {
		n = len(dst)
	}
	copy(dst, b.buf[b.readerIndex:b.readerIndex+n])
	b.readerIndex += n
	return n, nil
}

// ReadFrom implemented io.ReaderFrom interface
func (b *ByteBuf) ReadFrom(r io.Reader) (n int64, err error) {
	for {
		b.Grow(b.ioCopyBufferSize)
		m, e := r.Read(b.buf[b.writerIndex : b.writerIndex+b.ioCopyBufferSize])
		if m < 0 {
			panic("bug: negative Read")
		}

		b.writerIndex += m
		n += int64(m)
		if e == io.EOF {
			return n, nil // e is EOF, so return nil explicitly
		}
		if e != nil {
			return n, e
		}

		if m > 0 {
			return n, e
		}
	}
}

func (b *ByteBuf) String() string {
	return fmt.Sprintf("readerIndex:%d, writerIndex:%d, markedIndex:%d, capacity:%d options: alloc %v, minGrowSize %d ioCopyBufferSize %d disableCompactAfterGrow %v",
		b.readerIndex, b.writerIndex, b.markedIndex, b.capacity(), b.alloc, b.minGrowSize, b.ioCopyBufferSize, b.disableCompactAfterGrow)
}

func (b *ByteBuf) capacity() int {
	return len(b.buf)
}

// WriteTo write data to io.Writer, copyBuffer used to control how much data will written
// at a time.
func WriteTo(data []byte, conn io.Writer, copyBuffer int) error {
	if copyBuffer == 0 || copyBuffer > len(data) {
		copyBuffer = len(data)
	}

	written := 0
	total := len(data)
	var err error
	for {
		to := written + copyBuffer
		if to > total {
			to = total
		}

		n, e := conn.Write(data[written:to])
		if n < 0 {
			panic("invalid write")
		}
		written += n
		if e != nil {
			err = e
			break
		}

		if written == total {
			break
		}
	}
	return err
}
