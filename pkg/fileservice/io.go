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
	"io"
	"sync/atomic"

	"github.com/ncw/directio"
)

type readCloser struct {
	r         io.Reader
	closeFunc func() error
}

var _ io.ReadCloser = new(readCloser)

func (r *readCloser) Read(data []byte) (int, error) {
	return r.r.Read(data)
}

func (r *readCloser) Close() error {
	return r.closeFunc()
}

type countingReader struct {
	R io.Reader
	C *atomic.Int64
}

var _ io.Reader = new(countingReader)

func (c *countingReader) Read(data []byte) (int, error) {
	n, err := c.R.Read(data)
	c.C.Add(int64(n))
	return n, err
}

type writeCloser struct {
	w         io.Writer
	closeFunc func() error
}

var _ io.WriteCloser = new(writeCloser)

func (r *writeCloser) Write(data []byte) (int, error) {
	return r.w.Write(data)
}

func (r *writeCloser) Close() error {
	return r.closeFunc()
}

var ioBufferPool = NewPool(
	256,
	func() []byte {
		return make([]byte, 32*1024)
	},
	nil,
	nil,
)

// from std io.readFullBuffer
func ReadFullBuffer(r io.Reader, target []byte, buffer []byte) (n int, err error) {
	for n < len(target) && err == nil {
		var nn int
		nn, err = r.Read(buffer)
		copy(target[n:], buffer[:nn])
		n += nn
	}
	if n >= len(target) {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}

// from std io.CopyBuffer without CopyFrom and WriteTo to avoid using not-aligned buffer in direct I/O
func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	if buf == nil {
		size := 32 * 1024
		if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
			if l.N < 1 {
				size = 1
			} else {
				size = int(l.N)
			}
		}
		buf = make([]byte, size)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = io.ErrShortWrite
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

const directIOAlignSize = 8 * 1024

var directioBufferPool = NewPool(
	1024,
	func() []byte {
		return directio.AlignedBlock(directIOAlignSize)
	},
	nil, nil,
)
