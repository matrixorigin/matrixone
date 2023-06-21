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
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ioEntriesReader struct {
	ctx     context.Context
	entries []IOEntry
	offset  int64
}

var _ io.Reader = new(ioEntriesReader)

func newIOEntriesReader(ctx context.Context, entries []IOEntry) *ioEntriesReader {
	es := make([]IOEntry, len(entries))
	copy(es, entries)
	return &ioEntriesReader{
		ctx:     ctx,
		entries: es,
	}
}

func (i *ioEntriesReader) Read(buf []byte) (n int, err error) {
	for {

		select {
		case <-i.ctx.Done():
			return n, i.ctx.Err()
		default:
		}

		// no more data
		if len(i.entries) == 0 {
			err = io.EOF
			return
		}

		entry := i.entries[0]

		// gap
		if i.offset < entry.Offset {
			numBytes := entry.Offset - i.offset
			if l := int64(len(buf)); l < numBytes {
				numBytes = l
			}
			buf = buf[numBytes:] // skip
			n += int(numBytes)
			i.offset += numBytes
		}

		// buffer full
		if len(buf) == 0 {
			return
		}

		// copy data
		var bytesRead int

		if entry.ReaderForWrite != nil && entry.Size < 0 {
			// read from size unknown reader
			bytesRead, err = entry.ReaderForWrite.Read(buf)
			i.entries[0].Offset += int64(bytesRead)
			if err == io.EOF {
				i.entries = i.entries[1:]
			} else if err != nil {
				return
			}

		} else if entry.ReaderForWrite != nil {
			bytesToRead := entry.Size
			if l := int64(len(buf)); bytesToRead > l {
				bytesToRead = l
			}
			r := io.LimitReader(entry.ReaderForWrite, int64(bytesToRead))
			bytesRead, err = io.ReadFull(r, buf[:bytesToRead])
			if err != nil {
				return
			}
			if int64(bytesRead) != bytesToRead {
				err = moerr.NewSizeNotMatchNoCtx("")
				return
			}
			i.entries[0].Offset += int64(bytesRead)
			i.entries[0].Size -= int64(bytesRead)
			if i.entries[0].Size == 0 {
				i.entries = i.entries[1:]
			}

		} else {
			bytesToRead := entry.Size
			if l := int64(len(buf)); bytesToRead > l {
				bytesToRead = l
			}
			if int64(len(entry.Data)) != entry.Size {
				err = moerr.NewSizeNotMatchNoCtx("")
				return
			}
			bytesRead = copy(buf, entry.Data[:bytesToRead])
			if int64(bytesRead) != bytesToRead {
				err = moerr.NewSizeNotMatchNoCtx("")
				return
			}
			i.entries[0].Data = entry.Data[bytesRead:]
			i.entries[0].Offset += int64(bytesRead)
			i.entries[0].Size -= int64(bytesRead)
			if i.entries[0].Size == 0 {
				i.entries = i.entries[1:]
			}
		}

		buf = buf[bytesRead:]
		i.offset += int64(bytesRead)
		n += int(bytesRead)

	}
}

func (e *IOEntry) setObjectBytesFromData() error {
	if e.ToObjectBytes == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	bs, size, err := e.ToObjectBytes(bytes.NewReader(e.Data), e.Data)
	if err != nil {
		return err
	}
	e.ObjectBytes = bs
	e.ObjectSize = size
	return nil
}

func (e *IOEntry) ReadFromOSFile(file *os.File) error {
	r := io.LimitReader(file, e.Size)

	if len(e.Data) < int(e.Size) {
		e.Data = make([]byte, e.Size)
	}

	n, err := io.ReadFull(r, e.Data)
	if err != nil {
		return err
	}
	if n != int(e.Size) {
		return io.ErrUnexpectedEOF
	}

	if e.WriterForRead != nil {
		if _, err := e.WriterForRead.Write(e.Data); err != nil {
			return err
		}
	}
	if e.ReadCloserForRead != nil {
		*e.ReadCloserForRead = io.NopCloser(bytes.NewReader(e.Data))
	}
	if err := e.setObjectBytesFromData(); err != nil {
		return err
	}

	e.done = true

	return nil
}
