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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ioEntriesReader struct {
	entries []IOEntry
	offset  int64
}

var _ io.Reader = new(ioEntriesReader)

func newIOEntriesReader(entries []IOEntry) *ioEntriesReader {
	es := make([]IOEntry, len(entries))
	copy(es, entries)
	return &ioEntriesReader{
		entries: es,
	}
}

func (i *ioEntriesReader) Read(buf []byte) (n int, err error) {
	for {

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

func (e *IOEntry) setObjectFromData() error {
	if e.ToObject == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	obj, size, err := e.ToObject(bytes.NewReader(e.Data), e.Data)
	if err != nil {
		return err
	}
	e.Object = obj
	e.ObjectSize = size
	return nil
}
