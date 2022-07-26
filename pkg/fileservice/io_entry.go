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
)

type ioEntriesReader struct {
	entries []IOEntry
	offset  int
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
			if len(buf) < numBytes {
				numBytes = len(buf)
			}
			buf = buf[numBytes:] // skip
			n += numBytes
			i.offset += numBytes
		}

		// buffer full
		if len(buf) == 0 {
			return
		}

		// copy data
		numBytes := entry.Size
		if len(buf) < numBytes {
			numBytes = len(buf)
		}
		if entry.ReaderForWrite != nil {
			r := io.LimitReader(entry.ReaderForWrite, int64(numBytes))
			var bytesRead int
			bytesRead, err = io.ReadFull(r, buf[:numBytes])
			if err != nil {
				return
			}
			if bytesRead != numBytes {
				err = ErrSizeNotMatch
				return
			}
		} else {
			if len(entry.Data) != entry.Size {
				err = ErrSizeNotMatch
				return
			}
			copy(buf, entry.Data[:numBytes])
			i.entries[0].Data = entry.Data[numBytes:]
		}
		buf = buf[numBytes:]
		i.entries[0].Offset += numBytes
		i.entries[0].Size -= numBytes
		if i.entries[0].Size == 0 {
			i.entries = i.entries[1:]
		}
		i.offset += numBytes
		n += numBytes

	}
}

func (e *IOEntry) setObjectFromData() error {
	if e.ToObject == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	obj, size, err := e.ToObject(bytes.NewReader(e.Data))
	if err != nil {
		return err
	}
	e.Object = obj
	e.ObjectSize = size
	return nil
}
