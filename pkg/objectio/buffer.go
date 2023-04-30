// Copyright 2021 Matrix Origin
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

package objectio

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// ObjectBuffer is the buffer prepared before writing to
// the object file, all data written to the object needs
// to be filled in it, and then written to the object
// file at one time
type ObjectBuffer struct {
	buf    *bytes.Buffer
	vector fileservice.IOVector
}

func NewObjectBuffer(name string) *ObjectBuffer {
	buffer := &ObjectBuffer{
		buf: new(bytes.Buffer),
		vector: fileservice.IOVector{
			FilePath: name,
		},
	}
	buffer.vector.Entries = make([]fileservice.IOEntry, 0)
	return buffer
}

func (b *ObjectBuffer) Write(buf []byte, items ...WriteOptions) (int, int) {
	offset := int64(0)
	le := len(b.vector.Entries)
	if len(b.vector.Entries) > 0 {
		offset = b.vector.Entries[le-1].Offset +
			b.vector.Entries[le-1].Size
	}
	entry := fileservice.IOEntry{
		Offset: offset,
		Size:   int64(len(buf)),
		Data:   buf,
	}
	b.vector.Entries = append(b.vector.Entries, entry)
	return int(offset), len(buf)
}

func (b *ObjectBuffer) Length() int {
	return b.buf.Len()
}

func (b *ObjectBuffer) GetData() fileservice.IOVector {
	return b.vector
}

func (b *ObjectBuffer) SetDataOptions(items ...WriteOptions) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		switch item.Type {
		case WriteTS:
			ts := item.Val.(time.Time)
			b.vector.ExpireAt = ts
		default:
			continue
		}
	}
}
