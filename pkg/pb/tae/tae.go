// Copyright 2021 - 2022 Matrix Origin
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

package tae

import (
	"encoding/binary"
	"fmt"
	io "io"
)

func (d *TableEntryDelta) CompactString() string {
	return fmt.Sprintf("d[n-%s,cstr-%d]", d.Name, len(d.Constraints))
}

func (d *TableEntryDelta) Clone() TableEntryDelta {
	return TableEntryDelta{
		Name:        d.Name,
		Constraints: d.Constraints,
	}
}

func (d *TableEntryDelta) MarshalToWriter(w io.Writer) (int, error) {
	if bytes, err := d.Marshal(); err != nil {
		return 0, err
	} else {
		if err = binary.Write(w, binary.BigEndian, uint32(len(bytes))); err != nil {
			return 0, err
		}
		n, err := w.Write(bytes)
		return n + 4, err
	}
}

func (d *TableEntryDelta) UnmarshalFromReader(r io.Reader) (int, error) {
	size := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}
	buf := make([]byte, size)
	if x, err := r.Read(buf); err != nil {
		return x + 4, err
	}
	err := d.Unmarshal(buf)
	return int(size + 4), err
}
