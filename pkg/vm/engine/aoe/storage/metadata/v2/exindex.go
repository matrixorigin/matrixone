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

package metadata

import (
	"bytes"
	"fmt"
	"matrixone/pkg/encoding"
)

type LogIndex = ExternalIndex

type ExternalIndex struct {
	Id       uint64
	Start    uint64
	Count    uint64
	Capacity uint64
}

func (idx *ExternalIndex) String() string {
	return fmt.Sprintf("(%d,%d,%d,%d)", idx.Id, idx.Start, idx.Count, idx.Capacity)
}

func (idx *ExternalIndex) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}

func (idx *ExternalIndex) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(idx.Id))
	buf.Write(encoding.EncodeUint64(idx.Count))
	buf.Write(encoding.EncodeUint64(idx.Start))
	buf.Write(encoding.EncodeUint64(idx.Capacity))
	return buf.Bytes(), nil
}

func (idx *ExternalIndex) UnMarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	idx.Id = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Count = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Start = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Capacity = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	return nil
}
