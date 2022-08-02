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

package types

import (
	"bytes"
)

const (
	MaxStringSize = 10485760
)

func (a *Bytes) Reset() {
	a.Offsets = a.Offsets[:0]
	a.Lengths = a.Lengths[:0]
	a.Data = a.Data[:0]
}

func (a *Bytes) Window(start, end int) *Bytes {
	return &Bytes{
		Data:    a.Data,
		Offsets: a.Offsets[start:end],
		Lengths: a.Lengths[start:end],
	}
}

func (a *Bytes) AppendOnce(v []byte) {
	o := uint32(len(a.Data))
	a.Offsets = append(a.Offsets, o)
	a.Data = append(a.Data, v...)
	a.Lengths = append(a.Lengths, uint32(len(v)))
}

func (a *Bytes) Append(vs [][]byte) error {
	o := uint32(len(a.Data))
	for _, v := range vs {
		a.Offsets = append(a.Offsets, o)
		a.Data = append(a.Data, v...)
		o += uint32(len(v))
		a.Lengths = append(a.Lengths, uint32(len(v)))
	}
	return nil
}

func (a *Bytes) Get(n int64) []byte {
	offset := a.Offsets[n]
	return a.Data[offset : offset+a.Lengths[n]]
}

func (a *Bytes) Swap(i, j int64) {
	a.Offsets[i], a.Offsets[j] = a.Offsets[j], a.Offsets[i]
	a.Lengths[i], a.Lengths[j] = a.Lengths[j], a.Lengths[i]
}

func (a *Bytes) String() string {
	var buf bytes.Buffer

	buf.WriteByte('[')
	j := len(a.Offsets) - 1
	for i, o := range a.Offsets {
		buf.Write(a.Data[o : o+a.Lengths[i]])
		if i != j {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	return buf.String()
}
