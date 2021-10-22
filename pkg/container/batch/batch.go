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

package batch

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/shuffle"
	"matrixone/pkg/vm/mheap"
)

func New(ro bool, attrs []string) *Batch {
	return &Batch{
		Ro:    ro,
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func Reorder(bat *Batch, attrs []string) {
	if bat.Ro {
		Cow(bat)
	}
	for i, name := range attrs {
		for j, attr := range bat.Attrs {
			if name == attr {
				bat.Vecs[i], bat.Vecs[j] = bat.Vecs[j], bat.Vecs[i]
				bat.Attrs[i], bat.Attrs[j] = bat.Attrs[j], bat.Attrs[i]
			}
		}
	}
}

func Shuffle(bat *Batch, m *mheap.Mheap) error {
	var err error

	if bat.SelsData != nil {
		for i, vec := range bat.Vecs {
			if bat.Vecs[i], err = vector.Shuffle(vec, bat.Sels, m); err != nil {
				return err
			}
		}
		for _, r := range bat.Ring.Rs {
			r.Shuffle(bat.Sels, m)
		}
		data, err := mheap.Alloc(m, int64(len(bat.Ring.Zs))*8)
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		bat.Ring.Zs = shuffle.I64Shuffle(bat.Ring.Zs, ws, bat.Sels)
		mheap.Free(m, data)
		mheap.Free(m, bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	return nil
}

func Length(bat *Batch) int {
	return len(bat.Ring.Zs)
}

func Prefetch(bat *Batch, attrs []string, vecs []*vector.Vector) {
	for i, attr := range attrs {
		vecs[i] = GetVector(bat, attr)
	}
}

func GetVector(bat *Batch, name string) *vector.Vector {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}
		return bat.Vecs[i]
	}
	return nil
}

func Clean(bat *Batch, m *mheap.Mheap) {
	if bat.SelsData != nil {
		mheap.Free(m, bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}
	bat.Vecs = nil
	for _, r := range bat.Ring.Rs {
		r.Free(m)
	}
	bat.Ring.Rs = nil
	bat.Ring.As = nil
	bat.Ring.Zs = nil
}

func Reduce(bat *Batch, attrs []string, m *mheap.Mheap) {
	if bat.Ro {
		Cow(bat)
	}
	for _, attr := range attrs {
		for i := 0; i < len(bat.Attrs); i++ {
			if bat.Attrs[i] != attr {
				continue
			}
			if bat.Vecs[i].Ref != 0 {
				vector.Free(bat.Vecs[i], m)
			}
			if bat.Vecs[i].Ref == 0 {
				bat.Vecs = append(bat.Vecs[:i], bat.Vecs[i+1:]...)
				bat.Attrs = append(bat.Attrs[:i], bat.Attrs[i+1:]...)
				i--
			}
			break
		}
	}
}

func Cow(bat *Batch) {
	attrs := make([]string, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrs[i] = attr
	}
	bat.Ro = false
	bat.Attrs = attrs
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	if len(bat.Sels) > 0 {
		fmt.Printf("%v\n", bat.Sels)
	}
	for i, attr := range bat.Attrs {
		buf.WriteString(fmt.Sprintf("%s\n", attr))
		buf.WriteString(fmt.Sprintf("\t%s\n", bat.Vecs[i]))
	}
	return buf.String()
}
