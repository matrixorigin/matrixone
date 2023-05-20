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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

type EncodeBatch struct {
	Zs       []int64
	Vecs     []*vector.Vector
	Attrs    []string
	AggInfos []aggInfo
}

func (m *EncodeBatch) MarshalBinary() ([]byte, error) {
	// --------------------------------------------------------------------
	// | len | Zs... | len | Vecs... | len | Attrs... | len | AggInfos... |
	// --------------------------------------------------------------------
	var buf bytes.Buffer

	// Zs
	l := int32(len(m.Zs))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		n, _ := buf.Write(types.EncodeInt64(&m.Zs[i]))
		if n != 8 {
			panic("unexpected length for int64")
		}
	}

	// Vecs
	l = int32(len(m.Vecs))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		data, err := m.Vecs[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(data)
	}

	// Attrs
	l = int32(len(m.Attrs))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		size := int32(len(m.Attrs[i]))
		buf.Write(types.EncodeInt32(&size))
		n, _ := buf.WriteString(m.Attrs[i])
		if int32(n) != size {
			panic("unexpected length for string")
		}
	}

	// AggInfos
	l = int32(len(m.AggInfos))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		data, err := m.AggInfos[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(data)
	}

	return buf.Bytes(), nil
}

func (m *EncodeBatch) UnmarshalBinary(data []byte) error {
	// types.DecodeXXX plays with raw pointer, so we make a copy of binary data
	buf := make([]byte, len(data))
	copy(buf, data)

	// Zs
	l := types.DecodeInt32(buf[:4])
	buf = buf[4:]
	zs := make([]int64, l)
	for i := 0; i < int(l); i++ {
		zs[i] = types.DecodeInt64(buf[:8])
		buf = buf[8:]
	}
	m.Zs = zs

	// Vecs
	l = types.DecodeInt32(buf[:4])
	buf = buf[4:]
	vecs := make([]*vector.Vector, l)
	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(buf[:4])
		buf = buf[4:]

		vec := new(vector.Vector)
		if err := vec.UnmarshalBinary(buf[:size]); err != nil {
			return err
		}
		buf = buf[size:]
		vecs[i] = vec
	}
	m.Vecs = vecs

	// Attrs
	l = types.DecodeInt32(buf[:4])
	buf = buf[4:]
	attrs := make([]string, l)
	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(buf[:4])
		buf = buf[4:]
		attrs[i] = string(buf[:size])
		buf = buf[size:]
	}
	m.Attrs = attrs

	// AggInfos
	l = types.DecodeInt32(buf[:4])
	buf = buf[4:]
	aggs := make([]aggInfo, l)
	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(buf[:4])
		buf = buf[4:]

		var agg aggInfo
		if err := agg.UnmarshalBinary(buf[:size]); err != nil {
			return err
		}
		buf = buf[size:]
		aggs[i] = agg
	}
	m.AggInfos = aggs

	return nil
}

type aggInfo struct {
	Op         int
	Dist       bool
	inputTypes types.Type
	Agg        agg.Agg[any]
}

// Batch represents a part of a relationship
// including an optional list of row numbers, columns and list of attributes
//
//	(SelsData, Sels) - list of row numbers
//	(Attrs) - list of attributes
//	(vecs) 	- columns
type Batch struct {
	// Ro if true, Attrs is read only
	Ro bool
	// reference count, default is 1
	Cnt int64
	// Attrs column name list
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector
	// ring
	Zs   []int64
	Aggs []agg.Agg[any]
	Ht   any // hash table
}
