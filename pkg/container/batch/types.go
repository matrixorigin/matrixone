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

var (
	EmptyBatch = &Batch{rowCount: 0}

	EmptyForConstFoldBatch = &Batch{
		Cnt:      1,
		Vecs:     make([]*vector.Vector, 0),
		rowCount: 1,
	}
)

type EncodeBatch struct {
	rowCount  int64
	Vecs      []*vector.Vector
	Attrs     []string
	AggInfos  []aggInfo
	Recursive int32
}

func (m *EncodeBatch) MarshalBinary() ([]byte, error) {
	// --------------------------------------------------------------------
	// | len | Zs... | len | Vecs... | len | Attrs... | len | AggInfos... |
	// --------------------------------------------------------------------
	var buf bytes.Buffer

	// row count.
	rl := int64(m.rowCount)
	buf.Write(types.EncodeInt64(&rl))

	// Vecs
	l := int32(len(m.Vecs))
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

	buf.Write(types.EncodeInt32(&m.Recursive))

	return buf.Bytes(), nil
}

func (m *EncodeBatch) UnmarshalBinary(data []byte) error {
	// types.DecodeXXX plays with raw pointer, so we make a copy of binary data
	buf := make([]byte, len(data))
	copy(buf, data)

	// row count
	m.rowCount = types.DecodeInt64(buf[:8])
	buf = buf[8:]

	// Vecs
	l := types.DecodeInt32(buf[:4])
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

	m.Recursive = types.DecodeInt32(buf[:4])

	return nil
}

type aggInfo struct {
	Op         int64
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
	// For recursive CTE, 1 is last batch, 2 is end of batch
	Recursive int32
	// Ro if true, Attrs is read only
	Ro         bool
	ShuffleIDX int //used only in shuffle dispatch
	// reference count, default is 1
	Cnt int64
	// Attrs column name list
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector
	// ring
	Aggs []agg.Agg[any]

	// row count of batch, to instead of old len(Zs).
	rowCount int

	AuxData any // hash table, runtime filter, etc.
}
