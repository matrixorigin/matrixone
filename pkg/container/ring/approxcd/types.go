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

package approxcd

import (
	"io"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ApproxCountDistinctRing struct {
	Typ types.Type
	Sk  []*hll.Sketch
	Vs  []uint64
	Da  []byte
}

// impl Serialize & Deserialize for sql/protocol

func (r *ApproxCountDistinctRing) Marshal(w io.Writer) error {
	// length
	n := len(r.Sk)
	w.Write(encoding.EncodeUint32(uint32(n)))
	// data & values
	if n > 0 {
		// in some tests Data is nil, encode Vs anyway
		w.Write(encoding.EncodeUint64Slice(r.Vs))
	}
	// sketches
	for _, sk := range r.Sk {
		sk_buf, err := sk.MarshalBinary()
		if err != nil {
			return err
		}
		w.Write(encoding.EncodeUint32(uint32(len(sk_buf))))
		w.Write(sk_buf)
	}
	// type
	w.Write(encoding.EncodeType(r.Typ))
	return nil
}

// Unmarshal builds ApproxCountDistinctRing from `data` and bytes in `data` is allowed to be reused directly
func (r *ApproxCountDistinctRing) Unmarshal(data []byte) (left []byte, err error) {
	return r.unmarshal(data, nil)
}

// UnmarshalWithProc builds ApproxCountDistinctRing from `data` and bytes in `data` is *not* allowed to be reused directly, new memory should be allocated in process instead.
func (r *ApproxCountDistinctRing) UnmarshalWithProc(data []byte, proc *process.Process) (left []byte, err error) {
	return r.unmarshal(data, proc)
}

func (r *ApproxCountDistinctRing) decodeData(data []byte) {
	r.Da = data
	r.Vs = encoding.DecodeUint64Slice(r.Da)
}

func (r *ApproxCountDistinctRing) decodeDataWithProc(data []byte, proc *process.Process) error {
	var err error
	if r.Da, err = mheap.Alloc(proc.Mp, int64(len(data))); err != nil {
		return err
	}
	copy(r.Da, data)
	r.Vs = encoding.DecodeUint64Slice(r.Da)
	return nil
}

func (r *ApproxCountDistinctRing) unmarshal(data []byte, proc *process.Process) (left []byte, err error) {
	// length
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	// data & values
	if n > 0 {
		if proc == nil {
			r.decodeData(data[:n*8])
		} else {
			r.decodeDataWithProc(data[:n*8], proc)
		}
		data = data[n*8:]
	}
	// sketches
	for ; n > 0; n -= 1 {
		n_sk := encoding.DecodeUint32(data[:4])
		data = data[4:]

		sk := hll.New()
		err = sk.UnmarshalBinary(data[:n_sk])
		if err != nil {
			return data, err
		}
		data = data[n_sk:]
		r.Sk = append(r.Sk, sk)
	}
	r.Typ = encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	return data, nil
}
