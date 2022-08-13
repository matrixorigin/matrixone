// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memEngine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func (r *reader) Close() error {
	return nil
}

func (r *reader) Read(attrs []string, _ *plan.Expr, m *mheap.Mheap) (*batch.Batch, error) {
	if len(r.segs) == 0 {
		return nil, nil
	}
	{
		if len(r.cds) == 0 {
			r.cds = make([]*bytes.Buffer, len(attrs))
			r.dds = make([]*bytes.Buffer, len(attrs))
			for i := range attrs {
				r.cds[i] = bytes.NewBuffer(make([]byte, 0, 1024))
				r.dds[i] = bytes.NewBuffer(make([]byte, 0, 1024))
			}
		}
	}
	bat := batch.New(true, attrs)
	id := r.segs[0]
	r.segs = r.segs[1:]
	for i, attr := range attrs {
		md := r.attrs[attr]
		if md.Alg == compress.None {
			data, err := r.db.Get(id+"."+attr, r.dds[i])
			if err != nil {
				return nil, err
			}
			bat.Vecs[i] = vector.New(md.Type)
			if err := bat.Vecs[i].Read(data); err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
		} else {
			data, err := r.db.Get(id+"."+attr, r.cds[i])
			if err != nil {
				return nil, err
			}
			bat.Vecs[i] = vector.New(md.Type)
			n := int(types.DecodeInt32(data[len(data)-4:]))
			r.dds[i].Reset()
			if n > r.dds[i].Cap() {
				r.dds[i].Grow(n)
			}
			buf := r.dds[i].Bytes()[:n]
			_, err = compress.Decompress(data[:len(data)-4], buf, int(md.Alg))
			if err != nil {
				return nil, err
			}
			data = buf[:n]
			if err := bat.Vecs[i].Read(data); err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
		}
	}
	n := vector.Length(bat.Vecs[0])
	sels := m.GetSels()
	if n > cap(sels) {
		m.PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for i := 0; i < n; i++ {
		bat.Zs[i] = 1
	}
	return bat, nil
}
