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

package spillEngine

import (
	"fmt"
	"hash/crc32"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/spillEngine/segment"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"path"

	"github.com/pierrec/lz4"
)

func (r *relation) Close() {}

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (s *relation) Size(_ string) int64 {
	return 0
}

func (r *relation) Segments() []engine.SegmentInfo {
	segs := make([]engine.SegmentInfo, r.md.Segs)
	for i := range segs {
		segs[i].Id = key(i, r.id)
	}
	return segs
}

func (r *relation) Index() []*engine.IndexTableDef {
	return nil
}

func (r *relation) Attribute() []metadata.Attribute {
	return r.md.Attrs
}

func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	return segment.New(si.Id, r.db, proc, r.mp)
}

func (r *relation) Write(_ uint64, bat *batch.Batch) error {
	seg := key(int(r.md.Segs), r.id)
	if n := len(bat.Sels); n > 0 {
		if err := r.db.Set(seg, bat.SelsData[:n*8]); err != nil {
			return err
		}
	}
	for i, attr := range bat.Attrs {
		data, err := bat.Vecs[i].Show()
		if err != nil {
			r.clean(seg, bat.Attrs[:i])
			return err
		}
		switch r.mp[attr].Alg {
		case compress.Lz4:
			buf := make([]byte, lz4.CompressBlockBound(len(data)))
			if buf, err = compress.Compress(data, buf, compress.Lz4); err != nil {
				r.clean(seg, bat.Attrs[:i])
				return err
			}
			buf = append(buf, encoding.EncodeInt32(int32(len(data)))...)
			data = buf
		}
		data = append(data, encoding.EncodeUint32(crc32.Checksum(data, crc32.IEEETable))...)
		if err = r.db.Set(seg+"."+attr, data); err != nil {
			r.clean(seg, bat.Attrs[:i])
			return err
		}
	}
	{
		r.md.Segs++
		data, _ := encoding.Encode(r.md)
		if err := r.db.Set(path.Join(r.id, MetaKey), data); err != nil {
			r.clean(seg, bat.Attrs)
			return err
		}
	}
	return nil
}

func (r *relation) AddAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) clean(seg string, attrs []string) {
	r.db.Del(seg)
	for _, attr := range attrs {
		r.db.Del(seg + "." + attr)
	}
}

func key(num int, id string) string {
	return fmt.Sprintf("%v/%v", id, num)
}
