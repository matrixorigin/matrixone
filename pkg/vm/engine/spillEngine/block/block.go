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

package block

import (
	"errors"
	"hash/crc32"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func New(id string, db *kv.KV, proc *process.Process, mp map[string]metadata.Attribute) *Block {
	return &Block{id, db, proc, mp}
}

func (b *Block) ID() string {
	return b.id
}

func (b *Block) Rows() int64 {
	return -1
}

func (b *Block) Size(attr string) int64 {
	siz, err := b.db.Size(b.id + "." + attr)
	if err != nil {
		return -1
	}
	return siz
}

func (b *Block) Read(siz int64, ref uint64, attr string, proc *process.Process) (*vector.Vector, error) {
	data, err := b.db.Get(b.id+"."+attr, siz, proc)
	if err != nil {
		return nil, err
	}
	md := b.mp[attr]
	vec := vector.New(md.Type)
	switch md.Alg {
	case compress.Lz4:
		var err error

		{
			if sum := crc32.Checksum(data[mempool.CountSize:len(data)-4], crc32.IEEETable); sum != encoding.DecodeUint32(data[len(data)-4:]) {
				proc.Free(data)
				return nil, errors.New("checksum mismatch")
			}
			data = data[:len(data)-4]
		}
		n := int(encoding.DecodeInt32(data[len(data)-4:]))
		buf, err := proc.Alloc(int64(n))
		if err != nil {
			proc.Free(data)
			return nil, err
		}
		tm, err := compress.Decompress(data[mempool.CountSize:len(data)-4], buf[mempool.CountSize:], compress.Lz4)
		if err != nil {
			proc.Free(buf)
			proc.Free(data)
			return nil, err
		}
		buf = buf[:mempool.CountSize+len(tm)]
		proc.Free(data)
		data = buf[:mempool.CountSize+n]
		if err := vec.Read(data); err != nil {
			proc.Free(data)
			return nil, err
		}
	default:
		if crc32.Checksum(data[mempool.CountSize:len(data)-4], crc32.IEEETable) != encoding.DecodeUint32(data[len(data)-4:]) {
			proc.Free(data)
			return nil, errors.New("checksum mismatch")
		}
		if err := vec.Read(data[:len(data)-4]); err != nil {
			proc.Free(data)
			return nil, err
		}
		vec.Data = data
	}
	copy(data, encoding.EncodeUint64(ref))
	return vec, nil
}

func (b *Block) Prefetch(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	bat.Is = make([]batch.Info, len(attrs))
	{
		data, err := b.db.GetCopy(b.id)
		if err != nil {
			return nil, err
		}
		if data != nil {
			buf, err := proc.Alloc(int64(len(data)))
			if err != nil {
				return nil, err
			}
			copy(buf[mempool.CountSize:], data)
			bat.SelsData = buf
			bat.Sels = encoding.DecodeInt64Slice(buf[mempool.CountSize : mempool.CountSize+len(data)])
		}
	}
	for i, attr := range attrs {
		id := b.id + "." + attr
		siz, err := b.db.Size(id)
		if err != nil {
			return nil, err
		}
		if err := b.db.Prefetch(id, siz); err != nil {
			return nil, err
		}
		bat.Is[i].R = b
		bat.Is[i].Len = siz
		bat.Is[i].Ref = cs[i]
	}
	return bat, nil
}
