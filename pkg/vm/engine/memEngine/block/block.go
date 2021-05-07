package block

import (
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/memEngine/kv"
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
	return -1
}

func (b *Block) Read(_ int64, _ uint64, _ string, _ *process.Process) (*vector.Vector, error) {
	return nil, nil
}

func (b *Block) Prefetch(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	for i, attr := range attrs {
		md := b.mp[attr]
		vec, err := b.read(b.id+"."+attr, md.Alg, md.Type, proc)
		if err != nil {
			for j := 0; j < i; j++ {
				copy(bat.Vecs[j].Data, mempool.OneCount)
				bat.Vecs[j].Free(b.proc)
			}
			return nil, err
		}
		copy(vec.Data, encoding.EncodeUint64(cs[i]))
		bat.Vecs[i] = vec
	}
	return bat, nil
}

func (b *Block) read(id string, alg int, typ types.Type, proc *process.Process) (*vector.Vector, error) {
	data, err := b.db.Get(id, proc)
	if err != nil {
		return nil, err
	}
	if alg == compress.Lz4 {
		n := int(encoding.DecodeInt32(data[len(data)-4:]))
		buf, err := proc.Alloc(int64(n))
		if err != nil {
			proc.Free(data)
			return nil, err
		}
		if _, err := compress.Decompress(data[mempool.CountSize:len(data)-4], buf[mempool.CountSize:], alg); err != nil {
			proc.Free(data)
			return nil, err
		}
		proc.Free(data)
		data = buf[:mempool.CountSize+n]
	}
	vec := vector.New(typ)
	if err := vec.Read(data); err != nil {
		proc.Free(data)
		return nil, err
	}
	return vec, nil
}
