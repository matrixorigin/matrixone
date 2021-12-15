package block

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
)

func New(id string, db *kv.KV, mp map[string]metadata.Attribute) *Block {
	return &Block{
		id: id,
		db: db,
		mp: mp,
	}
}

func (b *Block) Rows() int64 {
	return 0
}

func (b *Block) Size(_ string) int64 {
	return 0
}

func (b *Block) ID() string {
	return b.id
}

func (_ *Block) Prefetch(_ []string) {}

func (b *Block) Read(cs []uint64, attrs []string, cds, dds []*bytes.Buffer) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	for i, attr := range attrs {
		data, err := b.db.Get(b.id+"."+attr, cds[i])
		if err != nil {
			return nil, err
		}
		md := b.mp[attr]
		if md.Alg == compress.Lz4 {
			dds[i].Reset()
			n := int(encoding.DecodeInt32(data[len(data)-4:]))
			if n > dds[i].Cap() {
				dds[i].Grow(n)
			}
			buf := dds[i].Bytes()[:n]
			if _, err := compress.Decompress(data[:len(data)-4], buf, md.Alg); err != nil {
				return nil, err
			}
			data = buf
		}
		vec := vector.New(md.Type)
		vec.Or = true
		if err := vec.Read(data); err != nil {
			return nil, err
		}
		vec.Ref = cs[i]
		bat.Vecs[i] = vec
	}
	return bat, nil
}
