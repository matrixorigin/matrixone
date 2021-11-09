package memEngine

import (
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mheap"

	"github.com/pierrec/lz4"
)

func (r *relation) Close() {}

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (r *relation) Size(_ string) int64 {
	return 0
}

func (r *relation) Nodes() engine.Nodes {
	return engine.Nodes{r.n}
}

func (r *relation) TableDefs() []engine.TableDef {
	defs := make([]engine.TableDef, len(r.md.Attrs))
	for i, attr := range r.md.Attrs {
		defs[i] = &engine.AttributeDef{Attr: attr}
	}
	return defs
}

func (r *relation) NewReader(_ int, _ *mheap.Mheap) []engine.Reader {
	return nil
}

func (r *relation) Write(_ uint64, bat *batch.Batch) error {
	key := sKey(int(r.md.Segs), r.id)
	for i, attr := range bat.Attrs {
		v, err := bat.Vecs[i].Show()
		if err != nil {
			return err
		}
		if r.md.Attrs[i].Alg == compress.Lz4 {
			data := make([]byte, lz4.CompressBlockBound(len(v)))
			if data, err = compress.Compress(v, data, compress.Lz4); err != nil {
				return err
			}
			data = append(data, encoding.EncodeInt32(int32(len(v)))...)
			v = data
		}
		if err := r.db.Set(key+"."+attr, v); err != nil {
			return err
		}
	}
	{
		r.md.Segs++
		data, err := encoding.Encode(r.md)
		if err != nil {
			return err
		}
		if err := r.db.Set(r.id, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *relation) AddTableDef(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelTableDef(_ uint64, _ engine.TableDef) error {
	return nil
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}
