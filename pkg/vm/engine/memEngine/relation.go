package memEngine

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/pierrec/lz4"
)

func (r *relation) Close() {}

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (_ *relation) Size(_ string) int64 {
	return 0
}

func (_ *relation) CardinalNumber(_ string) int64 {
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

func (r *relation) NewReader(n int) []engine.Reader {
	segs := make([]string, r.md.Segs)
	for i := range segs {
		segs[i] = sKey(i, r.id)
	}
	attrs := make(map[string]engine.Attribute)
	{
		for i, attr := range r.md.Attrs {
			attrs[attr.Name] = r.md.Attrs[i]
		}
	}
	rs := make([]engine.Reader, n)
	if int64(n) < r.md.Segs {
		step := int(r.md.Segs) / n
		for i := 0; i < n; i++ {
			if i == n-1 {
				rs[i] = &reader{
					db:    r.db,
					attrs: attrs,
					segs:  segs[i*step:],
				}
			} else {
				rs[i] = &reader{
					db:    r.db,
					attrs: attrs,
					segs:  segs[i*step : (i+1)*step],
				}
			}
		}
	} else {
		for i := range segs {
			rs[i] = &reader{
				db:    r.db,
				attrs: attrs,
				segs:  segs[i : i+1],
			}
		}
		for i := len(segs); i < n; i++ {
			rs[i] = &reader{}
		}
	}
	return rs
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
