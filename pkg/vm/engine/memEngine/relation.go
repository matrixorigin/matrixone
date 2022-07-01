package memEngine

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/pierrec/lz4"
)

func (r *relation) Close(_ engine.Snapshot) {}

func (r *relation) ID(_ engine.Snapshot) string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (_ *relation) Size(_ string) int64 {
	return 0
}

func (_ *relation) Cardinality(_ string) int64 {
	return 0
}

func (r *relation) Nodes(_ engine.Snapshot) engine.Nodes {
	return engine.Nodes{r.n}
}

func (r *relation) GetPrimaryKeys(_ engine.Snapshot) []*engine.Attribute {
	return nil
}

func (r *relation) Truncate(_ engine.Snapshot) (uint64, error) {
	panic(any("implement me"))
}

func (r *relation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	return nil
}

func (r *relation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	return nil, false
}

func (r *relation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	defs := make([]engine.TableDef, len(r.md.Attrs)+len(r.md.Index))
	for i, attr := range r.md.Attrs {
		defs[i] = &engine.AttributeDef{Attr: attr}
	}
	j := len(r.md.Attrs)
	for _, index := range r.md.Index {
		// Don't refer to enclosing loop variables directly.
		localIndex := index
		defs[j] = &localIndex
		j++
	}
	return defs
}

func (r *relation) NewReader(n int, _ extend.Extend, _ []byte, _ engine.Snapshot) []engine.Reader {
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

func (r *relation) Write(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
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

func (r *relation) Delete(_ uint64, _ *vector.Vector, _ string, _ engine.Snapshot) error {
	return nil
}

func (r *relation) Update(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	return nil
}

func (r *relation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	return nil
}

func (r *relation) DropIndex(epoch uint64, name string) error {
	return nil
}

func (r *relation) AddTableDef(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
	return nil
}

func (r *relation) DelTableDef(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
	return nil
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}
