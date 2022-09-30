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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/pierrec/lz4/v4"
)

func (r *relation) Rows(context.Context) (int64, error) {
	return r.md.Rows, nil
}

func (*relation) Size(context.Context, string) (int64, error) {
	return 0, nil
}

func (r *relation) Ranges(_ context.Context, _ *plan.Expr) ([][]byte, error) {
	return nil, nil
}

func (r *relation) GetPrimaryKeys(_ context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (r *relation) GetHideKeys(_ context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (r *relation) TableDefs(_ context.Context) ([]engine.TableDef, error) {
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
	return defs, nil
}

func (r *relation) NewReader(_ context.Context, n int, _ *plan.Expr, _ [][]byte) ([]engine.Reader, error) {
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
	return rs, nil
}

func (r *relation) Write(_ context.Context, bat *batch.Batch) error {
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
			lenV := int32(len(v))
			data = append(data, types.EncodeInt32(&lenV)...)
			v = data
		}
		if err := r.db.Set(key+"."+attr, v); err != nil {
			return err
		}
	}
	{
		r.md.Segs++
		data, err := types.Encode(r.md)
		if err != nil {
			return err
		}
		if err := r.db.Set(r.id, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *relation) Delete(_ context.Context, _ *vector.Vector, _ string) error {
	return nil
}

func (r *relation) Update(_ context.Context, bat *batch.Batch) error {
	return nil
}

func (r *relation) Truncate(_ context.Context) (uint64, error) {
	return 0, nil
}

func (r *relation) AddTableDef(_ context.Context, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelTableDef(_ context.Context, _ engine.TableDef) error {
	return nil
}

func (r *relation) GetTableID(_ context.Context) string {
	//TODO
	return "0"
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}
