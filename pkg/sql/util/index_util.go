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

package util

import (
	"context"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func BuildIndexTableName(ctx context.Context, unique bool, indexName string) (string, error) {
	var name string
	name = catalog.PrefixIndexTableName
	if unique {
		name += "unique_"
	} else {
		name += "secondary_"
	}
	name += indexName
	id, err := uuid.NewUUID()
	if err != nil {
		return "", moerr.NewInternalError(ctx, "newuuid failed")
	}
	name += "_"
	name += id.String()
	return name, nil
}

func BuildUniqueKeyBatch(vecs []*vector.Vector, attrs []string, p []*plan.ColDef, proc *process.Process) (*batch.Batch, int) {
	b := &batch.Batch{
		// make sure that when batch is cleaned, origin batch is not affected
		Attrs: make([]string, 1),
		Vecs:  make([]*vector.Vector, 1),
	}
	// Judge whether it is a compound unique key
	isCompKey := JudgeIsCompositePrimaryKeyColumn(p[0].Name)
	if isCompKey {
		names := SplitCompositePrimaryKeyColumnName(p[0].Name)
		cPkeyVecMap := make(map[string]*vector.Vector)
		for num, attrName := range attrs {
			for _, name := range names {
				if attrName == name {
					cPkeyVecMap[name] = vecs[num]
				}
			}
		}
		vs := make([]*vector.Vector, 0)
		for _, name := range names {
			v := cPkeyVecMap[name]
			vs = append(vs, v)
		}
		vec, _ := multi.Serial(vs, proc)
		b.Attrs[0] = p[0].Name
		b.Vecs[0] = vec
	} else {
		for i, name := range attrs {
			b.Attrs[0] = p[0].Name
			if p[0].Name == name {
				b.Vecs[0] = vecs[i]
			}
		}
	}
	if isCompKey {
		b.Cnt = 1
	} else {
		v, needClean := compactUniqueKeyBatch(b.Vecs[0], proc)
		if needClean {
			b.Cnt = 1
		} else {
			b.Cnt = 2
		}
		b.Vecs[0] = v
	}
	return b, vector.Length(b.Vecs[0])
}

func compactUniqueKeyBatch(v *vector.Vector, proc *process.Process) (*vector.Vector, bool) {
	if !nulls.Any(v.Nsp) {
		return v, false
	}
	nsp := new(nulls.Nulls)
	var vec *vector.Vector
	length := vector.Length(v)
	switch v.Typ.Oid {
	case types.T_bool:
		s := vector.MustTCols[bool](v)
		ns := make([]bool, 0, length)
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int8:
		s := vector.MustTCols[int8](v)
		ns := make([]int8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int16:
		s := vector.MustTCols[int16](v)
		ns := make([]int16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int32:
		s := vector.MustTCols[int32](v)
		ns := make([]int32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_int64:
		s := vector.MustTCols[int64](v)
		ns := make([]int64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint8:
		s := vector.MustTCols[uint8](v)
		ns := make([]uint8, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint16:
		s := vector.MustTCols[uint16](v)
		ns := make([]uint16, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint32:
		s := vector.MustTCols[uint32](v)
		ns := make([]uint32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_uint64:
		s := vector.MustTCols[uint64](v)
		ns := make([]uint64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float32:
		s := vector.MustTCols[float32](v)
		ns := make([]float32, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_float64:
		s := vector.MustTCols[float64](v)
		ns := make([]float64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_date:
		s := vector.MustTCols[types.Date](v)
		ns := make([]types.Date, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_time:
		s := vector.MustTCols[types.Time](v)
		ns := make([]types.Time, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_datetime:
		s := vector.MustTCols[types.Datetime](v)
		ns := make([]types.Datetime, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_timestamp:
		s := vector.MustTCols[types.Timestamp](v)
		ns := make([]types.Timestamp, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal64:
		s := vector.MustTCols[types.Decimal64](v)
		ns := make([]types.Decimal64, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_decimal128:
		s := vector.MustTCols[types.Decimal128](v)
		ns := make([]types.Decimal128, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	case types.T_json, types.T_char, types.T_varchar, types.T_blob:
		s := vector.GetStrVectorValues(v)
		ns := make([]string, 0, len(s)-nulls.Size(nsp))
		for i, b := range s {
			if !nulls.Contains(v.Nsp, uint64(i)) {
				ns = append(ns, b)
			}
		}
		vec = vector.NewWithFixed(v.Typ, ns, nsp, proc.Mp())
	}
	return vec, true
}
