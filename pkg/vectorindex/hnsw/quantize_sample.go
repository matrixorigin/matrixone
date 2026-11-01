// Copyright 2026 Matrix Origin
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

package hnsw

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// quantizeSampleElements is the number of vector elements, not vectors, an int8/uint8 index
// trains its range from: quantizer.TrainInt8 samples at most this many values.
const quantizeSampleElements = 2_000_000

// SampleQuantizeBounds trains the range an int8/uint8 index maps its vectors with from a random
// sample of the source column, taken as ivfflat samples its training set: a SAMPLE rate of the
// wanted rows over tblcfg.DataSize, the column's vector count. Unset when the column has no
// vector.
func SampleQuantizeBounds(sqlproc *sqlexec.SqlProcess, tblcfg vectorindex.IndexTableConfig, dimension int) (QuantizeBounds, error) {
	nsample := max(quantizeSampleElements/max(dimension, 1), 1)
	ratio := 1.0
	if tblcfg.DataSize > 0 {
		ratio = min(float64(nsample)/float64(tblcfg.DataSize), 1.0)
	}
	sql := fmt.Sprintf("SELECT SAMPLE(%s, %f PERCENT) FROM %s WHERE %s IS NOT NULL LIMIT %d",
		sqlquote.Ident(tblcfg.KeyPart), ratio*100, sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.SrcTable),
		sqlquote.Ident(tblcfg.KeyPart), nsample)
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return QuantizeBounds{}, err
	}
	defer res.Close()

	var sample [][]float32
	for _, bat := range res.Batches {
		if bat == nil || len(bat.Vecs) == 0 {
			continue
		}
		vec := bat.Vecs[0]
		for i := 0; i < bat.RowCount(); i++ {
			if vec.IsNull(uint64(i)) {
				continue
			}
			sample = append(sample, sampleToFloat32(vec, i))
		}
	}
	if len(sample) == 0 {
		return QuantizeBounds{}, nil
	}
	return TrainQuantizeBounds(sample), nil
}

func sampleToFloat32(vec *vector.Vector, i int) []float32 {
	data := vec.GetBytesAt(i)
	if f32, narrow := BytesToFloat32(int32(vec.GetType().Oid), data); narrow {
		return f32
	}
	if vec.GetType().Oid == types.T_array_float64 {
		f64 := types.BytesToArray[float64](data)
		out := make([]float32, len(f64))
		for j, x := range f64 {
			out[j] = float32(x)
		}
		return out
	}
	return append([]float32(nil), types.BytesToArray[float32](data)...)
}
