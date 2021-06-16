package result

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
)

func (rp *Result) FillResult(bat *batch.Batch) error {
	if n := len(bat.Sels); n == 0 {
		n = bat.Vecs[0].Length()
		rows := make([][]interface{}, n)
		for i := 0; i < n; i++ {
			rows[i] = make([]interface{}, len(rp.Attrs))
		}
		for i, vec := range bat.Vecs {
			switch vec.Typ.Oid {
			case types.T_float64:
				vs := vec.Col.([]float64)
				for j := 0; j < n; j++ {
					rows[j][i] = vs[j]
				}
			}
		}
		rp.Rows = rows
	} else {
		rows := make([][]interface{}, n)
		for i := 0; i < n; i++ {
			rows[i] = make([]interface{}, len(rp.Attrs))
		}
		for i, vec := range bat.Vecs {
			switch vec.Typ.Oid {
			case types.T_float64:
				vs := vec.Col.([]float64)
				for j := 0; j < n; j++ {
					rows[j][i] = vs[bat.Sels[j]]
				}
			}
		}
		rp.Rows = rows
	}
	return nil
}
