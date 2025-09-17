package hnsw

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	usearch "github.com/unum-cloud/usearch/golang"
)

func QuantizationToUsearch(typid int32) (usearch.Quantization, error) {
	switch typid {
	case int32(types.T_array_float32):
		return usearch.F32, nil
	case int32(types.T_array_float64):
		return usearch.F64, nil
	default:
		return usearch.F32, moerr.NewInternalErrorNoCtx(fmt.Sprintf("HNSW Quantization: type id not supported (type id = %d)", typid))
	}
}
