package hnsw

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestQuantization(t *testing.T) {
	q, err := QuantizationToUsearch(int32(types.T_array_float32))
	require.Nil(t, err)
	require.Equal(t, q, usearch.F32)
	q, err = QuantizationToUsearch(int32(types.T_array_float64))
	require.Nil(t, err)
	require.Equal(t, q, usearch.F64)
	q, err = QuantizationToUsearch(int32(types.T_varchar))
	require.NotNil(t, err)
}
