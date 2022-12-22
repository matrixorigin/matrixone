package json_unquote

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func StringSingle(v []byte) (string, error) {
	bj, err := types.ParseSliceToByteJson(v)
	if err != nil {
		return "", err
	}
	return bj.Unquote()
}
func JsonSingle(v []byte) (string, error) {
	bj := types.DecodeJson(v)
	return bj.Unquote()
}

func StringBatch(xs [][]byte, rs []string, nsp *nulls.Nulls) ([]string, error) {
	for i, v := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		bj, err := types.ParseSliceToByteJson(v)
		if err != nil {
			return nil, err
		}
		r, err := bj.Unquote()
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return rs, nil
}

func JsonBatch(xs [][]byte, rs []string, nsp *nulls.Nulls) ([]string, error) {
	for i, v := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		bj := types.DecodeJson(v)
		r, err := bj.Unquote()
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return rs, nil
}
