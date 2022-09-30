package index

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/container/dict"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

var (
	ErrNotSupported = errors.New("the type is not supported for low cardinality index")
)

type LowCardinalityIndex struct {
	typ types.Type

	m    *mheap.Mheap
	dict *dict.Dict
	// poses is the positions of original data in the dictionary.
	// Currently, the type of poses[i] is `T_uint16` which means
	// the max cardinality of LowCardinalityIndex is 65536.
	// The position of `null` value is 0.
	poses *vector.Vector
}

func NewLowCardinalityIndex(typ types.Type, m *mheap.Mheap) (*LowCardinalityIndex, error) {
	// TODO: int8, int16, uint8, uint16
	if typ.Oid == types.T_decimal128 || typ.Oid == types.T_json {
		return nil, ErrNotSupported
	}

	d, err := dict.New(typ, m)
	if err != nil {
		return nil, err
	}
	return &LowCardinalityIndex{
		typ:   typ,
		m:     m,
		dict:  d,
		poses: vector.New(types.T_uint16.ToType()),
	}, nil
}

func (idx *LowCardinalityIndex) GetPoses() *vector.Vector {
	return idx.poses
}

func (idx *LowCardinalityIndex) GetDict() *dict.Dict {
	return idx.dict
}

func (idx *LowCardinalityIndex) Dup() any {
	return &LowCardinalityIndex{
		typ:   idx.typ,
		m:     idx.m,
		dict:  idx.dict.Dup(),
		poses: vector.New(types.T_uint16.ToType()),
	}
}

func (idx *LowCardinalityIndex) InsertBatch(data *vector.Vector) error {
	// TODO: null value
	ips, err := idx.dict.InsertBatch(data)
	if err != nil {
		return err
	}

	us := make([]uint16, len(ips))
	for i, ip := range ips {
		us[i] = uint16(ip)
	}
	return vector.AppendFixed(idx.poses, us, idx.m)
}

// Encode uses the dictionary of the current index to encode the original data.
func (idx *LowCardinalityIndex) Encode(dst, src *vector.Vector) error {
	poses := idx.dict.FindBatch(src)
	col := make([]uint16, len(poses))
	for i, pos := range poses {
		col[i] = uint16(pos)
	}
	return vector.AppendFixed(dst, col, idx.m)
}

func (idx *LowCardinalityIndex) Free() {
	idx.poses.Free(idx.m)
	idx.dict.Free()
}
