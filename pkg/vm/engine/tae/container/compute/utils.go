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

package compute

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

func Hash(v any, typ types.Type) (uint64, error) {
	data, err := EncodeKey(v, typ)
	if err != nil {
		return 0, err
	}
	//murmur := murmur3.Sum64(data)
	xx := xxhash.Sum64(data)
	return xx, nil
}

func DecodeKey(key []byte, typ types.Type) any {
	switch typ.Oid {
	case types.T_int8:
		return encoding.DecodeInt8(key)
	case types.T_int16:
		return encoding.DecodeInt16(key)
	case types.T_int32:
		return encoding.DecodeInt32(key)
	case types.T_int64:
		return encoding.DecodeInt64(key)
	case types.T_uint8:
		return encoding.DecodeUint8(key)
	case types.T_uint16:
		return encoding.DecodeUint16(key)
	case types.T_uint32:
		return encoding.DecodeUint32(key)
	case types.T_uint64:
		return encoding.DecodeUint64(key)
	case types.T_float32:
		return encoding.DecodeFloat32(key)
	case types.T_float64:
		return encoding.DecodeFloat64(key)
	case types.T_date:
		return encoding.DecodeDate(key)
	case types.T_datetime:
		return encoding.DecodeDatetime(key)
	case types.T_char, types.T_varchar:
		return key
	default:
		panic("unsupported type")
	}
}

func EncodeKey(key any, typ types.Type) ([]byte, error) {
	switch typ.Oid {
	case types.T_int8:
		if v, ok := key.(int8); ok {
			return encoding.EncodeInt8(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_int16:
		if v, ok := key.(int16); ok {
			return encoding.EncodeInt16(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_int32:
		if v, ok := key.(int32); ok {
			return encoding.EncodeInt32(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_int64:
		if v, ok := key.(int64); ok {
			return encoding.EncodeInt64(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_uint8:
		if v, ok := key.(uint8); ok {
			return encoding.EncodeUint8(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_uint16:
		if v, ok := key.(uint16); ok {
			return encoding.EncodeUint16(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_uint32:
		if v, ok := key.(uint32); ok {
			return encoding.EncodeUint32(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_uint64:
		if v, ok := key.(uint64); ok {
			return encoding.EncodeUint64(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_decimal64:
		return encoding.EncodeDecimal64(key.(types.Decimal64)), nil
	case types.T_decimal128:
		return encoding.EncodeDecimal128(key.(types.Decimal128)), nil
	case types.T_float32:
		if v, ok := key.(float32); ok {
			return encoding.EncodeFloat32(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_float64:
		if v, ok := key.(float64); ok {
			return encoding.EncodeFloat64(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_date:
		if v, ok := key.(types.Date); ok {
			return encoding.EncodeDate(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_timestamp:
		return encoding.EncodeTimestamp(key.(types.Timestamp)), nil
	case types.T_datetime:
		if v, ok := key.(types.Datetime); ok {
			return encoding.EncodeDatetime(v), nil
		} else {
			panic("unsupported type")
		}
	case types.T_char, types.T_varchar:
		if v, ok := key.([]byte); ok {
			return v, nil
		} else {
			panic("unsupported type")
		}
	default:
		panic("unsupported type")
	}
}

func ProcessVector(vec *vector.Vector, offset uint32, length uint32, task func(v any, pos uint32) error, keyselects *roaring.Bitmap) error {
	var idxes []uint32
	if keyselects != nil {
		idxes = keyselects.ToArray()
	}
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_int16:
		vs := vec.Col.([]int16)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_int32:
		vs := vec.Col.([]int32)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_int64:
		vs := vec.Col.([]int64)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_decimal64:
		vs := vec.Col.([]types.Decimal64)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_decimal128:
		vs := vec.Col.([]types.Decimal128)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_float32:
		vs := vec.Col.([]float32)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_float64:
		vs := vec.Col.([]float64)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_timestamp:
		vs := vec.Col.([]types.Timestamp)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_date:
		vs := vec.Col.([]types.Date)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_datetime:
		vs := vec.Col.([]types.Datetime)[offset:]
		if keyselects == nil {
			for i, v := range vs {
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	case types.T_char, types.T_varchar:
		vs := vec.Col.(*types.Bytes)
		if keyselects == nil {
			for i := range vs.Offsets[offset:] {
				v := vs.Get(int64(i))
				if err := task(v, uint32(i)); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes[offset:] {
				v := vs.Get(int64(idx))
				if err := task(v, idx); err != nil {
					return err
				}
			}
		}
	default:
		panic("unsupported type")
	}
	return nil
}
