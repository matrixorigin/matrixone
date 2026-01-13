// Copyright 2022 Matrix Origin
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

package readutil

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

/* DONT remove me, will be used later

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// Either len(val) == 0 or vec == nil
// inVec should be sorted
func CompilePrimaryKeyEqualFilter(
	ctx context.Context,
	val []byte,
	inVec *vector.Vector,
	colSeqnum uint16,
	isFakePK bool,
	skipBloomFilter bool,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	err error,
) {
	if skipBloomFilter {
		loadOp = loadMetadataOnlyOpFactory(fs)
	} else {
		loadOp = loadMetadataAndBFOpFactory(fs)
	}

	// Here process the pk in filter
	if inVec != nil {
		if !isFakePK {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().AnyIn(inVec), nil
			}
		}

		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if !isFakePK {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(colSeqnum).ZoneMap().AnyIn(inVec), nil
		}

		blockFilterOp = func(
			blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			// TODO: support skipFollowing
			zm := blkMeta.MustGetColumn(colSeqnum).ZoneMap()
			if !zm.AnyIn(inVec) {
				return false, false, nil
			}

			if skipBloomFilter || bf.Size() == 0 {
				return false, true, nil
			}

			buf := bf.GetBloomFilter(uint32(blkIdx))
			var blkBF index.BloomFilter
			if err := blkBF.Unmarshal(buf); err != nil {
				return false, false, err
			}
			lb, ub := zm.SubVecIn(inVec)
			if exist := blkBF.MayContainsAny(
				inVec, lb, ub,
			); !exist {
				return false, false, nil
			}
			return false, true, nil
		}

		return
	}

	// Here process the pk equal filter

	// for non-fake PK, we can use the object stats sort key zone map
	// to filter as the fastFilterOp
	if !isFakePK {
		fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
			if obj.ZMIsEmpty() {
				return true, nil
			}
			return obj.SortKeyZoneMap().ContainsKey(val), nil
		}
	}

	objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
		if !isFakePK {
			return true, nil
		}
		dataMeta := meta.MustDataMeta()
		return dataMeta.MustGetColumn(colSeqnum).ZoneMap().ContainsKey(val), nil
	}

	blockFilterOp = func(
		blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, bool, error) {
		var (
			skipFollowing, ok bool
		)
		zm := blkMeta.MustGetColumn(colSeqnum).ZoneMap()
		if isFakePK {
			skipFollowing = false
			ok = zm.ContainsKey(val)
		} else {
			skipFollowing = !zm.AnyLEByValue(val)
			if skipFollowing {
				ok = false
			} else {
				ok = zm.ContainsKey(val)
			}
		}
		if !ok || skipBloomFilter || bf.Size() == 0 {
			return skipFollowing, ok, nil
		}

		// check bloom filter here
		blkBFBuf := bf.GetBloomFilter(uint32(blkIdx))
		var blkBF index.BloomFilter
		if err := blkBF.Unmarshal(blkBFBuf); err != nil {
			return false, false, err
		}
		exist, err := blkBF.MayContainsKey(val)
		if err != nil || !exist {
			return false, false, err
		}

		return false, true, nil
	}
	if !isFakePK {
		seekOp = func(meta objectio.ObjectDataMeta) int {
			blockCnt := int(meta.BlockCount())
			blkIdx := sort.Search(blockCnt, func(i int) bool {
				return meta.GetBlockMeta(uint32(i)).MustGetColumn(colSeqnum).ZoneMap().AnyGEByValue(val)
			})
			return blkIdx
		}
	}
	return
}
*/

func EncodePrimaryKey(v any, packer *types.Packer) []byte {
	packer.Reset()

	switch v := v.(type) {

	case bool:
		packer.EncodeBool(v)

	case int8:
		packer.EncodeInt8(v)

	case int16:
		packer.EncodeInt16(v)

	case int32:
		packer.EncodeInt32(v)

	case int64:
		packer.EncodeInt64(v)

	case uint8:
		packer.EncodeUint8(v)

	case uint16:
		packer.EncodeUint16(v)

	case uint32:
		packer.EncodeUint32(v)

	case uint64:
		packer.EncodeUint64(v)

	case float32:
		packer.EncodeFloat32(v)

	case float64:
		packer.EncodeFloat64(v)

	case types.Date:
		packer.EncodeDate(v)

	case types.Time:
		packer.EncodeTime(v)

	case types.Datetime:
		packer.EncodeDatetime(v)

	case types.Timestamp:
		packer.EncodeTimestamp(v)

	case types.Decimal64:
		packer.EncodeDecimal64(v)

	case types.Decimal128:
		packer.EncodeDecimal128(v)

	case types.Uuid:
		packer.EncodeStringType(v[:])

	case types.Enum:
		packer.EncodeEnum(v)

	case types.MoYear:
		packer.EncodeMoYear(v)

	case string:
		packer.EncodeStringType([]byte(v))

	case []byte:
		packer.EncodeStringType(v)

	default:
		panic(fmt.Sprintf("unknown type: %T", v))

	}

	return packer.Bytes()
}

func EncodePrimaryKeyVector(vec *vector.Vector, packer *types.Packer) (ret [][]byte) {
	packer.Reset()

	if vec.IsConstNull() {
		return make([][]byte, vec.Length())
	}

	switch vec.GetType().Oid {

	case types.T_bool:
		s := vector.MustFixedColNoTypeCheck[bool](vec)
		for _, v := range s {
			packer.EncodeBool(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_bit:
		s := vector.MustFixedColNoTypeCheck[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int8:
		s := vector.MustFixedColNoTypeCheck[int8](vec)
		for _, v := range s {
			packer.EncodeInt8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int16:
		s := vector.MustFixedColNoTypeCheck[int16](vec)
		for _, v := range s {
			packer.EncodeInt16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int32:
		s := vector.MustFixedColNoTypeCheck[int32](vec)
		for _, v := range s {
			packer.EncodeInt32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int64:
		s := vector.MustFixedColNoTypeCheck[int64](vec)
		for _, v := range s {
			packer.EncodeInt64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint8:
		s := vector.MustFixedColNoTypeCheck[uint8](vec)
		for _, v := range s {
			packer.EncodeUint8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint16:
		s := vector.MustFixedColNoTypeCheck[uint16](vec)
		for _, v := range s {
			packer.EncodeUint16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint32:
		s := vector.MustFixedColNoTypeCheck[uint32](vec)
		for _, v := range s {
			packer.EncodeUint32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint64:
		s := vector.MustFixedColNoTypeCheck[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float32:
		s := vector.MustFixedColNoTypeCheck[float32](vec)
		for _, v := range s {
			packer.EncodeFloat32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float64:
		s := vector.MustFixedColNoTypeCheck[float64](vec)
		for _, v := range s {
			packer.EncodeFloat64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_date:
		s := vector.MustFixedColNoTypeCheck[types.Date](vec)
		for _, v := range s {
			packer.EncodeDate(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_time:
		s := vector.MustFixedColNoTypeCheck[types.Time](vec)
		for _, v := range s {
			packer.EncodeTime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_datetime:
		s := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		for _, v := range s {
			packer.EncodeDatetime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_timestamp:
		s := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		for _, v := range s {
			packer.EncodeTimestamp(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_enum:
		s := vector.MustFixedColNoTypeCheck[types.Enum](vec)
		for _, v := range s {
			packer.EncodeEnum(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_year:
		s := vector.MustFixedColNoTypeCheck[types.MoYear](vec)
		for _, v := range s {
			packer.EncodeMoYear(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal64:
		s := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		for _, v := range s {
			packer.EncodeDecimal64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal128:
		s := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		for _, v := range s {
			packer.EncodeDecimal128(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uuid:
		s := vector.MustFixedColNoTypeCheck[types.Uuid](vec)
		for _, v := range s {
			packer.EncodeStringType(v[:])
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_Rowid,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		for i := 0; i < vec.Length(); i++ {
			packer.EncodeStringType(vec.GetBytesAt(i))
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}
	default:
		panic(fmt.Sprintf("unknown type: %v", vec.GetType().String()))

	}

	l := vec.Length()

	if vec.IsConst() {
		for len(ret) < l {
			ret = append(ret, ret[0])
		}
	}

	if len(ret) != l {
		panic(fmt.Sprintf("bad result, expecting %v, got %v", l, len(ret)))
	}

	return
}
