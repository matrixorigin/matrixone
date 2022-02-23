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

package tuplecodec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"github.com/smartystreets/goconvey/convey"
	"math"
	"testing"
)

func TestBatchAdapter_ForEach(t *testing.T) {
	convey.Convey("for each",t, func() {
		names,attrs := makeAttributes(
			types.T_int8,
			types.T_int16,
			types.T_int32,
			types.T_int64,
			types.T_uint8,
			types.T_uint16,
			types.T_uint32,
			types.T_uint64,
			types.T_float32,
			types.T_float64,
			types.T_char, types.T_varchar,
			types.T_date,
			types.T_datetime)

		cnt := 10

		bat := makeBatch(cnt,names,attrs)

		lines := randomLines(cnt,names,attrs)

		fillBatch(lines,bat)

		ba := NewBatchAdapter(bat)

		rowIdx := 0
		callback := func(callbackCtx interface{}, tuple Tuple) error {
			colcnt,_ := tuple.GetAttributeCount()
			for i := uint32(0); i < colcnt; i++ {
				v,_ := tuple.GetValue(i)
				tv := fmt.Sprintf("%v",v)
				typ, _, err := tuple.GetAttribute(i)
				if err != nil {
					return err
				}
				if typ.Oid == types.T_char ||
					typ.Oid == types.T_varchar {
					s := v.([]uint8)
					tv = string(s)
				}
				field := lines[rowIdx][i]
				if tv != field {
					return fmt.Errorf("wrong value")
				}
			}
			rowIdx++
			return nil
		}
		err := ba.ForEach(nil, callback)
		convey.So(err,convey.ShouldBeNil)
	})
}

func TestRowColumnConverterImpl_FillBatchFromDecodedIndexKey(t *testing.T) {
	convey.Convey("fill batch",t, func() {
		type args struct {
			typ types.T
			valueT orderedcodec.ValueType
			value interface{}
		}

		dateValue,_ := types.ParseDate("2022-02-23")
		datetimeValue,_ := types.ParseDatetime("2022-02-23 00:00:00")

		kases := []args{
			{types.T_int8,        orderedcodec.VALUE_TYPE_INT8,    int8(math.MaxInt8),           },
			{types.T_int16,       orderedcodec.VALUE_TYPE_INT16,   int16(math.MaxInt16),         },
			{types.T_int32,       orderedcodec.VALUE_TYPE_INT32,   int32(math.MaxInt32),         },
			{types.T_int64,       orderedcodec.VALUE_TYPE_INT64,   int64(math.MaxInt64),         },
			{types.T_uint8,       orderedcodec.VALUE_TYPE_UINT8,   uint8(math.MaxUint8),         },
			{types.T_uint16,      orderedcodec.VALUE_TYPE_UINT16,  uint16(math.MaxUint16),       },
			{types.T_uint32,      orderedcodec.VALUE_TYPE_UINT32,  uint32(math.MaxUint32),       },
			{types.T_uint64,      orderedcodec.VALUE_TYPE_UINT64,  uint64(math.MaxUint64),       },
			{types.T_float32,     orderedcodec.VALUE_TYPE_FLOAT32, float32(math.MaxFloat32),     },
			{types.T_float64,     orderedcodec.VALUE_TYPE_FLOAT64, float64(math.MaxFloat64),     },
			{types.T_char,        orderedcodec.VALUE_TYPE_BYTES,   []byte("abc"),                },
			{types.T_varchar,     orderedcodec.VALUE_TYPE_STRING,  "abc",                        },
			{types.T_date,        orderedcodec.VALUE_TYPE_DATE,    dateValue,                    },
			{types.T_datetime,    orderedcodec.VALUE_TYPE_DATETIME,datetimeValue,                },
		}

		var types []types.T
		for _, kase := range kases {
			types = append(types,kase.typ)
		}
		names,attrs := makeAttributes(types...)

		cnt := 10

		bat := makeBatch(cnt,names,attrs)

		var iattrs []descriptor.IndexDesc_Attribute
		var wantAttrIDs []int
		var dis []*orderedcodec.DecodedItem

		for i, kase := range kases {
			iattrs = append(iattrs,descriptor.IndexDesc_Attribute{
				Name:      names[i],
				Direction: 0,
				ID: uint32(i),
				Type:      kase.valueT,
			})
			wantAttrIDs = append(wantAttrIDs,i)

			dis = append(dis,&orderedcodec.DecodedItem{
				Value:                    kase.value,
				ValueType:                kase.valueT,
				SectionType:              0,
				OffsetInUndecodedKey:     0,
				BytesCountInUndecodedKey: 0,
			})
		}
		indexDesc  := &descriptor.IndexDesc{Attributes: iattrs}

		rcc := &RowColumnConverterImpl{}
		for i := 0; i < cnt; i++ {
			err := rcc.FillBatchFromDecodedIndexKey(indexDesc,
				0,
				dis,
				wantAttrIDs,
				bat,
				i,
			)
			convey.So(err,convey.ShouldBeNil)
		}

	})
}