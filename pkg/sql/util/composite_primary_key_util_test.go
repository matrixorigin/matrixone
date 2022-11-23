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
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSimpleCompositePrimaryKey(t *testing.T) {
	tests := []struct {
		args []string
	}{
		{
			args: []string{"a", "b", "c"},
		},
	}
	for _, test := range tests {
		args := test.args
		cKeyName := BuildCompositePrimaryKeyColumnName(args)
		s := SplitCompositePrimaryKeyColumnName(cKeyName)
		require.Equal(t, len(args), len(s))
		for i := range s {
			require.Equal(t, args[i], s[i])
		}
	}
}

func TestFillCompositePKeyBatch(t *testing.T) {
	var proc = testutil.NewProc()
	columnSi := 10
	rowCount := 10
	bat, col, valueCount := MakeBatch(columnSi, rowCount, proc.Mp())
	err := FillCompositePKeyBatch(bat, col, proc)
	require.Equal(t, err, nil)
	bs := vector.GetBytesVectorValues(bat.Vecs[len(bat.Vecs)-1])
	tuples := make([]types.Tuple, 0)
	for i := 0; i < len(bs); i++ {
		tuple, err := types.Unpack(bs[i])
		require.Equal(t, err, nil)
		tuples = append(tuples, tuple)
	}
	names := SplitCompositePrimaryKeyColumnName(col.Name)
	for i := 0; i < rowCount*len(names); i++ {
		num, _ := strconv.Atoi(names[i/rowCount])
		require.Equal(t, tuples[i%rowCount][i/rowCount], valueCount[num*rowCount+i%rowCount])
	}
}

func MakeBatch(columnSi int, rowCount int, mp *mpool.MPool) (*batch.Batch, *plan.ColDef, map[int]interface{}) {
	idx := columnSi
	attrs := make([]string, 0, idx)

	for i := 0; i < idx; i++ {
		attrs = append(attrs, strconv.Itoa(i))
	}

	var keys []string
	for i := 0; i < idx; i++ {
		x := rand.Intn(2)
		if x == 0 {
			keys = append(keys, strconv.Itoa(i))
		}
	}
	cPkeyName := BuildCompositePrimaryKeyColumnName(keys)
	attrs = append(attrs, cPkeyName)

	bat := batch.New(true, attrs)

	valueCount := make(map[int]interface{})

	for i := 0; i < idx; i++ {
		bat.Vecs[i] = vector.New(types.Type{Oid: randType()})
		randInsertValues(bat.Vecs[i], bat.Vecs[i].Typ.Oid, rowCount, valueCount, i*rowCount, mp)
	}

	bat.Vecs[idx] = vector.New(types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen})

	colDef := &plan.ColDef{
		Name: cPkeyName,
	}
	return bat, colDef, valueCount
}

func randType() types.T {
	t := rand.Intn(17)
	var vt types.T
	switch t {
	case 0:
		vt = types.T_bool
	case 1:
		vt = types.T_int8
	case 2:
		vt = types.T_int16
	case 3:
		vt = types.T_int32
	case 4:
		vt = types.T_int64
	case 5:
		vt = types.T_uint8
	case 6:
		vt = types.T_uint16
	case 7:
		vt = types.T_uint32
	case 8:
		vt = types.T_uint64
	case 9:
		vt = types.T_date
	case 10:
		vt = types.T_datetime
	case 11:
		vt = types.T_timestamp
	case 12:
		vt = types.T_float32
	case 13:
		vt = types.T_float64
	case 14:
		vt = types.T_decimal64
	case 15:
		vt = types.T_decimal128
	case 16:
		vt = types.T_varchar
	}
	return vt
}

func randInsertValues(v *vector.Vector, t types.T, rowCount int, valueCount map[int]interface{}, valueBegin int, mp *mpool.MPool) {
	switch t {
	case types.T_bool:
		vs := make([]bool, rowCount)
		for i := 0; i < rowCount; i++ {
			if i < rowCount/2 {
				vs[i] = true
				valueCount[valueBegin+i] = true
			} else {
				vs[i] = false
				valueCount[valueBegin+i] = false
			}
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_int8:
		vs := make([]int8, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randPositiveInt8()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_int16:
		vs := make([]int16, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randPositiveInt16()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_int32:
		vs := make([]int32, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randPositiveInt32()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_int64:
		vs := make([]int64, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randPositiveInt64()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_uint8:
		vs := make([]uint8, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randUint8()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_uint16:
		vs := make([]uint16, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randUint16()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_uint32:
		vs := make([]uint32, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randUint32()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_uint64:
		vs := make([]uint64, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randUint64()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_date:
		vs := make([]types.Date, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randDate()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_time:
		vs := make([]types.Time, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randTime()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_datetime:
		vs := make([]types.Datetime, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randDatetime()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_timestamp:
		vs := make([]types.Timestamp, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randTimestamp()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_float32:
		vs := make([]float32, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = rand.Float32()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_float64:
		vs := make([]float64, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = rand.Float64()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_decimal64:
		vs := make([]types.Decimal64, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randDecimal64()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_decimal128:
		vs := make([]types.Decimal128, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randDecimal128()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendFixed(v, vs, mp)
	case types.T_varchar:
		vs := make([][]byte, rowCount)
		for i := 0; i < rowCount; i++ {
			vs[i] = randStringType()
			valueCount[valueBegin+i] = vs[i]
		}
		vector.AppendBytes(v, vs, mp)
	}

}

func randPositiveInt8() int8 {
	return int8(rand.Int31n(math.MaxInt8 + 1))
}

func randPositiveInt16() int16 {
	return int16(rand.Int31n(math.MaxInt16 + 1))
}

func randPositiveInt32() int32 {
	return rand.Int31()
}

func randPositiveInt64() int64 {
	return rand.Int63()
}

func randUint8() uint8 {
	return uint8(rand.Int31n(math.MaxUint8 + 1))
}

func randUint16() uint16 {
	return uint16(rand.Int31n(math.MaxUint16 + 1))
}

func randUint32() uint32 {
	return rand.Uint32()
}

func randUint64() uint64 {
	return rand.Uint64()
}

func randDate() types.Date {
	year := rand.Intn(types.MaxDateYear) + types.MinDateYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(types.LastDay(int32(year), uint8(month)))) + 1
	return types.FromCalendar(int32(year), uint8(month), uint8(day))
}

func randTime() types.Time {
	isNeg := false
	if tmp := rand.Intn(2); tmp == 0 {
		isNeg = true
	}
	hour := rand.Intn(2562047788)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microSecond := rand.Intn(1e6)
	return types.FromTimeClock(isNeg, uint64(hour), uint8(minute), uint8(second), uint32(microSecond))
}

func randDatetime() types.Datetime {
	year := rand.Intn(types.MaxDatetimeYear) + types.MinDatetimeYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(types.LastDay(int32(year), uint8(month)))) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microSecond := rand.Intn(1e6)
	return types.FromClock(int32(year), uint8(month), uint8(day), uint8(hour), uint8(minute), uint8(second), uint32(microSecond))
}

func randTimestamp() types.Timestamp {
	year := rand.Intn(types.MaxDatetimeYear) + types.MinDatetimeYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(types.LastDay(int32(year), uint8(month)))) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microSecond := rand.Intn(1e6)
	return types.FromClockUTC(int32(year), uint8(month), uint8(day), uint8(hour), uint8(minute), uint8(second), uint32(microSecond))
}

func randDecimal64() types.Decimal64 {
	decimal, _ := types.Decimal64FromFloat64(rand.Float64(), 20, 10)
	return decimal
}

func randDecimal128() types.Decimal128 {
	decimal, _ := types.Decimal128FromFloat64(rand.Float64(), 20, 10)
	return decimal
}

func randStringType() []byte {
	b := make([]byte, 1024)
	rand.Read(b)
	return b
}
