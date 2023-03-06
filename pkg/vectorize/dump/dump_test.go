// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dump

import (
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type Kase struct {
	tp types.Type
	xs []string
	ns *nulls.Nulls
}

var (
	cases []*Kase
	typs  = []types.Type{
		types.T_bool.ToType(),
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_decimal64.ToType(),
		types.T_decimal128.ToType(),
		types.T_date.ToType(),
		types.T_time.ToType(),
		types.T_datetime.ToType(),
		types.T_timestamp.ToType(),
		types.T_varchar.ToType(),
		types.T_char.ToType(),
		types.T_json.ToType(),
		types.T_uuid.ToType(),
	}
	vals = [][]string{
		{"true", "false"},
		{"1", "127", "-128"},
		{"1", "32767", "-32768"},
		{"1", "2147483647", "-2147483648"},
		{"1", "9223372036854775807", "-9223372036854775808"},
		{"1", "255"},
		{"1", "65535"},
		{"1", "4294967295"},
		{"1", "18446744073709551615"},
		{"1.1", "3.412"},
		{"1.1", "3.412"},
		{"1.1", "2.2", "3.3", "4.4", "5.5"},
		{"1.1", "2.2", "3.3", "4.4", "5.5"},
		{"2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"},
		{"2021-01-01 00:00:00", "2021-01-02 00:00:00", "2021-01-03 00:00:00", "2021-01-04 00:00:00", "2021-01-05 00:00:00"},
		{"2021-01-01 00:00:00", "2021-01-02 00:00:00", "2021-01-03 00:00:00", "2021-01-04 00:00:00", "2021-01-05 00:00:00"},
		{"2021-01-01 00:00:00", "2021-01-02 00:00:00", "2021-01-03 00:00:00", "2021-01-04 00:00:00", "2021-01-05 00:00:00"},
		{"xsxs", "xsxwda", "dafafef", "fefefqw", "adeqf"},
		{"xsxs", "xsxwda", "dafafef", "fefefqw", "adeqf"},
		{"{\"a\":1}", "{\"a\":2}", "{\"a\":3}", "{\"a\":4}", "{\"a\":5}"},
		{"00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000003", "00000000-0000-0000-0000-000000000004"},
	}
)

func newTestCase(tp types.Type, xs []string) *Kase {
	return &Kase{
		tp: tp,
		xs: xs,
	}
}

func init() {
	for i, typ := range typs {
		cases = append(cases, newTestCase(typ, vals[i]))
	}
}

func TestParser(t *testing.T) {
	for _, kase := range cases {
		rs := make([]string, len(kase.xs)+1)
		var err error
		switch kase.tp.Oid {
		case types.T_bool:
			xs := make([]bool, len(kase.xs))
			for i, x := range kase.xs {
				xs[i] = x == "true"
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseBool(xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatBool(xs[i]), rs[i])
			}
		case types.T_int8:
			xs := make([]int8, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseInt(x, 10, 8)
				require.Nil(t, err)
				xs[i] = int8(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseSigned[int8](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatInt(int64(xs[i]), 10), rs[i])
			}
		case types.T_int16:
			xs := make([]int16, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseInt(x, 10, 16)
				require.Nil(t, err)
				xs[i] = int16(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseSigned[int16](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatInt(int64(xs[i]), 10), rs[i])
			}
		case types.T_int32:
			xs := make([]int32, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseInt(x, 10, 32)
				require.Nil(t, err)
				xs[i] = int32(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseSigned[int32](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatInt(int64(xs[i]), 10), rs[i])
			}
		case types.T_int64:
			xs := make([]int64, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseInt(x, 10, 64)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseSigned[int64](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatInt(xs[i], 10), rs[i])
			}
		case types.T_uint8:
			xs := make([]uint8, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseUint(x, 10, 8)
				require.Nil(t, err)
				xs[i] = uint8(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseUnsigned[uint8](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatUint(uint64(xs[i]), 10), rs[i])
			}
		case types.T_uint16:
			xs := make([]uint16, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseUint(x, 10, 16)
				require.Nil(t, err)
				xs[i] = uint16(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseUnsigned[uint16](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatUint(uint64(xs[i]), 10), rs[i])
			}
		case types.T_uint32:
			xs := make([]uint32, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseUint(x, 10, 32)
				require.Nil(t, err)
				xs[i] = uint32(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseUnsigned[uint32](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatUint(uint64(xs[i]), 10), rs[i])
			}
		case types.T_uint64:
			xs := make([]uint64, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseUint(x, 10, 64)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseUnsigned[uint64](xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatUint(uint64(xs[i]), 10), rs[i])
			}
		case types.T_float32:
			xs := make([]float32, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseFloat(x, 32)
				require.Nil(t, err)
				xs[i] = float32(tmp)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseFloats[float32](xs, kase.ns, rs, 32)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatFloat(float64(xs[i]), 'f', -1, 32), rs[i])
			}
		case types.T_float64:
			xs := make([]float64, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := strconv.ParseFloat(x, 64)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseFloats[float64](xs, kase.ns, rs, 64)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				require.Equal(t, strconv.FormatFloat(xs[i], 'f', -1, 64), rs[i])
			}
		case types.T_decimal64:
			xs := make([]types.Decimal64, len(kase.xs))
			for i, x := range kase.xs {
				xs[i] = types.MustDecimal64FromString(x)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[types.Decimal64](xs, kase.ns, rs, DefaultParser[types.Decimal64])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String(), unquote)
			}
		case types.T_decimal128:
			xs := make([]types.Decimal128, len(kase.xs))
			for i, x := range kase.xs {
				xs[i] = types.MustDecimal128FromString(x)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[types.Decimal128](xs, kase.ns, rs, DefaultParser[types.Decimal128])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String(), unquote)
			}
		case types.T_date:
			xs := make([]types.Date, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseDateCast(x)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[types.Date](xs, kase.ns, rs, DefaultParser[types.Date])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String(), unquote)
			}
		case types.T_time:
			xs := make([]types.Time, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseTime(x, kase.tp.Scale)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[types.Time](xs, kase.ns, rs, DefaultParser[types.Time])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String(), unquote)
			}
		case types.T_datetime:
			xs := make([]types.Datetime, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseDatetime(x, kase.tp.Scale)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[types.Datetime](xs, kase.ns, rs, DefaultParser[types.Datetime])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String(), unquote)
			}
		case types.T_timestamp:
			xs := make([]types.Timestamp, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseTimestamp(time.Local, x, kase.tp.Scale)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseTimeStamp(xs, kase.ns, rs, time.Local, kase.tp.Scale)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].String2(time.Local, kase.tp.Scale), unquote)
			}

		case types.T_varchar, types.T_char, types.T_blob:
			xs := make([]string, len(kase.xs))
			copy(xs, kase.xs)
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[string](xs, kase.ns, rs, DefaultParser[string])
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i], unquote)
			}
		case types.T_json:
			xs := make([][]byte, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseStringToByteJson(x)
				require.Nil(t, err)
				xs[i], err = tmp.Marshal()
				require.Nil(t, err)
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseQuoted[[]byte](xs, kase.ns, rs, JsonParser)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.JSONEq(t, kase.xs[i], unquote)
			}
		case types.T_uuid:
			xs := make([]types.Uuid, len(kase.xs))
			for i, x := range kase.xs {
				tmp, err := types.ParseUuid(x)
				require.Nil(t, err)
				xs[i] = tmp
			}
			xs = append(xs, xs[0])
			kase.ns = nulls.NewWithSize(len(xs))
			kase.ns.Set(uint64(len(xs) - 1))
			rs, err = ParseUuid(xs, kase.ns, rs)
			require.Nil(t, err)
			require.Equal(t, rs[len(rs)-1], "NULL")
			for i := 0; i < len(xs)-1; i++ {
				unquote := rs[i][1 : len(rs[i])-1]
				require.Equal(t, xs[i].ToString(), unquote)
			}

		default:
			require.Fail(t, "unsupported type")
		}
	}
}
