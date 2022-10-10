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

package external

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
	"strconv"
	"strings"
	"time"
)

func judgeInteger(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] == '-' || field[i] == '+' {
			continue
		}
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

func getNullFlag(list []string, field string) bool {
	for i := 0; i < len(list); i++ {
		field = strings.ToLower(field)
		if list[i] == field {
			return true
		}
	}
	return false
}

func TrimSpace(xs []string) ([]string, error) {
	var err error
	rs := make([]string, len(xs))
	for i := range xs {
		rs[i] = strings.TrimSpace(xs[i])
		if err != nil {
			return nil, err
		}
	}
	return rs, nil
}
func ParseNullFlagNormal(xs, nullList []string) []bool {
	rs := make([]bool, len(xs))
	for i := range xs {
		rs[i] = xs[i] == NULL_FLAG || len(xs[i]) == 0 || getNullFlag(nullList, xs[i])
	}
	return rs
}
func ParseNullFlagStrings(xs, nullList []string) []bool {
	rs := make([]bool, len(xs))
	for i := range xs {
		rs[i] = xs[i] == NULL_FLAG || getNullFlag(nullList, xs[i])
	}
	return rs
}

func InsertNsp(xs []bool, nsp *nulls.Nulls) *nulls.Nulls {
	for i := range xs {
		if xs[i] {
			nsp.Set(uint64(i))
		}
	}
	return nsp
}

func ParseBool(xs []string, nsp *nulls.Nulls, rs []bool) ([]bool, error) {

	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := strings.ToLower(xs[i])
			if xs[i] == "true" || field == "1" {
				rs[i] = true
			} else if field == "false" || field == "0" {
				rs[i] = false
			} else {
				return nil, moerr.NewInternalError("the input value '%s' is not bool type for row %d", field, i)
			}
		}
	}
	return rs, nil
}

func ParseInt8(xs []string, nsp *nulls.Nulls, rs []int8) ([]int8, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseInt(field, 10, 8)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int8 type for row %d", field, i)
				}
				rs[i] = int8(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
					return nil, moerr.NewInternalError("the input value '%v' is not int8 type for row %d", field, i)
				}
				rs[i] = int8(d)
			}
		}
	}
	return rs, nil
}

func ParseInt16(xs []string, nsp *nulls.Nulls, rs []int16) ([]int16, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseInt(field, 10, 16)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int16(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int16(d)
			}
		}
	}
	return rs, nil
}
func ParseInt32(xs []string, nsp *nulls.Nulls, rs []int32) ([]int32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseInt(field, 10, 32)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int32(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int32(d)
			}
		}
	}
	return rs, nil
}

func ParseInt64(xs []string, nsp *nulls.Nulls, rs []int64) ([]int64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseInt(field, 10, 64)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = d
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int64(d)
			}
		}
	}
	return rs, nil
}

func ParseUint8(xs []string, nsp *nulls.Nulls, rs []uint8) ([]uint8, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseUint(field, 10, 8)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint8 type for row %d", field, i)
				}
				rs[i] = uint8(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint8 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint8 type for row %d", field, i)
				}
				rs[i] = uint8(d)
			}
		}
	}
	return rs, nil
}

func ParseUint16(xs []string, nsp *nulls.Nulls, rs []uint16) ([]uint16, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseUint(field, 10, 16)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint16 type for row %d", field, i)
				}
				rs[i] = uint16(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint16 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint16 type for row %d", field, i)
				}
				rs[i] = uint16(d)
			}
		}
	}
	return rs, nil
}

func ParseUint32(xs []string, nsp *nulls.Nulls, rs []uint32) ([]uint32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseUint(field, 10, 32)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint32 type for row %d", field, i)
				}
				rs[i] = uint32(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint32 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint32 type for row %d", field, i)
				}
				rs[i] = uint32(d)
			}
		}
	}
	return rs, nil
}

func ParseUint64(xs []string, nsp *nulls.Nulls, rs []uint64) ([]uint64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInteger(field) {
				d, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint64 type for row %d", field, i)
				}
				rs[i] = d
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint64 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint64 type for row %d", field, i)
				}
				rs[i] = uint64(d)
			}
		}
	}
	return rs, nil
}

func ParseFloat32(xs []string, nsp *nulls.Nulls, rs []float32) ([]float32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := strconv.ParseFloat(field, 32)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not float32 type for row %d", field, i)
			}
			rs[i] = float32(d)
		}
	}
	return rs, nil
}

func ParseFloat64(xs []string, nsp *nulls.Nulls, rs []float64) ([]float64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not float64 type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func ParseJson(xs []string, nsp *nulls.Nulls, rs [][]byte) ([][]byte, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			byteJson, err := types.ParseStringToByteJson(field)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not json type for row %d", field, i)
			}
			jsonBytes, err := types.EncodeJson(byteJson)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not json type for row %d", field, i)
			}
			rs[i] = jsonBytes
		}
	}
	return rs, nil
}

func ParseDate(xs []string, nsp *nulls.Nulls, rs []types.Date) ([]types.Date, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseDate(field)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not date type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}
func ParseDateTime(xs []string, nsp *nulls.Nulls, precision int32, rs []types.Datetime) ([]types.Datetime, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseDatetime(field, precision)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not datetime type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func ParseTimeStamp(xs []string, nsp *nulls.Nulls, precision int32, rs []types.Timestamp) ([]types.Timestamp, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseTimestamp(time.UTC, field, precision)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not timestamp type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func ParseDecimal64(xs []string, nsp *nulls.Nulls, width, scale int32, rs []types.Decimal64) ([]types.Decimal64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.Decimal64_FromStringWithScale(field, width, scale)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return nil, moerr.NewInternalError("the input value '%v' is invalid Decimal64 type for row %d", field, i)
				}
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func ParseDecimal128(xs []string, nsp *nulls.Nulls, width, scale int32, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.Decimal128_FromStringWithScale(field, width, scale)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return nil, moerr.NewInternalError("the input value '%v' is invalid Decimal128 type for row %d", field, i)
				}
			}
			rs[i] = d
		}
	}
	return rs, nil
}
