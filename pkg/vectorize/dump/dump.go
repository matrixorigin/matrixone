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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strconv"
)

//ignore varchar, char, text

type quotedTp interface {
	types.Timestamp | types.Datetime | types.Date | types.Decimal | string
}

type transTp interface {
	[]byte | quotedTp
}

func ParseBool(xs []bool, nsp *nulls.Nulls, rs []string) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		rs[i] = strconv.FormatBool(xs[i])
	}
	return rs, nil
}

func ParseSigned[T types.Ints](xs []T, nsp *nulls.Nulls, rs []string) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		rs[i] = strconv.FormatInt(int64(xs[i]), 10)
	}
	return rs, nil
}
func ParseUnsigned[T types.UInts](xs []T, nsp *nulls.Nulls, rs []string) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		rs[i] = strconv.FormatUint(uint64(xs[i]), 10)
	}
	return rs, nil
}

func ParseFloats[T types.Floats](xs []T, nsp *nulls.Nulls, rs []string, bitsSize int) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		rs[i] = strconv.FormatFloat(float64(xs[i]), 'f', -1, bitsSize) //TODO: precision
	}
	return rs, nil
}

func ParseQuoted[T transTp](xs []T, nsp *nulls.Nulls, rs []string, fn func(dt T) string) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		v := fn(xs[i])
		rs[i] = v
	}
	return rs, nil
}

func DefaultParser[T quotedTp](t T) string {
	ret := fmt.Sprint("'", t, "'")
	return ret
}
func JsonParser(dt []byte) string {
	json := types.DecodeJson(dt)
	return "'" + json.String() + "'"
}

//func ParseDate(xs []types.Date, nsp *nulls.Nulls, rs []string) ([]string, error) {
//	for i := range xs {
//		if nsp.Contains(uint64(i)) {
//			rs[i] = "NULL"
//			continue
//		}
//		rs[i] = xs[i].String()
//	}
//	return rs, nil
//}

//func ParseDateTime(xs []types.Datetime, nsp *nulls.Nulls, precision int32, rs []types.Datetime) ([]types.Datetime, error) {
//	for i := range xs {
//		if !nsp.Contains(uint64(i)) {
//			field := xs[i]
//			d, err := types.ParseDatetime(field, precision)
//			if err != nil {
//				return nil, moerr.NewInternalError("the input value '%v' is not datetime type for row %d", field, i)
//			}
//			rs[i] = d
//		}
//	}
//	return rs, nil
//}
//
//func ParseTimeStamp(xs []types.Timestamp, nsp *nulls.Nulls, precision int32, rs []types.Timestamp) ([]types.Timestamp, error) {
//	for i := range xs {
//		if !nsp.Contains(uint64(i)) {
//			field := xs[i]
//			d, err := types.ParseTimestamp(time.UTC, field, precision)
//			if err != nil {
//				return nil, moerr.NewInternalError("the input value '%v' is not timestamp type for row %d", field, i)
//			}
//			rs[i] = d
//		}
//	}
//	return rs, nil
//}
