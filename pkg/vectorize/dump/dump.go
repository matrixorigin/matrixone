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
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type quotedTp interface {
	types.Datetime | types.Time | types.Date | types.Decimal | string
}

type transTp interface {
	[]byte | quotedTp | types.Datetime | types.Time
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
		rs[i] = strconv.FormatFloat(float64(xs[i]), 'f', -1, bitsSize) //TODO: scale
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

func ParseTimeStamp(xs []types.Timestamp, nsp *nulls.Nulls, rs []string, loc *time.Location, scale int32) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		v := fmt.Sprintf("'%s'", xs[i].String2(loc, scale))
		rs[i] = v
	}
	return rs, nil
}
func ParseUuid(xs []types.Uuid, nsp *nulls.Nulls, rs []string) ([]string, error) {
	for i := range xs {
		if nsp.Contains(uint64(i)) {
			rs[i] = "NULL"
			continue
		}
		v := fmt.Sprintf("'%s'", xs[i].ToString())
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
