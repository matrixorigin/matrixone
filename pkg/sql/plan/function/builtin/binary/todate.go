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

package binary

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ToDate(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if !ivecs[1].IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "the second parameter of function to_date", "not constant")
	}
	rtyp := types.T_date.ToType()
	if ivecs[0].IsConstNull() || ivecs[1].IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}
	ivals := vector.MustStrCol(ivecs[0])
	format := ivecs[1].GetStringAt(0)
	if ivecs[0].IsConst() && ivecs[1].IsConst() {
		result, err := ToDateInputBytes(proc.Ctx, ivals[0], format)
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, result, ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, ivecs[0].Length(), ivecs[0].GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Date](rvec)
		for i := range ivals {
			if rvec.GetNulls().Contains(uint64(i)) {
				continue
			}
			rvals[i], err = ToDateInputBytes(proc.Ctx, ivals[i], format)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	}
}

var otherFormats = map[string]string{
	"MMDDYYYY":        "01022006",
	"DDMMYYYY":        "02012006",
	"MM-DD-YYYY":      "01-02-2006",
	"DD-MM-YYYY":      "02-01-2006",
	"YYYY-MM-DD":      "2006-01-02",
	"YYYYMMDD":        "20060102",
	"YYYYMMDD HHMMSS": "20060102 15:04:05",
}

func ToDateInputBytes(ctx context.Context, input string, format string) (types.Date, error) {
	val := otherFormats[format]
	t, err := time.Parse(val, input)
	if err != nil {
		return 0, err
	}
	return types.DateFromCalendar(int32(t.Year()), uint8(t.Month()), uint8(t.Day())), nil
}
