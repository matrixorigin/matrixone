// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TemporalCastIntegerOverload preserves an explicit temporal CAST's integer
// evaluation policy, separately from storage values and private argument casts.
const TemporalCastIntegerOverload int32 = 9

func NewTemporalCastInteger(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) != 2 {
		return moerr.NewInvalidInputNoCtx("temporal CAST integer conversion requires a value and target")
	}
	switch parameters[0].GetType().Oid {
	case types.T_datetime, types.T_time, types.T_timestamp:
	default:
		return moerr.NewInvalidInputNoCtx("temporal CAST integer conversion requires a temporal source")
	}
	truncate, err := process.ResolveTimeTruncateFractional(proc)
	if err != nil {
		return err
	}
	switch parameters[1].GetType().Oid {
	case types.T_int64:
		return explicitTemporalIntegerCast[int64](parameters, result, proc, length, selectList, truncate)
	case types.T_uint64:
		return explicitTemporalIntegerCast[uint64](parameters, result, proc, length, selectList, truncate)
	default:
		return moerr.NewInvalidInputNoCtx("temporal CAST integer conversion requires int64 or uint64 target")
	}
}

// Round in the calendar/clock domain before packing; adding one to HHMMSS
// would give invalid minute/hour carries. Keep decimal packing fractional.
func roundedDatetimeInteger(value types.Datetime) int64 {
	rounded := value.TruncateToScale(0)
	if rounded.Year() > types.MaxDatetimeYear {
		// MySQL retains the original date with a zero clock when the carry cannot
		// advance the maximum calendar date.
		rounded = value.ToDate().ToDatetime()
	}
	return packedDatetimeInt64(rounded)
}

func roundedTimeInteger(value types.Time) int64 {
	magnitude := uint64(value)
	if value < 0 {
		magnitude = -magnitude
	}
	seconds := (magnitude + 500000) / 1000000
	// Preserve MySQL's ceiling without saturating MO's extended TIME domain.
	const mysqlLastSecond = 838*3600 + 59*60 + 59
	if magnitude/1000000 == mysqlLastSecond && seconds > mysqlLastSecond {
		seconds = mysqlLastSecond
	}
	packed := int64(seconds/3600*10000 + (seconds/60)%60*100 + seconds%60)
	if value < 0 {
		packed = -packed
	}
	return packed
}

// This is the public integer CAST policy, not assignment or the binder-owned
// integer-argument protocol. Negative TIME wraps only for explicit UNSIGNED.
func explicitTemporalIntegerCast[R int64 | uint64](parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, truncate bool) error {
	switch parameters[0].GetType().Oid {
	case types.T_date:
		return opUnaryFixedToFixed(parameters, result, proc, length, func(value types.Date) R {
			return R(packedDateInt64(value))
		}, selectList)
	case types.T_time:
		return opUnaryFixedToFixed(parameters, result, proc, length, func(value types.Time) R {
			if truncate {
				return R(value.ToInt64())
			}
			return R(roundedTimeInteger(value))
		}, selectList)
	case types.T_datetime:
		return opUnaryFixedToFixed(parameters, result, proc, length, func(value types.Datetime) R {
			if truncate {
				return R(packedDatetimeInt64(value))
			}
			return R(roundedDatetimeInteger(value))
		}, selectList)
	default: // TIMESTAMP: pack the session's civil clock, not UTC.
		zone := time.Local
		if proc != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		return opUnaryFixedToFixed(parameters, result, proc, length, func(value types.Timestamp) R {
			if truncate {
				return R(packedDatetimeInt64(value.ToDatetime(zone)))
			}
			return R(roundedDatetimeInteger(value.ToDatetime(zone)))
		}, selectList)
	}
}
