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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// packedDatetimeFloat preserves fractional seconds without integer rounding or
// text parsing. The calendar packing helpers handle MySQL's zero-date sentinel,
// which is not the internal integer zero (0001-01-01).
func packedDatetimeFloat(v types.Datetime) float64 {
	if v == types.ZeroDatetime {
		return 0
	}
	return float64(packedDatetimeInt64(v)) + float64(v.MicroSec())/float64(types.MicroSecsPerSec)
}

func packedTimeFloat(v types.Time) float64 {
	// ToInt64 packs the whole signed HHMMSS; signed remainder preserves even a
	// negative subsecond TIME whose whole HHMMSS is zero.
	return float64(v.ToInt64()) + float64(v%types.Time(types.MicroSecsPerSec))/float64(types.MicroSecsPerSec)
}

func temporalContextResult[S types.FixedSizeTExceptStrType](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process,
	length int, selectList *FunctionSelectList, numeric func(S) float64,
) error {
	if result.GetResultVector().GetType().Oid == types.T_bool {
		return opUnaryFixedToFixed(parameters, result, proc, length, func(v S) bool { return numeric(v) != 0 }, selectList)
	}
	return opUnaryFixedToFixed(parameters, result, proc, length, numeric, selectList)
}

// Only the context-local matchers opt in to these casts implicitly. Other
// numeric targets, especially integer rounding and DECIMAL range behavior,
// retain their existing executors.
func castTemporalContext(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	switch parameters[0].GetType().Oid {
	case types.T_date:
		return temporalContextResult(parameters, result, proc, length, selectList, func(v types.Date) float64 { return float64(packedDateInt64(v)) })
	case types.T_datetime:
		return temporalContextResult(parameters, result, proc, length, selectList, packedDatetimeFloat)
	case types.T_time:
		return temporalContextResult(parameters, result, proc, length, selectList, packedTimeFloat)
	default: // TIMESTAMP is interpreted in the session time zone, never raw UTC microseconds.
		zone := proc.GetSessionInfo().TimeZone
		return temporalContextResult(parameters, result, proc, length, selectList, func(v types.Timestamp) float64 { return packedDatetimeFloat(v.ToDatetime(zone)) })
	}
}
