// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

// Numeric Parquet DATE/TIMESTAMP sources that feed a temporal target under
// LOAD receive the same SQL assignment contract as INSERT:
//
//   - values above the supported SQL calendar (year > 9999 in the target
//     calendar) are rejected row by row, before any value reaches the target
//     vector;
//   - the carrier keeps the true source precision (scale 6) and the single
//     target-scale normalization is the standard assignment cast, which owns
//     half-step rounding, carry, TIME_TRUNCATE_FRACTIONAL, and
//     statement-ignore semantics;
//   - after that normalization the rows are re-checked, because rounding can
//     carry 9999-12-31 23:59:59.999999 into 10000-01-01 00:00:00;
//   - the lower bound and zero-date policies stay owned by the insert layer.
//
// Foreign-table reads never route through this path: their raw mapping policy
// is unchanged.
//
// All checks are per consumed logical row. Dictionary pages are expanded per
// referenced entry only, so an out-of-range entry that no consumed row
// references does not fail the load, and NULL-only pages pass unchanged.

var (
	// parquetUnixEpochMicros is the numeric of 1970-01-01 in the year-0 based
	// microsecond space shared by types.Timestamp and types.Datetime.
	parquetUnixEpochMicros = int64(types.UnixMicroToTimestamp(0))
	// parquetUnixEpochDays is the Date numeric of 1970-01-01.
	parquetUnixEpochDays = int32(types.DaysFromUnixEpochToDate(0))
	// The storable SQL calendar in days since the Unix epoch: 0001-01-01
	// through 9999-12-31. A raw day count inside this interval can never wrap
	// in the epoch add nor in any derived DATETIME/TIMESTAMP conversion.
	parquetMinLoadDateDays = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / int64(86400)
	parquetMaxLoadDateDays = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / int64(86400)
	// parquetMaxLoadDatetime is 9999-12-31 23:59:59.999999 in the shared
	// year-0 based numeric space: the SQL-calendar upper bound.
	parquetMaxLoadDatetime = types.FromClockUTC(9999, 12, 31, 23, 59, 59, 999999).ToDatetime(time.UTC)
	// Staging carriers carry the true parquet source precision.
	parquetTimestampStagingType = types.T_timestamp.ToTypeWithScale(6)
	parquetDatetimeStagingType  = types.T_datetime.ToTypeWithScale(6)
)

// parquetLoadTemporalOffsetBudgetMicros bounds the timezone offset applied by
// ToDatetime when a timestamp is displayed in the session timezone. Real
// offsets stay within +-15h; the two-day headroom additionally keeps the
// epoch-space guard below from covering every wrap exactly at its boundary.
const parquetLoadTemporalOffsetBudgetMicros = int64(2 * 86400 * 1000000)

// parquetGuardTimestampMicros rejects raw epoch microseconds that would
// overflow the shared year-0 based numeric space when the epoch or a zone
// offset is added. A wrapped value could otherwise look like a small valid
// timestamp and slip past calendar validation.
func parquetGuardTimestampMicros(ctx context.Context, micros int64) error {
	if micros > math.MaxInt64-parquetUnixEpochMicros-parquetLoadTemporalOffsetBudgetMicros ||
		micros < math.MinInt64+parquetUnixEpochMicros+parquetLoadTemporalOffsetBudgetMicros {
		return moerr.NewInvalidInputf(ctx,
			"parquet timestamp %d microseconds is out of range", micros)
	}
	return nil
}

// parquetLoadDate32 maps days since the Unix epoch into a Date value under
// the storable SQL calendar. The raw-domain guard runs before the epoch add
// and before any derived DATETIME/TIMESTAMP conversion, so extreme int32 day
// counts cannot wrap into a small valid-looking value downstream. Zero-date
// and TIMESTAMP lower-bound policies stay owned by the insert layer.
func parquetLoadDate32(ctx context.Context, days int32) (types.Date, error) {
	if int64(days) < parquetMinLoadDateDays || int64(days) > parquetMaxLoadDateDays {
		return 0, moerr.NewInvalidInputf(ctx,
			"parquet date is out of range for column type DATE")
	}
	return types.Date(int64(days) + int64(parquetUnixEpochDays)), nil
}

// parquetLoadCheckDatetime rejects datetime carriers whose civil year exceeds
// the SQL calendar. TIMESTAMP display years are compared through the same
// numeric space after conversion into the session timezone.
func parquetLoadCheckDatetime(ctx context.Context, dt types.Datetime) (types.Datetime, error) {
	if dt > parquetMaxLoadDatetime {
		return 0, moerr.NewInvalidInputf(ctx,
			"parquet datetime value is out of range for column type DATETIME")
	}
	return dt, nil
}

func parquetLoadCheckTimestamp(ctx context.Context, ts types.Timestamp, loc *time.Location) (types.Timestamp, error) {
	if ts.ToDatetime(loc) > parquetMaxLoadDatetime {
		return 0, moerr.NewInvalidInputf(ctx,
			"parquet timestamp value is out of range for column type TIMESTAMP")
	}
	return ts, nil
}

// parquetLoadTimestampMicrosToTimestamp converts raw epoch microseconds into
// the TIMESTAMP carrier under the LOAD domain checks. adjusted=true values are
// UTC instants validated in the session-local display calendar, so a UTC tail
// that becomes local year 10000 at +14:00 is rejected while a UTC head that is
// still local year 9999 at -12:00 is accepted. adjusted=false values are local
// wall-clock readings that must not be timezone-shifted twice: their wall
// calendar is validated directly and the converted display is re-checked.
func parquetLoadTimestampMicrosToTimestamp(ctx context.Context, micros int64, adjustedToUTC bool, loc *time.Location) (types.Timestamp, error) {
	if err := parquetGuardTimestampMicros(ctx, micros); err != nil {
		return 0, err
	}
	if adjustedToUTC {
		return parquetLoadCheckTimestamp(ctx, types.UnixMicroToTimestamp(micros), loc)
	}
	if _, err := parquetLoadCheckDatetime(ctx, types.Datetime(micros+parquetUnixEpochMicros)); err != nil {
		return 0, err
	}
	return parquetLoadCheckTimestamp(ctx, parquetLocalTimestampMicrosToTimestamp(micros, loc), loc)
}

// parquetLoadTimestampValueToDatetime converts a numeric Parquet timestamp
// value into its DATETIME carrier under the same LOAD domain checks.
func parquetLoadTimestampValueToDatetime(ctx context.Context, st parquet.Type, v parquet.Value, loc *time.Location) (types.Datetime, error) {
	lt := st.LogicalType()
	micros, err := parquetTimestampValueToMicros(ctx, v, lt)
	if err != nil {
		return 0, err
	}
	if err := parquetGuardTimestampMicros(ctx, micros); err != nil {
		return 0, err
	}
	if lt != nil && lt.Timestamp != nil && lt.Timestamp.IsAdjustedToUTC {
		return parquetLoadCheckDatetime(ctx, types.UnixMicroToTimestamp(micros).ToDatetime(loc))
	}
	return parquetLoadCheckDatetime(ctx, types.Datetime(micros+parquetUnixEpochMicros))
}

// processParquetTemporalAssignment applies SQL assignment semantics to one
// numeric Parquet temporal page under LOAD. Values are staged per consumed
// logical row in a carrier typed at the true source precision (scale 6), then
// normalized once by function.NewAssignCast / NewAssignIgnoreCast into the
// target column type and scale, then re-checked against the target SQL
// calendar, and only then appended to the target vector. convert must return
// a value already validated against the pre-normalization domain (call the
// parquetLoad* helpers above).
func processParquetTemporalAssignment[T types.Timestamp | types.Datetime](
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	stagingType types.Type,
	convert func(parquet.Value) (T, error),
) error {
	staging := vector.NewVec(stagingType)
	defer staging.Free(proc.Mp())
	if err := processParquetValuesToFixed(proc.Ctx, mp, page, proc, staging, T(0), convert); err != nil {
		return err
	}
	length := staging.Length()
	result := vector.NewFunctionResultWrapper(*vec.GetType(), proc.Mp())
	defer result.Free()
	if err := result.PreExtendAndReset(length); err != nil {
		return err
	}
	cast := function.NewAssignCast
	if proc.GetStmtProfile().GetStatementIgnore() {
		cast = function.NewAssignIgnoreCast
	}
	if err := cast([]*vector.Vector{staging, vec}, result, proc, length, nil); err != nil {
		return err
	}
	if err := validateParquetTemporalLoadAssignment(proc.Ctx, result.GetResultVector(), length, parquetSessionLocation(proc)); err != nil {
		return err
	}
	checkpoint := vec.MakeAppendCheckpoint()
	if err := vec.UnionBatch(result.GetResultVector(), 0, length, nil, proc.Mp()); err != nil {
		return vector.JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, length))
	}
	return nil
}

// validateParquetTemporalLoadAssignment re-validates normalized values
// against the target SQL calendar: default-mode rounding at a lower target
// scale can carry 9999-12-31 23:59:59.999999 into 10000-01-01 00:00:00, and
// that normalized result must be rejected even though the raw staged value was
// in range.
func validateParquetTemporalLoadAssignment(ctx context.Context, result *vector.Vector, length int, loc *time.Location) error {
	switch result.GetType().Oid {
	case types.T_timestamp:
		col := vector.MustFixedColWithTypeCheck[types.Timestamp](result)
		for row := 0; row < length; row++ {
			if result.IsNull(uint64(row)) {
				continue
			}
			if _, err := parquetLoadCheckTimestamp(ctx, col[row], loc); err != nil {
				return err
			}
		}
	case types.T_datetime:
		col := vector.MustFixedColWithTypeCheck[types.Datetime](result)
		for row := 0; row < length; row++ {
			if result.IsNull(uint64(row)) {
				continue
			}
			if _, err := parquetLoadCheckDatetime(ctx, col[row]); err != nil {
				return err
			}
		}
	}
	return nil
}
