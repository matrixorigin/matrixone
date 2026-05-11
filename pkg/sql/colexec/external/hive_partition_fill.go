// Copyright 2024 Matrix Origin
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
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isHivePartitionCol returns true if colName is a declared Hive partition column.
func (param *ExternalParam) isHivePartitionCol(colName string) bool {
	if param.Extern == nil || !param.Extern.HivePartitioning {
		return false
	}
	lower := strings.ToLower(colName)
	for _, pc := range param.Extern.HivePartitionCols {
		if pc == lower {
			return true
		}
	}
	return false
}

// refreshPartitionValues extracts partition values from the current file path.
func (param *ExternalParam) refreshPartitionValues() error {
	if param.Extern == nil || !param.Extern.HivePartitioning {
		return nil
	}
	values, err := ExtractPartitionValues(
		param.Fileparam.Filepath,
		param.Extern.Filepath,
		param.Extern.HivePartitionCols,
	)
	if err != nil {
		return err
	}
	param.currentPartValues = values
	return nil
}

// fillVirtualColumns fills partition columns and __mo_filepath for a batch.
func (h *ParquetHandler) fillVirtualColumns(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	rowCount := bat.RowCount()
	mp := proc.Mp()

	if h.filepathColIndex >= 0 {
		vec := bat.Vecs[h.filepathColIndex]
		if err := vector.SetConstBytes(vec, []byte(param.Fileparam.Filepath), rowCount, mp); err != nil {
			return err
		}
	}

	if len(h.partitionColIndices) > 0 {
		return h.fillPartitionColumns(bat, param, proc)
	}
	return nil
}

// fillPartitionColumns fills partition column vectors with constant values from the path.
func (h *ParquetHandler) fillPartitionColumns(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	partValues := param.currentPartValues
	rowCount := bat.RowCount()
	mp := proc.Mp()

	// Error messages use the path relative to the DDL base so that BVT output
	// is portable across machines (absolute filesystem paths would embed
	// /Users/... or /tmp/... in .result files).
	relPath := param.Fileparam.Filepath
	if param.Extern != nil && param.Extern.Filepath != "" {
		relPath = relPartitionPath(param.Fileparam.Filepath, param.Extern.Filepath)
	}

	for _, idx := range h.partitionColIndices {
		col := param.Cols[idx]
		colName := strings.ToLower(col.Name)
		strVal, present := partValues[colName]
		vec := bat.Vecs[idx]

		if !present {
			return moerr.NewInternalErrorf(param.Ctx,
				"partition column '%s' not found in path '%s'", colName, relPath)
		}

		if strVal == HiveDefaultPartition {
			notNullable := col.Default != nil && !col.Default.NullAbility
			if notNullable {
				return moerr.NewConstraintViolationf(param.Ctx,
					"partition column '%s' is NOT NULL but directory has __HIVE_DEFAULT_PARTITION__ in path '%s'; allow NULL on the partition column or remove/rename the default partition directory",
					colName, relPath)
			}
			if err := vector.SetConstNull(vec, rowCount, mp); err != nil {
				return err
			}
			continue
		}

		if err := fillConstantVector(vec, strVal, col, rowCount, proc, relPath); err != nil {
			return err
		}
	}
	return nil
}

// fillConstantVector converts a string partition value to the column's typed vector.
// Follows external loader semantics (getColData path), not SQL CAST.
func fillConstantVector(
	vec *vector.Vector, strVal string, col *plan.ColDef,
	rowCount int, proc *process.Process, filePath string,
) error {
	mp := proc.Mp()
	typ := types.T(col.Typ.Id)

	wrapErr := func(err error) error {
		return moerr.NewInternalErrorf(proc.Ctx,
			"partition value type conversion failed: col=%s, value='%s', path=%s: %v",
			col.Name, strVal, filePath, err)
	}

	switch typ {
	case types.T_int8:
		v, err := parseIntWithFloatFallback(strVal, 8)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, int8(v), rowCount, mp)

	case types.T_int16:
		v, err := parseIntWithFloatFallback(strVal, 16)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, int16(v), rowCount, mp)

	case types.T_int32:
		v, err := parseIntWithFloatFallback(strVal, 32)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, int32(v), rowCount, mp)

	case types.T_int64:
		v, err := parseIntWithFloatFallback(strVal, 64)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_uint8:
		v, err := parseUintWithFloatFallback(strVal, 8)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, uint8(v), rowCount, mp)

	case types.T_uint16:
		v, err := parseUintWithFloatFallback(strVal, 16)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, uint16(v), rowCount, mp)

	case types.T_uint32:
		v, err := parseUintWithFloatFallback(strVal, 32)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, uint32(v), rowCount, mp)

	case types.T_uint64:
		if col.Typ.Enumvalues != "" {
			// SET type stored as uint64 with Enumvalues
			v, err := types.ParseSet(col.Typ.Enumvalues, strVal)
			if err != nil {
				return wrapErr(err)
			}
			return vector.SetConstFixed(vec, v, rowCount, mp)
		}
		v, err := parseUintWithFloatFallback(strVal, 64)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_bit:
		v, err := strconv.ParseUint(strVal, 10, int(col.Typ.Width))
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_float32:
		v, err := strconv.ParseFloat(strVal, 32)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, float32(v), rowCount, mp)

	case types.T_float64:
		v, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_decimal64:
		v, err := types.ParseDecimal64(strVal, col.Typ.Width, col.Typ.Scale)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_decimal128:
		v, err := types.ParseDecimal128(strVal, col.Typ.Width, col.Typ.Scale)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_date:
		v, err := types.ParseDateCast(strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_datetime:
		v, err := types.ParseDatetime(strVal, col.Typ.Scale)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_timestamp:
		v, err := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, strVal, col.Typ.Scale)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_time:
		v, err := types.ParseTime(strVal, col.Typ.Scale)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_bool:
		v, err := types.ParseBool(strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_enum:
		v, err := types.ParseEnum(col.Typ.Enumvalues, strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_char, types.T_varchar, types.T_text,
		types.T_blob, types.T_binary, types.T_varbinary, types.T_datalink:
		return vector.SetConstBytes(vec, []byte(strVal), rowCount, mp)

	case types.T_json:
		v, err := types.ParseStringToByteJson(strVal)
		if err != nil {
			return wrapErr(err)
		}
		bs, err := v.Marshal()
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstBytes(vec, bs, rowCount, mp)

	case types.T_uuid:
		v, err := types.ParseUuid(strVal)
		if err != nil {
			return wrapErr(err)
		}
		return vector.SetConstFixed(vec, v, rowCount, mp)

	case types.T_array_float32, types.T_array_float64:
		return moerr.NewNotSupportedf(proc.Ctx,
			"unsupported partition column type VECTOR for col=%s, path=%s", col.Name, filePath)

	default:
		return moerr.NewNotSupportedf(proc.Ctx,
			"unsupported partition column type %v for col=%s, path=%s", typ, col.Name, filePath)
	}
}

// Float-domain boundary constants for 64-bit overflow detection.
//
// math.MaxInt64 = 2^63 - 1 and math.MaxUint64 = 2^64 - 1 are odd numbers that
// cannot be exactly represented in float64 (only 53 bits of mantissa). Comparing
// float64(f) > math.MaxInt64 silently compares against 2^63 (the nearest float64),
// which lets f == 2^63 slip through and then int64(f) wraps to -2^63.
//
// Using the exact float64 values 2^63 and 2^64 as strict upper bounds closes
// this gap: any f ≥ 2^63 is out of int64 range; any f ≥ 2^64 is out of uint64
// range. Both 2^63 and 2^64 are exactly representable in float64 (powers of 2).
//
// Additionally, float64 can represent consecutive integers exactly only up to
// 2^53. Beyond that, distinct source integers collapse to the same float64
// (e.g. "-9223372036854775809.0" and "-9223372036854775808.0" both parse to
// -2^63). When we reach the float fallback for a 64-bit target, we therefore
// reject any |f| ≥ 2^53: genuine integer strings would have succeeded in
// ParseInt and never reached this path, so a float in the non-exact range
// here implies the source string is a non-integer (decimal/exponent form)
// that we cannot safely round to int64/uint64.
const (
	float64MaxInt64Exclusive  = 0x1p63 // 2^63, one past max int64
	float64MaxUint64Exclusive = 0x1p64 // 2^64, one past max uint64
	float64IntExactLimit      = 0x1p53 // 2^53, largest |x| with consecutive-integer precision
)

// parseIntWithFloatFallback mimics getColData behavior (external.go:1019):
// 1. ParseInt succeeds → use it
// 2. ParseInt fails with ErrRange → reject (no fallback for overflow)
// 3. ParseInt fails otherwise → ParseFloat + range check
func parseIntWithFloatFallback(s string, bitSize int) (int64, error) {
	v, err := strconv.ParseInt(s, 10, bitSize)
	if err == nil {
		return v, nil
	}
	if errors.Is(err, strconv.ErrRange) {
		return 0, err
	}
	f, ferr := strconv.ParseFloat(s, 64)
	if ferr != nil {
		return 0, err
	}
	// Reject non-finite values ("nan" / "inf" parse successfully to NaN/±Inf).
	// NaN in particular is dangerous: NaN < x and NaN >= x are both false, so
	// the range checks below silently accept it and int64(NaN) is undefined.
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, err
	}
	if bitSize == 64 {
		// Exact-float boundaries: MaxInt64 rounds up to 2^63 in float64.
		// Also reject any |f| ≥ 2^53 — beyond float64's consecutive-integer
		// precision, round-trip through float is ambiguous (see const comment).
		if f < math.MinInt64 || f >= float64MaxInt64Exclusive {
			return 0, err
		}
		if f >= float64IntExactLimit || f <= -float64IntExactLimit {
			return 0, err
		}
	} else {
		// Compare f against the type's float-domain bounds BEFORE truncation.
		// Go's int64(f) truncates toward zero, so a naive post-truncation
		// bounds check silently accepts e.g. int32 "-2147483648.9" (truncates
		// to -2^31, which passes a ">= -2^31" check). Both ±(2^(N-1)) and
		// (2^(N-1) - 1) are exactly representable in float64 for N ≤ 53, so
		// the inclusive comparison has no rounding slack.
		lo := float64(int64(-1) << (bitSize - 1)) // -2^(N-1)
		hi := float64(int64(1)<<(bitSize-1) - 1)  //  2^(N-1) - 1
		if f < lo || f > hi {
			return 0, err
		}
	}
	return int64(f), nil
}

// parseUintWithFloatFallback mimics getColData behavior (external.go:1111):
// 1. ParseUint succeeds → use it
// 2. ParseUint fails with ErrRange → reject (no fallback for overflow)
// 3. ParseUint fails otherwise → ParseFloat + range check
func parseUintWithFloatFallback(s string, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, bitSize)
	if err == nil {
		return v, nil
	}
	if errors.Is(err, strconv.ErrRange) {
		return 0, err
	}
	f, ferr := strconv.ParseFloat(s, 64)
	if ferr != nil || f < 0 {
		return 0, err
	}
	// Reject non-finite values (same NaN/Inf risk as parseIntWithFloatFallback).
	// Note: f < 0 above already rejects -Inf, but NaN is not ordered so must be
	// handled explicitly.
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, err
	}
	if bitSize == 64 {
		// Same boundary issue as int64 (MaxUint64 rounds up to 2^64) plus the
		// 2^53 precision limit for round-tripping integers through float64.
		if f >= float64MaxUint64Exclusive {
			return 0, err
		}
		if f >= float64IntExactLimit {
			return 0, err
		}
	} else {
		// Compare f against MaxUintN BEFORE truncation, to reject values like
		// uint32 "4294967295.9" that Go's uint64(f) would truncate to 2^32 - 1
		// and let through. 2^N - 1 is exactly representable in float64 for
		// N ≤ 53, so the inclusive upper bound has no rounding slack. The
		// lower bound is implicit: f < 0 is rejected above.
		hi := float64(uint64(1)<<bitSize - 1) // 2^N - 1
		if f > hi {
			return 0, err
		}
	}
	return uint64(f), nil
}
