// Copyright 2026 Matrix Origin
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

package mongodb

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const conversionErrorRateMinAttempts = 100

var errDecodedBatchBudget = moerr.NewInternalErrorNoCtx("MongoDB decoded batch byte limit exceeded")

type Converter struct {
	columns                []ColumnMapping
	maxValueBytes          int64
	maxConversionErrors    int64
	maxConversionErrorRate float64
	conversionAttempts     int64
	conversionErrors       int64
}

func NewConverter(ctx context.Context, columns []ColumnMapping, maxValueBytes int64) (*Converter, error) {
	if len(columns) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB scan requires explicit column mappings")
	}
	seen := make(map[string]struct{}, len(columns))
	for i := range columns {
		if columns[i].Name == "" {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB mapped column name cannot be empty")
		}
		if _, ok := seen[strings.ToLower(columns[i].Name)]; ok {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB mapped column names must be unique")
		}
		seen[strings.ToLower(columns[i].Name)] = struct{}{}
		if columns[i].Path == "" {
			columns[i].Path = columns[i].Name
		}
		if err := ValidateBSONPath(ctx, columns[i].Path); err != nil {
			return nil, err
		}
		if columns[i].Conversion == "" {
			columns[i].Conversion = ConversionStrict
		}
		if columns[i].Conversion != ConversionStrict && columns[i].Conversion != ConversionTryNull {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB conversion mode must be strict or try_null")
		}
		if !supportedMOType(types.T(columns[i].TypeID)) {
			return nil, moerr.NewNotSupportedf(ctx, "MongoDB mapping target type %s", types.T(columns[i].TypeID).String())
		}
	}
	if maxValueBytes <= 0 {
		maxValueBytes = DefaultRuntimeConfig().MaxValueBytes
	}
	defaults := DefaultRuntimeConfig()
	return &Converter{
		columns: columns, maxValueBytes: maxValueBytes,
		maxConversionErrors:    defaults.MaxConversionErrors,
		maxConversionErrorRate: defaults.MaxConversionErrorRate,
	}, nil
}

// SetConversionErrorLimits configures statement-local try_null protection.
// The converter itself is statement-scoped, so Reset/Prepare starts fresh
// counters without sharing mutable state across operators.
func (c *Converter) SetConversionErrorLimits(maxErrors int64, maxRate float64) {
	defaults := DefaultRuntimeConfig()
	if maxErrors <= 0 {
		maxErrors = defaults.MaxConversionErrors
	}
	if maxRate <= 0 || maxRate > 1 {
		maxRate = defaults.MaxConversionErrorRate
	}
	c.maxConversionErrors = maxErrors
	c.maxConversionErrorRate = maxRate
}

func (c *Converter) ConversionErrors() int64 { return c.conversionErrors }

func (c *Converter) Columns() []ColumnMapping {
	return append([]ColumnMapping(nil), c.columns...)
}

func (c *Converter) NewBatch() *batch.Batch {
	attrs := make([]string, len(c.columns))
	bat := batch.NewWithSize(len(c.columns))
	for i, column := range c.columns {
		attrs[i] = column.Name
		bat.Vecs[i] = vector.NewVec(types.New(types.T(column.TypeID), column.Width, column.Scale))
	}
	bat.Attrs = attrs
	return bat
}

func (c *Converter) AppendDocument(ctx context.Context, bat *batch.Batch, raw []byte, mp *mpool.MPool) error {
	return c.AppendDocumentWithBudget(ctx, bat, raw, mp, 0)
}

// AppendDocumentWithBudget appends one BSON document while admitting every
// vector slot and varlen payload before it is copied. maxBatchBytes <= 0 keeps
// the legacy unbounded behavior for direct converter users.
func (c *Converter) AppendDocumentWithBudget(
	ctx context.Context,
	bat *batch.Batch,
	raw []byte,
	mp *mpool.MPool,
	maxBatchBytes int64,
) error {
	if int64(len(raw)) > c.maxValueBytes {
		return moerr.NewInvalidInput(ctx, "MongoDB BSON document exceeds max-value-bytes")
	}
	doc := bson.Raw(raw)
	if err := doc.Validate(); err != nil {
		return moerr.NewInvalidInput(ctx, "MongoDB returned invalid BSON")
	}
	startRows := bat.RowCount()
	startAttempts, startErrors := c.conversionAttempts, c.conversionErrors
	budget := decodedBatchBudget{remaining: maxBatchBytes - int64(bat.Size()), enabled: maxBatchBytes > 0}
	if budget.enabled && budget.remaining < 0 {
		return errDecodedBatchBudget
	}
	committed := false
	defer func() {
		if !committed {
			// A budget miss can defer this same raw document to the next batch.
			// Keep statement-level try_null accounting transactional with the row
			// so the retry does not count its conversions twice.
			c.conversionAttempts, c.conversionErrors = startAttempts, startErrors
			for _, vec := range bat.Vecs {
				vec.SetLength(startRows)
			}
			bat.SetRowCount(startRows)
		}
	}()
	for i, column := range c.columns {
		value, found, lookupErr := lookupScalarPath(doc, column.Path)
		if err := budget.reserve(int64(bat.Vecs[i].GetType().TypeSize())); err != nil {
			return err
		}
		if !found || lookupErr == nil && (value.Type == bson.TypeNull || value.Type == bson.TypeUndefined) {
			if column.NotNullable {
				return mongoDBNotNullError(ctx, column)
			}
			if appendErr := vector.AppendNull(bat.Vecs[i], mp); appendErr != nil {
				return appendErr
			}
			continue
		}
		if column.Conversion == ConversionTryNull {
			c.conversionAttempts++
		}
		appendErr := lookupErr
		if appendErr == nil {
			appendErr = c.appendValue(bat.Vecs[i], value, column, mp, &budget)
		}
		if err := appendErr; err != nil {
			if errors.Is(err, errConversion) && column.Conversion == ConversionTryNull {
				c.conversionErrors++
				metric.MongoDBConversionErrorCounter.Inc()
				if column.NotNullable {
					return mongoDBNotNullError(ctx, column)
				}
				if c.conversionErrors > c.maxConversionErrors ||
					c.conversionAttempts >= conversionErrorRateMinAttempts &&
						float64(c.conversionErrors)/float64(c.conversionAttempts) > c.maxConversionErrorRate {
					return moerr.NewInvalidInput(ctx, "MongoDB try_null conversion error limit exceeded")
				}
				if appendErr := vector.AppendNull(bat.Vecs[i], mp); appendErr != nil {
					return appendErr
				}
				continue
			}
			if errors.Is(err, errConversion) {
				return moerr.NewInvalidInputf(ctx, "MongoDB value at path %s cannot be converted to %s", column.Path, types.T(column.TypeID).String())
			}
			return err
		}
	}
	bat.SetRowCount(startRows + 1)
	committed = true
	return nil
}

type decodedBatchBudget struct {
	remaining int64
	enabled   bool
}

func (b *decodedBatchBudget) reserve(bytes int64) error {
	if !b.enabled || bytes <= 0 {
		return nil
	}
	if bytes > b.remaining {
		return errDecodedBatchBudget
	}
	b.remaining -= bytes
	return nil
}

func IsDecodedBatchBudgetExceeded(err error) bool {
	return errors.Is(err, errDecodedBatchBudget)
}

func mongoDBNotNullError(ctx context.Context, column ColumnMapping) error {
	return moerr.NewInvalidInputf(ctx, "MongoDB NOT NULL column %s at path %s produced NULL", column.Name, column.Path)
}

func lookupScalarPath(doc bson.Raw, path string) (bson.RawValue, bool, error) {
	parts := strings.Split(path, ".")
	current := doc
	for i, part := range parts {
		value, err := current.LookupErr(part)
		if err != nil {
			if errors.Is(err, bsoncore.ErrElementNotFound) {
				return bson.RawValue{}, false, nil
			}
			return bson.RawValue{}, true, errConversion
		}
		if i == len(parts)-1 {
			return value, true, nil
		}
		nested, ok := value.DocumentOK()
		if !ok {
			// Arrays and polymorphic scalar parents are not silently interpreted as
			// dotted documents. They follow the mapping's strict/try_null policy.
			return bson.RawValue{}, true, errConversion
		}
		current = nested
	}
	return bson.RawValue{}, false, nil
}

func (c *Converter) appendValue(
	vec *vector.Vector,
	value bson.RawValue,
	column ColumnMapping,
	mp *mpool.MPool,
	budget *decodedBatchBudget,
) error {
	target := types.T(column.TypeID)
	switch target {
	case types.T_bool:
		v, ok := value.BooleanOK()
		if !ok {
			return errConversion
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_int8:
		v, ok := bsonInt64(value)
		if !ok || v < math.MinInt8 || v > math.MaxInt8 {
			return errConversion
		}
		return vector.AppendFixed(vec, int8(v), false, mp)
	case types.T_int16:
		v, ok := bsonInt64(value)
		if !ok || v < math.MinInt16 || v > math.MaxInt16 {
			return errConversion
		}
		return vector.AppendFixed(vec, int16(v), false, mp)
	case types.T_int32:
		v, ok := bsonInt64(value)
		if !ok || v < math.MinInt32 || v > math.MaxInt32 {
			return errConversion
		}
		return vector.AppendFixed(vec, int32(v), false, mp)
	case types.T_int64:
		v, ok := bsonInt64(value)
		if !ok {
			return errConversion
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		v, ok := bsonInt64(value)
		if !ok || v < 0 {
			return errConversion
		}
		u := uint64(v)
		switch target {
		case types.T_uint8:
			if u > math.MaxUint8 {
				return errConversion
			}
			return vector.AppendFixed(vec, uint8(u), false, mp)
		case types.T_uint16:
			if u > math.MaxUint16 {
				return errConversion
			}
			return vector.AppendFixed(vec, uint16(u), false, mp)
		case types.T_uint32:
			if u > math.MaxUint32 {
				return errConversion
			}
			return vector.AppendFixed(vec, uint32(u), false, mp)
		default:
			return vector.AppendFixed(vec, u, false, mp)
		}
	case types.T_float32, types.T_float64:
		v, ok := value.AsFloat64OK()
		if !ok {
			return errConversion
		}
		if target == types.T_float32 {
			if !math.IsInf(v, 0) && (v > math.MaxFloat32 || v < -math.MaxFloat32) {
				return errConversion
			}
			return vector.AppendFixed(vec, float32(v), false, mp)
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		text, ok := bsonNumberString(value)
		if !ok {
			return errConversion
		}
		switch target {
		case types.T_decimal64:
			v, err := types.ParseDecimal64(text, column.Width, column.Scale)
			if err != nil {
				return errConversion
			}
			return vector.AppendFixed(vec, v, false, mp)
		case types.T_decimal128:
			v, err := types.ParseDecimal128(text, column.Width, column.Scale)
			if err != nil {
				return errConversion
			}
			return vector.AppendFixed(vec, v, false, mp)
		default:
			v, err := types.ParseDecimal256(text, column.Width, column.Scale)
			if err != nil {
				return errConversion
			}
			return vector.AppendFixed(vec, v, false, mp)
		}
	case types.T_date, types.T_datetime, types.T_timestamp:
		millis, ok := value.DateTimeOK()
		if !ok {
			return errConversion
		}
		instant := time.UnixMilli(millis).UTC()
		if instant.Year() < types.MinDatetimeYear || instant.Year() > types.MaxDatetimeYear {
			return errConversion
		}
		dt := types.DatetimeFromUnixWithNsec(time.UTC, instant.Unix(), int64(instant.Nanosecond()))
		switch target {
		case types.T_date:
			return vector.AppendFixed(vec, dt.ToDate(), false, mp)
		case types.T_datetime:
			return vector.AppendFixed(vec, dt.TruncateToScale(column.Scale), false, mp)
		default:
			return vector.AppendFixed(vec, dt.ToTimestamp(time.UTC).TruncateToScale(column.Scale), false, mp)
		}
	case types.T_char, types.T_varchar, types.T_text:
		var data []byte
		if s, ok := value.StringValueOK(); ok {
			data = []byte(s)
		} else if objectID, ok := value.ObjectIDOK(); ok {
			data = []byte(objectID.Hex())
		} else {
			return errConversion
		}
		return c.appendString(vec, data, column.Width, mp, budget)
	case types.T_binary, types.T_varbinary, types.T_blob:
		var data []byte
		if _, bytes, ok := value.BinaryOK(); ok {
			data = bytes
		} else if objectID, ok := value.ObjectIDOK(); ok {
			data = objectID[:]
		} else {
			return errConversion
		}
		return c.appendBytes(vec, data, column.Width, mp, budget)
	case types.T_json:
		// Canonical Extended JSON preserves BSON distinctions such as int32 vs
		// int64, Decimal128, Date and Binary instead of silently relaxing them
		// into a lossy generic JSON number/string representation.
		text := value.String()
		if text == "" {
			return errConversion
		}
		jsonValue, err := types.ParseStringToByteJson(text)
		if err != nil {
			return errConversion
		}
		data, err := types.EncodeJson(jsonValue)
		if err != nil {
			return errConversion
		}
		return c.appendBytes(vec, data, column.Width, mp, budget)
	default:
		return errConversion
	}
}

func (c *Converter) appendBytes(
	vec *vector.Vector,
	value []byte,
	width int32,
	mp *mpool.MPool,
	budget *decodedBatchBudget,
) error {
	if int64(len(value)) > c.maxValueBytes || width > 0 && int32(len(value)) > width {
		return errConversion
	}
	if err := budget.reserve(int64(len(value))); err != nil {
		return err
	}
	return vector.AppendBytes(vec, value, false, mp)
}

func (c *Converter) appendString(
	vec *vector.Vector,
	value []byte,
	width int32,
	mp *mpool.MPool,
	budget *decodedBatchBudget,
) error {
	// MO CHAR/VARCHAR width is measured in Unicode code points, while the
	// memory-protection limit remains a byte limit. This matches the existing
	// external reader and avoids rejecting valid multi-byte strings early.
	if int64(len(value)) > c.maxValueBytes || width > 0 && utf8.RuneCount(value) > int(width) {
		return errConversion
	}
	if err := budget.reserve(int64(len(value))); err != nil {
		return err
	}
	return vector.AppendBytes(vec, value, false, mp)
}

func bsonInt64(value bson.RawValue) (int64, bool) {
	switch value.Type {
	case bson.TypeInt32:
		v, ok := value.Int32OK()
		return int64(v), ok
	case bson.TypeInt64:
		return value.Int64OK()
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		// float64(math.MaxInt64) rounds to 2^63, so a conventional
		// v > math.MaxInt64 check accidentally accepts exactly 2^63 and Go's
		// conversion produces MinInt64. The upper bound must be exclusive.
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v ||
			v < -9223372036854775808.0 || v >= 9223372036854775808.0 {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

func bsonNumberString(value bson.RawValue) (string, bool) {
	switch value.Type {
	case bson.TypeInt32:
		v, ok := value.Int32OK()
		return strconv.FormatInt(int64(v), 10), ok
	case bson.TypeInt64:
		v, ok := value.Int64OK()
		return strconv.FormatInt(v, 10), ok
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
			return "", false
		}
		return strconv.FormatFloat(v, 'g', -1, 64), true
	case bson.TypeDecimal128:
		v, ok := value.Decimal128OK()
		if !ok || v.IsNaN() || v.IsInf() != 0 {
			return "", false
		}
		return v.String(), true
	default:
		return "", false
	}
}

func supportedMOType(t types.T) bool {
	switch t {
	case types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_datetime, types.T_timestamp,
		types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return true
	default:
		return false
	}
}

var errConversion = moerr.NewInvalidInputNoCtx("MongoDB BSON conversion failed")
