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

// Package jsonread contains shared helpers for the sealed JSON readers. It
// does not admit input or confer read authority on a ByteJson.
package jsonread

import (
	"errors"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

type conversionError struct{ err error }

func (e *conversionError) Error() string { return e.err.Error() }
func (e *conversionError) Unwrap() error { return e.err }

// IsConversionError distinguishes formatting failures from admission failures.
func IsConversionError(err error) bool {
	var conversion *conversionError
	return errors.As(err, &conversion)
}

func reserveBytes(reserve func(uint64) (func(), error), size uint64) (func(), error) {
	if reserve == nil {
		return nil, nil
	}
	release, err := reserve(size)
	if err != nil && release != nil {
		release()
		return nil, err
	}
	return release, err
}

func releaseBytes(release func()) {
	if release != nil {
		release()
	}
}

// DirectText returns a temporary view for synchronous copying into a result
// vector, or formats a bounded scalar into the caller's scratch. The view must
// never escape the sealed reader. A negative limit permits any string length.
func DirectText(value bytejson.ByteJson, legacyFloat bool, limit int, scratch []byte) ([]byte, bool, error) {
	if value.Type == bytejson.TpCodeString {
		text := value.GetString()
		if limit >= 0 && len(text) > limit {
			return nil, false, nil
		}
		return text, true, nil
	}
	text, ok, err := value.AppendUnquotedScalar(scratch, legacyFloat)
	if err != nil {
		return nil, false, &conversionError{err: err}
	}
	if ok && limit >= 0 && len(text) > limit {
		return nil, false, nil
	}
	return text, ok, nil
}

// CopyText admits the formatter's temporary storage and an independently
// owned result before allocating either. The result reservation belongs to
// the caller; formatting failures are tagged separately from budget errors.
func CopyText(value bytejson.ByteJson, legacyFloat bool, reserve func(uint64) (func(), error)) (string, func(), error) {
	if value.Type == bytejson.TpCodeString || value.Type == bytejson.TpCodeDecimal {
		raw := value.GetString()
		release, err := reserveBytes(reserve, uint64(len(raw)))
		if err != nil {
			return "", nil, err
		}
		return string(raw), release, nil
	}
	if value.Type == bytejson.TpCodeInt64 || value.Type == bytejson.TpCodeUint64 || value.Type == bytejson.TpCodeFloat64 || value.Type == bytejson.TpCodeLiteral {
		// Fixed-size numeric and literal encodings have bounded renderings.
		// One reservation covers both the temporary buffer and returned text.
		release, err := reserveBytes(reserve, 128)
		if err != nil {
			return "", nil, err
		}
		var text string
		if legacyFloat {
			text, err = value.UnquoteWithLegacyFloat()
		} else {
			text, err = value.Unquote()
		}
		if err != nil {
			releaseBytes(release)
			return "", nil, &conversionError{err: err}
		}
		return text, release, nil
	}
	// JSON escaping can expand bytes, and formatting may briefly hold old and
	// new buffers plus the final string. The reader's reservation callback
	// also covers Go allocator rounding. Keep this temporary charge until the
	// result has its own reservation.
	if uint64(len(value.Data)) > (math.MaxUint64-1024)/16 {
		return "", nil, moerr.NewInvalidInputNoCtx("JSON text size overflow")
	}
	temporary, err := reserveBytes(reserve, uint64(len(value.Data))*16+1024)
	if err != nil {
		return "", nil, err
	}
	defer releaseBytes(temporary)
	var text string
	if legacyFloat {
		text, err = value.UnquoteWithLegacyFloat()
	} else {
		text, err = value.Unquote()
	}
	if err != nil {
		return "", nil, &conversionError{err: err}
	}
	release, err := reserveBytes(reserve, uint64(len(text)))
	if err != nil {
		return "", nil, err
	}
	return text, release, nil
}
