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

package logutil

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
)

type deferredSHA256 string

func (s deferredSHA256) String() string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// StringFingerprintFields is the standard correlation shape for SQL, paths,
// identifiers, endpoints, payloads, and other strings that must not be kept
// in a shared log. SHA-256 calculation is deferred to zap encoding.
func StringFingerprintFields(name, value string) []zap.Field {
	return []zap.Field{
		zap.Stringer(name+"-sha256", deferredSHA256(value)),
		zap.Int(name+"-bytes", len(value)),
	}
}

// ErrorFingerprintFields keeps an error class, MatrixOne error code when
// available, and a non-retained correlation handle. Call it from EventLazy
// when the error text might carry SQL, paths, object names, or user input.
func ErrorFingerprintFields(name string, err error) []zap.Field {
	if err == nil {
		return []zap.Field{zap.Bool(name+"-present", false)}
	}
	fields := StringFingerprintFields(name, err.Error())
	fields = append(fields, zap.String(name+"-type", reflect.TypeOf(err).String()))
	var moErr *moerr.Error
	if errors.As(err, &moErr) {
		fields = append(fields, zap.Uint16("moerr-code", moErr.ErrorCode()))
	}
	return fields
}
