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

package siriusbridge

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// Numeric statuses are append-only ABI-v1 values. Classify by the operation's
// native status, never by diagnostic text or the enclosing SQL expression.
func nativeNumericError(code uint32, message string) error {
	switch code {
	case 12:
		return moerr.NewOutOfRangeNoCtx("decimal", message)
	case 13:
		return moerr.NewInvalidInputNoCtx(message)
	default:
		return nil
	}
}
