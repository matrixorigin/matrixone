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

package wal

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// RecordTooLargeError is returned only before an entry has been enqueued and
// before an LSN or DSN has been allocated. Its durability outcome is known.
type RecordTooLargeError struct {
	cause *moerr.Error
}

func (e *RecordTooLargeError) Error() string       { return e.cause.Error() }
func (e *RecordTooLargeError) Unwrap() error       { return e.cause }
func (e *RecordTooLargeError) MoErr() *moerr.Error { return e.cause }

func NewRecordTooLargeError(actual, limit int) *RecordTooLargeError {
	return &RecordTooLargeError{cause: moerr.NewInternalErrorNoCtxf(
		"WAL record is too large: %d bytes, limit %d bytes", actual, limit,
	)}
}
