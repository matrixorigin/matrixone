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

// Package udferr defines diagnostic errors for the routine catalog and external
// execution contract. These errors retain the domain's message verbatim; they
// are not MO SQL error classifications. Callers that own a specific SQL error
// must continue to construct that moerr at the SQL boundary.
package udferr

import "fmt"

// Error is a routine-contract diagnostic. Each construction has distinct
// identity so equal text does not accidentally alias independent sentinels.
type Error struct{ message string }

func (e *Error) Error() string { return e.message }

func New(message string) error { return &Error{message: message} }

// Newf formats a leaf diagnostic. Use errutil wrapping for an existing cause;
// this function deliberately does not interpret wrapping verbs.
func Newf(format string, args ...any) error { return New(fmt.Sprintf(format, args...)) }
