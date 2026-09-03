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

// Package arrowbridge owns the in-process Arrow-to-MatrixOne container
// boundary. BindLoad freezes a LOAD schema and conversion policy before data
// is acquired; Convert then either retains immutable Arrow backing or
// materializes an owned vector transactionally under the caller's allocation
// account.
//
// The package intentionally knows nothing about files, object stores, Flight,
// SQL transactions, or runtime credentials. LOAD and external runtimes may
// share its physical conversion and lease rules while retaining independent
// protocol, authorization, error, and exact-type policies. In particular,
// BindLoad implements the LOAD conversion matrix. It is deliberately named
// for that consumer because Python UDF results have a stricter, versioned ABI:
// a future UDF binder must validate its logical metadata and exact type matrix
// before constructing a Plan, rather than silently accepting LOAD widening or
// temporal conversions.
package arrowbridge
