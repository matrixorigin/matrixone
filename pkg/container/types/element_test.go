// Copyright 2021 Matrix Origin
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

package types

import (
	"fmt"
	"testing"
)

type testCase struct {
	oid T
	e   Element
}

var tcs []testCase

func init() {
	tcs = append(tcs, newTestCase(T_bool, Bool(true)))
	tcs = append(tcs, newTestCase(T_int8, Int8(0)))
	tcs = append(tcs, newTestCase(T_int16, Int16(0)))
	tcs = append(tcs, newTestCase(T_int32, Int32(0)))
	tcs = append(tcs, newTestCase(T_int64, Int64(0)))
	tcs = append(tcs, newTestCase(T_uint8, UInt8(0)))
	tcs = append(tcs, newTestCase(T_uint16, UInt16(0)))
	tcs = append(tcs, newTestCase(T_uint32, UInt32(0)))
	tcs = append(tcs, newTestCase(T_uint64, UInt64(0)))
	tcs = append(tcs, newTestCase(T_float32, Float32(0)))
	tcs = append(tcs, newTestCase(T_float64, Float64(0)))
	tcs = append(tcs, newTestCase(T_date, Date(0)))
	tcs = append(tcs, newTestCase(T_datetime, Datetime(0)))
	tcs = append(tcs, newTestCase(T_timestamp, Timestamp(0)))
	tcs = append(tcs, newTestCase(T_decimal64, Decimal64(0)))
	tcs = append(tcs, newTestCase(T_decimal64, Decimal128{}))
	tcs = append(tcs, newTestCase(T_char, String("x")))
}

func TestSize(t *testing.T) {
	for _, tc := range tcs {
		tc.e.Size()
	}
}

func TestNew(t *testing.T) {
	for _, tc := range tcs {
		New(tc.oid, 0, 0, 0)
	}
}

func TestTypeSize(t *testing.T) {
	for _, tc := range tcs {
		fmt.Printf("%v\n", TypeSize(tc.oid))
	}
}

func TestString(t *testing.T) {
	for _, tc := range tcs {
		fmt.Printf("%v\n", tc.oid.String())
	}
}

func TestTypeString(t *testing.T) {
	for _, tc := range tcs {
		fmt.Printf("%v\n", New(tc.oid, 0, 0, 0).String())
	}
}

func newTestCase(oid T, e Element) testCase {
	return testCase{
		e:   e,
		oid: oid,
	}
}
