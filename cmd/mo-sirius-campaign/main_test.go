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

package main

import "testing"

func TestExactPublicCodec(t *testing.T) {
	for _, test := range []struct {
		input            string
		precision, scale int64
		want             string
	}{
		{"-0.00", 3, 2, "0"},
		{"1.2", 4, 2, "120"},
		{"-340282366920938463463374607431768211457.12345678", 65, 8, "-34028236692093846346337460743176821145712345678"},
	} {
		got, err := exactDecimal(test.input, test.precision, test.scale)
		if err != nil || got != test.want {
			t.Fatalf("exact decimal: got %s, %v", got, err)
		}
	}
	for _, input := range []string{"1e2", "NaN", "1.234", "1234", ".1", "--1"} {
		if _, err := exactDecimal(input, 3, 2); err == nil {
			t.Fatalf("accepted invalid decimal %s", input)
		}
	}
	for _, test := range []struct{ typ, value, want string }{
		{"BIGINT", "-9223372036854775808", "int"},
		{"UNSIGNED BIGINT", "18446744073709551615", "uint"},
		{"DATE", "0000-00-00", "date"},
	} {
		got, err := encodeCell(publicColumn{SQLType: test.typ}, []byte(test.value))
		if err != nil || got.Type != test.want || got.Value != test.value {
			t.Fatalf("public scalar: %+v %v", got, err)
		}
	}
	if _, err := encodeCell(publicColumn{SQLType: "UNSIGNED BIGINT"}, []byte("-1")); err == nil {
		t.Fatal("accepted negative unsigned integer")
	}
	got, err := encodeCell(publicColumn{SQLType: "VARCHAR"}, nil)
	if err != nil || !got.Null {
		t.Fatal("lost NULL")
	}
	got, err = encodeCell(publicColumn{SQLType: "VARCHAR"}, []byte{})
	if err != nil || got.Null || got.Value != "" {
		t.Fatal("empty string became NULL")
	}
}
