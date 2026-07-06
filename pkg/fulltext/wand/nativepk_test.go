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

package wand

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TestEncodePkNativeTypes round-trips the native fixed-width temporal / decimal pk
// types delivered by the ISCP extractor's ReprNative mode: encodePk -> decodePk must
// reproduce the exact native Go value.
func TestEncodePkNativeTypes(t *testing.T) {
	cases := []struct {
		name   string
		pkType types.T
		val    any
	}{
		{"date", types.T_date, types.Date(0x0135_7924)},
		{"datetime", types.T_datetime, types.Datetime(0x0123_4567_89AB_CDEF)},
		{"time", types.T_time, types.Time(-0x0011_2233_4455_6677)},
		{"timestamp", types.T_timestamp, types.Timestamp(0x7FFF_FFFF_FFFF_FFFF)},
		{"decimal64", types.T_decimal64, types.Decimal64(0xDEAD_BEEF_CAFE_F00D)},
		{"decimal128", types.T_decimal128, types.Decimal128{B0_63: 0x0102_0304_0506_0708, B64_127: 0x1112_1314_1516_1718}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := encodePk(int32(c.pkType), c.val)
			if err != nil {
				t.Fatalf("encodePk(%s): %v", c.name, err)
			}
			got, err := decodePk(int32(c.pkType), b)
			if err != nil {
				t.Fatalf("decodePk(%s): %v", c.name, err)
			}
			if !reflect.DeepEqual(got, c.val) {
				t.Fatalf("%s round-trip: got %#v (%T), want %#v (%T)", c.name, got, got, c.val, c.val)
			}
		})
	}
}

// TestWandCdcNativePkRoundTrip exercises the whole CDC channel blob (WandCdc.Encode ->
// DecodeWandCdc) with native temporal / decimal pks, mirroring what the ISCP sinker
// ships once extractRowFromVector delivers them natively (ReprNative).
func TestWandCdcNativePkRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		pkType types.T
		pk     any
	}{
		{"date", types.T_date, types.Date(0x0135_7924)},
		{"datetime", types.T_datetime, types.Datetime(0x0123_4567_89AB_CDEF)},
		{"timestamp", types.T_timestamp, types.Timestamp(0x7FFF_FFFF_FFFF_FFFF)},
		{"decimal64", types.T_decimal64, types.Decimal64(0xDEAD_BEEF_CAFE_F00D)},
		{"decimal128", types.T_decimal128, types.Decimal128{B0_63: 1000, B64_127: 7}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cdc := NewWandCdc(int32(c.pkType))
			cdc.Insert(c.pk, "arbitrary text")
			cdc.Delete(c.pk)
			blob, err := cdc.Encode()
			if err != nil {
				t.Fatalf("Encode(%s): %v", c.name, err)
			}
			got, err := DecodeWandCdc(blob)
			if err != nil {
				t.Fatalf("DecodeWandCdc(%s): %v", c.name, err)
			}
			if len(got.Events) != 2 {
				t.Fatalf("%s: want 2 events, got %d", c.name, len(got.Events))
			}
			for _, e := range got.Events {
				if !reflect.DeepEqual(e.Pk, c.pk) {
					t.Fatalf("%s pk round-trip: got %#v (%T), want %#v (%T)", c.name, e.Pk, e.Pk, c.pk, c.pk)
				}
			}
		})
	}
}
