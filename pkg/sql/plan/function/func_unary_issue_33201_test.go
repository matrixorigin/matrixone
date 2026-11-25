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

package function

import (
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Issue 33201: a MultiPoint that contains an empty Point must round-trip
// through ST_AsWKB / ST_GeomFromWKB (WKB reader, WKT re-rendering, and storage
// validation) like any other geometry, and independently produced standard WKB
// for it must be accepted. The fix is validator-only: bare EMPTY member
// acceptance in validateMultiPointGeometryTextContent. The WKB writer keeps
// its established bytes and the reader already accepts any NaN/NaN
// empty-point payload, so both MatrixOne's own encoding and the GEOS encoding
// are covered as distinct inputs.
const (
	issue33201MixedWKT = "MULTIPOINT(EMPTY,1 2)"
	// Standard little-endian WKB of MULTIPOINT(EMPTY,(1 2)) as encoded by
	// GEOS 3.13.1 / Shapely 2.1.2 and supplied with the issue: an empty
	// Point (NaN/NaN, canonical quiet-NaN payload) followed by Point(1 2).
	issue33201GEOSWKBHex = "0104000000020000000101000000000000000000f87f000000000000f87f0101000000000000000000f03f0000000000000040"
	// Established MatrixOne ST_AsWKB output for the same geometry,
	// unchanged by this fix: the empty-point sentinel keeps the
	// Go-convention NaN payload bits (0x7FF8000000000001).
	issue33201PreviousWKBHex = "0104000000020000000101000000010000000000F87F010000000000F87F0101000000000000000000F03F0000000000000040"
	// Established MatrixOne encoding of the standalone empty point control.
	issue33201PointEmptyWKBHex = "0101000000010000000000f87f010000000000f87f"
)

func issue33201decodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestStAsWKBEmptyMemberMultiPointKeepsEstablishedBytes pins the producer
// side: ST_AsWKB emits the same established bytes as before the fix — for the
// mixed MultiPoint and the standalone empty point — on both stored widths, so
// the repair carries no serialization change.
func TestStAsWKBEmptyMemberMultiPointKeepsEstablishedBytes(t *testing.T) {
	proc := testutil.NewProcess(t)
	want := string(issue33201decodeHex(t, issue33201PreviousWKBHex))
	for _, inputType := range []types.T{types.T_geometry, types.T_geometry32} {
		t.Run(inputType.String(), func(t *testing.T) {
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(inputType.ToType(), []string{issue33201MixedWKT}, []bool{false})},
				NewFunctionTestResult(types.T_blob.ToType(), false, []string{want}, []bool{false}), StAsWKB)
			ok, info := tc.RunAndFree()
			require.True(t, ok, info)
		})
	}
	pointTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT EMPTY"}, []bool{false})},
		NewFunctionTestResult(types.T_blob.ToType(), false,
			[]string{string(issue33201decodeHex(t, issue33201PointEmptyWKBHex))}, []bool{false}), StAsWKB)
	ok, info := pointTC.RunAndFree()
	require.True(t, ok, info)
}

// TestStGeomFromWKBEmptyMemberMultiPoint pins the reader side of the contract:
// the GEOS bytes and MatrixOne's own ST_AsWKB output decode to the same mixed
// geometry, on every bound input type, and keep SRID metadata behavior on the
// two-argument constructor.
func TestStGeomFromWKBEmptyMemberMultiPoint(t *testing.T) {
	proc := testutil.NewProcess(t)
	geosWKB := string(issue33201decodeHex(t, issue33201GEOSWKBHex))
	// The actual ST_AsWKB result for the mixed geometry: the established
	// MatrixOne encoding, distinct from the GEOS bytes (the empty-point NaN
	// payload bits differ), used as a separate accepted input.
	stored := encodeGeometryPayload(issue33201MixedWKT, 0, false)
	asWKB, _, err := GeometryPayloadToStandardWKB(stored, types.T_geometry.ToType())
	require.NoError(t, err)
	ownWKB := string(asWKB)
	require.NotEqual(t, geosWKB, ownWKB)
	for _, inputType := range []types.T{types.T_varchar, types.T_blob, types.T_varbinary} {
		for _, tc := range []struct {
			name string
			wkb  string
		}{
			{"geos bytes", geosWKB},
			{"own aswkb bytes", ownWKB},
		} {
			t.Run(inputType.String()+" "+tc.name, func(t *testing.T) {
				fc := NewFunctionTestCase(proc,
					[]FunctionTestInput{NewFunctionTestInput(inputType.ToType(), []string{tc.wkb}, []bool{false})},
					NewFunctionTestResult(types.T_geometry.ToType(), false, []string{issue33201MixedWKT}, []bool{false}), StGeomFromWKB)
				ok, info := fc.RunAndFree()
				require.True(t, ok, info)
			})
		}
	}
	sridTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{geosWKB}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{4326}, []bool{false}),
		},
		NewFunctionTestResult(types.T_geometry.ToType(), false, []string{issue33201MixedWKT}, []bool{false}), StGeomFromWKBWithSRID)
	ok, info := sridTC.RunAndFree()
	require.True(t, ok, info)
}

// TestStGeomFromTextEmptyMemberMultiPoint pins the WKT entry path for the same
// form, including member keyword case-insensitivity.
func TestStGeomFromTextEmptyMemberMultiPoint(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, wkt := range []string{"MULTIPOINT(EMPTY,1 2)", "multipoint(empty,1 2)"} {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{wkt}, []bool{false})},
			NewFunctionTestResult(types.T_geometry.ToType(), false, []string{issue33201MixedWKT}, []bool{false}), StGeomFromText)
		ok, info := tc.RunAndFree()
		require.True(t, ok, info)
	}
}

// TestValidateMultiPointTextEmptyMember pins the storage validator directly:
// the bare EMPTY member that geo.WriteWKT emits is valid, while lookalike and
// non-point members keep being rejected.
func TestValidateMultiPointTextEmptyMember(t *testing.T) {
	require.NoError(t, validateGeometryTextForStorage("MULTIPOINT(EMPTY,1 2)", 0))
	require.NoError(t, validateGeometryTextForStorage("MULTIPOINT(EMPTY)", 0))
	require.NoError(t, validateGeometryTextForStorage("MULTIPOINT(EMPTY,EMPTY,0 0)", 0))
	require.ErrorContains(t, validateGeometryTextForStorage("MULTIPOINT(EMPTYX,1 2)", 0), "invalid geometry payload")
	require.ErrorContains(t, validateGeometryTextForStorage("MULTIPOINT(NAN,1 2)", 0), "invalid geometry payload")
	require.ErrorContains(t, validateGeometryTextForStorage("MULTIPOINT(LINESTRING EMPTY,1 2)", 0), "invalid geometry payload")
}

// TestStGeomFromWKBEmptyMemberMultiPointStillRejectsInvalidWKB pins the
// truncated and malformed rejection classes around the accepted form.
func TestStGeomFromWKBEmptyMemberMultiPointStillRejectsInvalidWKB(t *testing.T) {
	proc := testutil.NewProcess(t)
	full := issue33201decodeHex(t, issue33201GEOSWKBHex)
	for _, tc := range []struct {
		name string
		wkb  string
	}{
		{"truncated multipoint", string(full[:10])},
		{"truncated empty point", string(full[:12])},
		{"unsupported geometry type", string(issue33201decodeHex(t, "0108000000"))},
		{"single NaN ordinate", string(issue33201decodeHex(t, "0101000000000000000000f87f000000000000f03f"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.wkb}, []bool{false})},
				NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil), StGeomFromWKB)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

// TestStGeomFromWKBControlsRoundTrip pins that the geometries around the fix
// keep their round-trip behavior, including an empty MULTIPOINT member nested
// inside a collection.
func TestStGeomFromWKBControlsRoundTrip(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, wkt := range []string{
		"MULTIPOINT(0 0,1 2)",
		"POINT EMPTY",
		"GEOMETRYCOLLECTION(POINT EMPTY)",
		"GEOMETRYCOLLECTION(MULTIPOINT(EMPTY,1 2),POINT(3 3))",
	} {
		t.Run(wkt, func(t *testing.T) {
			wkb := string(encodeGeometryPayload(wkt, 0, false))
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{wkb}, []bool{false})},
				NewFunctionTestResult(types.T_geometry.ToType(), false, []string{wkt}, []bool{false}), StGeomFromWKB)
			ok, info := tc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
