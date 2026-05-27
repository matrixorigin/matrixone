// Copyright 2024 Matrix Origin
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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// pinnedURLOf returns the datalink URL that datalink_pin is expected to emit for
// a live file:// reference: the live path plus ?contenthash=<sha256(content)>.
func pinnedURLOf(liveURL string, content []byte) string {
	sum := sha256.Sum256(content)
	return liveURL + "?contenthash=" + hex.EncodeToString(sum[:])
}

// datalink_pin freezes the referenced bytes and returns a datalink carrying the
// content hash of those bytes.
func TestDatalinkPinReturnsContentHash(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("v1"), 0o600))

	proc := testutil.NewProc(t) // NewProc(t) backs SHARED with LocalFS, matching standalone
	liveURL := "file://" + filePath

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinnedURLOf(liveURL, []byte("v1"))}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// The core issue scenario: after an out-of-band overwrite, reading the pinned
// datalink still returns the original bytes, while the live datalink returns new.
func TestDatalinkPinReproducesBytesAfterOverwrite(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("v1"), 0o600))

	proc := testutil.NewProc(t)
	liveURL := "file://" + filePath
	pinnedURL := pinnedURLOf(liveURL, []byte("v1"))

	// pin: performs the CAS write into the SHARED store
	pinTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinnedURL}, []bool{false}),
		DatalinkPin)
	s, info := pinTC.Run()
	require.True(t, s, info)

	// out-of-band overwrite of the external file
	require.NoError(t, os.WriteFile(filePath, []byte("v2-overwritten"), 0o600))

	// reading the pinned datalink returns the original frozen bytes
	pinnedReadTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{pinnedURL}, []bool{false})},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{"v1"}, []bool{false}),
		LoadFileDatalink)
	s, info = pinnedReadTC.Run()
	require.True(t, s, info)

	// reading the live datalink returns the new bytes (pin did not freeze live)
	liveReadTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{"v2-overwritten"}, []bool{false}),
		LoadFileDatalink)
	s, info = liveReadTC.Run()
	require.True(t, s, info)
}

// An already-pinned datalink is returned unchanged (idempotent): no live read,
// no second CAS write. The live file need not even exist.
func TestDatalinkPinIdempotent(t *testing.T) {
	proc := testutil.NewProc(t)
	alreadyPinned := "file:///does/not/matter.txt?contenthash=" + strings.Repeat("a", 64)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{alreadyPinned}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{alreadyPinned}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// NULL in, NULL out.
func TestDatalinkPinNull(t *testing.T) {
	proc := testutil.NewProc(t)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{""}, []bool{true})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{""}, []bool{true}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// When the external file cannot be read, pin must error out rather than silently
// producing an un-pinned or empty value.
func TestDatalinkPinMissingLiveFileErrors(t *testing.T) {
	proc := testutil.NewProc(t)
	liveURL := "file:///nonexistent/datalink/pin/xyz.txt"

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), true,
			[]string{""}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// pin accepts a plain varchar URL too (implicitly treated as a datalink).
func TestDatalinkPinAcceptsVarchar(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0o600))

	proc := testutil.NewProc(t)
	liveURL := "file://" + filePath

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinnedURLOf(liveURL, []byte("hello"))}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}
