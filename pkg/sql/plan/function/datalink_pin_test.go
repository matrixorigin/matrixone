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
	"github.com/matrixorigin/matrixone/pkg/datalink"
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

// An already-pinned datalink whose CAS blob exists for the calling account is
// returned unchanged (idempotent): no live read, no second CAS write.
func TestDatalinkPinIdempotentWhenBlobExists(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("v1"), 0o600))

	proc := testutil.NewProc(t)
	liveURL := "file://" + filePath
	pinned := pinnedURLOf(liveURL, []byte("v1"))

	// first pin materializes the CAS blob for this account
	first := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{liveURL}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinned}, []bool{false}),
		DatalinkPin)
	s, info := first.Run()
	require.True(t, s, info)

	// re-pinning the already-pinned URL returns it unchanged: the blob is present.
	again := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{pinned}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinned}, []bool{false}),
		DatalinkPin)
	s, info = again.Run()
	require.True(t, s, info)
}

// Re-pinning an already-pinned URL whose CAS blob is absent for the calling
// account re-materializes it from the live source when the live bytes still hash
// to the declared contenthash. The blob is then readable for this account (under
// the old unconditional early-return it was never stored, so a read would fail).
func TestDatalinkPinRepinsAbsentBlobFromLive(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	content := []byte("frozen")
	require.NoError(t, os.WriteFile(filePath, content, 0o600))

	proc := testutil.NewProc(t)
	liveURL := "file://" + filePath
	pinned := pinnedURLOf(liveURL, content) // contenthash = sha256(content); blob not yet stored

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{pinned}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{pinned}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)

	dl, err := datalink.NewDatalink(pinned, proc)
	require.NoError(t, err)
	got, err := dl.GetBytes(proc)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// Re-pinning an already-pinned URL whose CAS blob is absent for the calling
// account re-reads the live source. If the live bytes no longer hash to the
// declared contenthash, the requested version is unavailable, so pin errors out
// rather than silently pinning different bytes under the old hash.
func TestDatalinkPinRepinRejectsChangedLiveContent(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("actual-bytes"), 0o600))

	proc := testutil.NewProc(t)
	// a valid-format hash that does not match the live file; no CAS blob exists.
	mismatch := strings.Repeat("a", 64)
	pinned := "file://" + filePath + "?contenthash=" + mismatch

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{pinned}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), true,
			[]string{""}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

func TestDatalinkPinRejectsInvalidExistingContentHash(t *testing.T) {
	proc := testutil.NewProc(t)

	cases := []struct {
		name string
		url  string
	}{
		{
			name: "short",
			url:  "file:///does/not/matter.txt?contenthash=abc",
		},
		{
			name: "non-hex",
			url:  "file:///does/not/matter.txt?contenthash=" + strings.Repeat("g", 64),
		},
		{
			name: "empty",
			url:  "file:///does/not/matter.txt?contenthash=",
		},
		{
			name: "mixed-case-key",
			url:  "file:///does/not/matter.txt?ContentHash=bad",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
					[]string{c.url}, []bool{false})},
				NewFunctionTestResult(types.T_datalink.ToType(), true,
					[]string{""}, []bool{false}),
				DatalinkPin)
			s, info := tc.Run()
			require.True(t, s, info)
		})
	}
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

// Mixed-case offset/size params must be stripped after the sliced bytes are
// frozen, so the pinned URL carries only the contenthash. Otherwise a later read
// would slice the (already-sliced) CAS object again and return wrong bytes.
func TestDatalinkPinStripsMixedCaseSlice(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "doc.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("Hello world!"), 0o600))

	proc := testutil.NewProc(t)
	liveURL := "file://" + filePath
	// ?Offset=6&Size=5 selects "world"; the pinned URL must address only that
	// content by hash, with no residual Offset/Size.
	input := liveURL + "?Offset=6&Size=5"
	expected := pinnedURLOf(liveURL, []byte("world"))

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{input}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), false,
			[]string{expected}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// pin refuses to copy more than one blob's worth of bytes into memory, mirroring
// load_file's MaxBlobLen guard, rather than risking OOM on a huge object.
func TestDatalinkPinRejectsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "big.bin")
	f, err := os.Create(filePath)
	require.NoError(t, err)
	// sparse file just over MaxBlobLen: StatFile reports the full size without
	// allocating it, so the guard must reject before reading anything.
	require.NoError(t, f.Truncate(int64(types.MaxBlobLen)+1))
	require.NoError(t, f.Close())

	proc := testutil.NewProc(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_datalink.ToType(),
			[]string{"file://" + filePath}, []bool{false})},
		NewFunctionTestResult(types.T_datalink.ToType(), true,
			[]string{""}, []bool{false}),
		DatalinkPin)
	s, info := tc.Run()
	require.True(t, s, info)
}

// save_file() must reject writes to a pinned (contenthash) datalink: the pinned
// value addresses an immutable CAS object, so writing through it would target the
// internal CAS key rather than any real external path.
func TestWriteFileDatalinkRejectsPinned(t *testing.T) {
	proc := testutil.NewProc(t)
	pinned := "file:///does/not/matter.txt?contenthash=" + strings.Repeat("a", 64)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_datalink.ToType(), []string{pinned}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"new content"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), true, []int64{0}, []bool{false}),
		WriteFileDatalink)
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
