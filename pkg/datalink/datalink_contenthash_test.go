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

package datalink

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// procWithAccount returns a process whose context carries an account id, plus
// that id. A pinned datalink resolves its CAS namespace from this account.
func procWithAccount(t *testing.T) (*process.Process, uint32) {
	proc := testutil.NewProc(t)
	accountID, err := defines.GetAccountId(proc.Ctx)
	require.NoError(t, err)
	return proc, accountID
}

// A pinned datalink resolves to the immutable CAS object path, never the live path.
func TestParseDatalinkContentHashResolvesToCAS(t *testing.T) {
	proc, accountID := procWithAccount(t)
	hash := strings.Repeat("a", 64)
	moUrl, offsetSize, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash, proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID, hash), moUrl)
	require.NotEqual(t, "/a/b/c/1.txt", moUrl) // must not fall back to the live path
	require.Equal(t, []int64{0, -1}, offsetSize)
}

// An uppercase hash is normalized to lowercase so it matches the stored sha256 hex.
func TestParseDatalinkContentHashNormalizesCase(t *testing.T) {
	proc, accountID := procWithAccount(t)
	moUrl, _, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+strings.Repeat("A", 64), proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID, strings.Repeat("a", 64)), moUrl)
}

// offset/size still apply on top of a pinned object.
func TestParseDatalinkContentHashWithOffsetSize(t *testing.T) {
	proc, accountID := procWithAccount(t)
	hash := strings.Repeat("a", 64)
	moUrl, offsetSize, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash+"&offset=1&size=2", proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID, hash), moUrl)
	require.Equal(t, []int64{1, 2}, offsetSize)
}

// A pinned stage datalink does not need the live stage resolution.
func TestParseDatalinkContentHashStageNoLiveResolution(t *testing.T) {
	proc, accountID := procWithAccount(t)
	hash := strings.Repeat("b", 64)
	moUrl, _, err := ParseDatalink("stage://st/doc.txt?contenthash="+hash, proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID, hash), moUrl)
}

// A pinned datalink resolved in different account contexts maps to distinct CAS
// keys: the contenthash alone does not address a globally shared object.
func TestParseDatalinkContentHashIsAccountScoped(t *testing.T) {
	proc, accountID := procWithAccount(t)
	hash := strings.Repeat("a", 64)

	moUrl, _, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash, proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID, hash), moUrl)

	// a different account resolving the same URL gets a different CAS key
	proc.Ctx = defines.AttachAccountId(proc.Ctx, accountID+1)
	otherUrl, _, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash, proc)
	require.NoError(t, err)
	require.Equal(t, CASKey(accountID+1, hash), otherUrl)
	require.NotEqual(t, moUrl, otherUrl)
}

// Security: a live URL must not be allowed to resolve into the reserved internal
// datalink_cas/ prefix. Otherwise file://datalink_cas/<acct>/<hh>/<hash> would be
// treated as a pinned CAS key by casHashFromKey and read directly from SHARED,
// letting one account read another account's pinned blob by guessing the path.
func TestParseDatalinkRejectsReservedCASPrefix(t *testing.T) {
	proc, accountID := procWithAccount(t)
	raw := "file://" + CASKey(accountID, strings.Repeat("a", 64)) // file://datalink_cas/<acct>/aa/aa..

	_, _, err := ParseDatalink(raw, proc)
	require.Error(t, err, "ParseDatalink must reject the reserved datalink_cas/ prefix")

	_, err = NewDatalink(raw, proc)
	require.Error(t, err, "NewDatalink must reject the reserved datalink_cas/ prefix")
}

// Mixed-case duplicate contenthash params fold non-deterministically (the winning
// value depends on Go map iteration order), so they must be rejected outright.
func TestParseDatalinkRejectsDuplicateContentHash(t *testing.T) {
	proc, _ := procWithAccount(t)
	raw := "file:///x.txt?contenthash=" + strings.Repeat("a", 64) +
		"&ContentHash=" + strings.Repeat("b", 64)
	_, _, err := ParseDatalink(raw, proc)
	require.Error(t, err)
}

func TestParseDatalinkInvalidContentHash(t *testing.T) {
	cases := []string{
		"file:///a/b/c/1.txt?contenthash=abc",                        // too short
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("a", 63), // 63 chars
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("a", 65), // 65 chars
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("z", 64), // non-hex
		"file:///a/b/c/1.txt?contenthash=",                           // empty
	}
	for _, c := range cases {
		_, _, err := ParseDatalink(c, nil)
		require.Error(t, err, c)
	}
}
