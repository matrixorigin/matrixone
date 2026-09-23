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

package process

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveMaxDigestLengthValidatesAndCaches(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		err   error
		want  int
		bad   string
	}{
		{name: "default", value: nil, want: DefaultMaxDigestLength},
		{name: "explicit zero", value: int64(0), want: 0},
		{name: "maximum", value: int64(MaximumMaxDigestLength), want: MaximumMaxDigestLength},
		{name: "negative", value: int64(-1), bad: "invalid max_digest_length"},
		{name: "wrong type", value: "1024", bad: "invalid max_digest_length"},
		{name: "resolver error", err: errors.New("unavailable"), bad: "resolve max_digest_length"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := &Process{Base: &BaseProcess{}}
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return test.value, test.err })
			got, err := ResolveMaxDigestLengthWithError(proc)
			if test.bad != "" {
				require.ErrorContains(t, err, test.bad)
				require.False(t, proc.Base.SessionInfo.MaxDigestLengthSet)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.True(t, proc.Base.SessionInfo.MaxDigestLengthSet)
			require.Equal(t, int64(test.want), proc.Base.SessionInfo.MaxDigestLength)
		})
	}
}

func TestMaxDigestLengthSnapshotIsSharedAndResetPerStatement(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	value, calls := int64(0), 0
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		calls++
		return value, nil
	})
	require.Equal(t, 0, ResolveMaxDigestLength(proc))
	value = 19
	require.Equal(t, 0, ResolveMaxDigestLength(proc.NewNoContextChildProc(0)))
	require.Equal(t, 1, calls)
	proc.ResetMaxDigestLengthSnapshot()
	require.Equal(t, 19, ResolveMaxDigestLength(proc.NewNoContextChildProc(0)))
	require.Equal(t, 2, calls)
}

func TestResolveMaxDigestLengthUsesRemoteSnapshotAndLegacyDefault(t *testing.T) {
	for _, test := range []struct {
		name  string
		value int64
		set   bool
		want  int
	}{
		{name: "legacy absent", want: DefaultMaxDigestLength},
		{name: "explicit zero", set: true, want: 0},
		{name: "custom", value: 41, set: true, want: 41},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{
				MaxDigestLength: test.value, MaxDigestLengthSet: test.set,
			}}}
			require.Equal(t, test.want, ResolveMaxDigestLength(proc))
		})
	}

	malformed := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{
		MaxDigestLength: MaximumMaxDigestLength + 1, MaxDigestLengthSet: true,
	}}}
	_, err := ResolveMaxDigestLengthWithError(malformed)
	require.ErrorContains(t, err, "invalid max_digest_length")
}
