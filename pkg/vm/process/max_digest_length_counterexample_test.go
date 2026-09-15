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

// TestBuildProcessInfoKeepsOrdinaryQueriesIndependentOfDigestSettings guards
// the scope boundary: ordinary remote queries may use a resolver that knows
// sql_mode but does not implement the digest-only max_digest_length setting.
func TestBuildProcessInfoKeepsOrdinaryQueriesIndependentOfDigestSettings(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		if name == "max_digest_length" {
			return nil, errors.New("max_digest_length unavailable")
		}
		return nil, errors.New("unexpected variable")
	})

	info, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.True(t, info.SessionInfo.MaxDigestLengthSet)
	require.Equal(t, int64(DefaultMaxDigestLength), info.SessionInfo.MaxDigestLength)
}
