// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInternalTableCollationDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name                       string
		identity, revision, format uint32
		expected                   string
		failure, null              bool
		malformed                  string
	}{
		{name: "legacy presentation", expected: "utf8mb4_general_ci"},
		{name: "binary", identity: 1, expected: "binary"},
		{name: "utf8mb4 bin", identity: 2, expected: "utf8mb4_bin"},
		{name: "utf8mb4 general", identity: 3, expected: "utf8mb4_general_ci"},
		{name: "native mb3", identity: 9, revision: 1, expected: "utf8_unicode_ci"},
		{name: "native mb4", identity: 10, revision: 1, expected: "utf8mb4_unicode_ci"},
		{name: "null", null: true},
		{name: "unknown identity", identity: 256, failure: true},
		{name: "unknown revision", identity: 2, revision: 256, failure: true},
		{name: "disabled format", identity: 2, format: 1, failure: true},
		{name: "unknown format", identity: 2, format: 256, failure: true},
		{name: "missing native revision", identity: 10, failure: true},
		{name: "legacy with revision", revision: 1, failure: true},
		{name: "malformed metadata", malformed: "\xff", failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := (&api.SchemaExtra{DefaultCharset: tc.identity, CollationVersion: tc.revision, KeyFormat: tc.format}).Marshal()
			require.NoError(t, err)
			if tc.malformed != "" {
				data = []byte(tc.malformed)
			}
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{string(data)}, []bool{tc.null})},
				NewFunctionTestResult(types.New(types.T_varchar, 64, 0), tc.failure, []string{tc.expected}, []bool{tc.null}),
				builtInInternalTableCollation)
			ok, info := caseTest.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
