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

package udf

import (
	"encoding/base64"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

// The identical fixture is consumed by Operator's independent wire decoder.
// This prevents successful status alone from concealing fault-enum drift.
func TestPythonStatusWireContract(t *testing.T) {
	expected := map[string][]string{
		"": {RuntimeStatusReasonReady}, RuntimeStatusDisabled: {RuntimeStatusReasonRuntimeDisabled}, RuntimeStatusNotAllowed: {RuntimeStatusReasonUnisolatedNotAllowed},
		RuntimeStatusUnavailable: {RuntimeStatusReasonWorkerUnavailable, RuntimeStatusReasonStatusUnavailable}, RuntimeStatusContractMismatch: {RuntimeStatusReasonCapabilityMismatch}, RuntimeStatusTimeout: {RuntimeStatusReasonRequestTimeout}, RuntimeStatusClosed: {RuntimeStatusReasonRuntimeClosed}, RuntimeStatusInvalid: {RuntimeStatusReasonInvalidLanguage}, RuntimeStatusInternal: {RuntimeStatusReasonRuntimeError},
	}
	raw, err := os.ReadFile("testdata/python_status_wire.json")
	require.NoError(t, err)
	var rows []struct{ Class, Reason, Wire string }
	require.NoError(t, json.Unmarshal(raw, &rows))
	seen := map[string][]string{}
	for _, row := range rows {
		wire, err := base64.StdEncoding.DecodeString(row.Wire)
		require.NoError(t, err)
		var response query.Response
		require.NoError(t, response.Unmarshal(wire))
		require.Equal(t, query.CmdMethod_GetPythonUdfStatus, response.CmdMethod)
		require.Equal(t, row.Class, response.GetPythonUdfStatus.ErrorClass)
		require.Equal(t, row.Reason, response.GetPythonUdfStatus.Reason)
		require.Equal(t, row.Class == "", response.GetPythonUdfStatus.Ready)
		seen[row.Class] = append(seen[row.Class], row.Reason)
	}
	require.Equal(t, expected, seen)
}
