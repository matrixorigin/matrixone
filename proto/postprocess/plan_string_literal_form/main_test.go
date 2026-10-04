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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPatchAddsLiteralValidationAndPreservesLegacyDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.pb.go")
	input := `before
func (m *Expr) Unmarshal(dAtA []byte) error {
	return nil
}
func (m *FoldVal) Unmarshal(dAtA []byte) error {
}
proto.RegisterFile("proto/plan.proto", fileDescriptor_plan)
after
`
	require.NoError(t, os.WriteFile(path, []byte(input), 0o600))

	require.NoError(t, patch(path))
	patched, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(patched), "return m.validateOwnStringLiteralForm()")
	require.Contains(t, string(patched), `proto.RegisterFile("plan.proto", fileDescriptor_plan)`)
	require.NotContains(t, string(patched), `proto.RegisterFile("proto/plan.proto", fileDescriptor_plan)`)

	// Regeneration may invoke the postprocessor more than once; the second pass
	// must preserve both transformations without duplicating either one.
	require.NoError(t, patch(path))
	patchedAgain, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, patched, patchedAgain)
}

func TestPatchRejectsUnexpectedGeneratedShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.pb.go")
	require.NoError(t, os.WriteFile(path, []byte("package plan\n"), 0o600))
	require.ErrorContains(t, patch(path), "generated Expr.Unmarshal boundary not found")
}
