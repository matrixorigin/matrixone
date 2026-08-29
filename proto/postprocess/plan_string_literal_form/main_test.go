// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
