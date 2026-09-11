// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/stretchr/testify/require"
)

func TestDefinitionFingerprintCanonicalContractExcludesSource(t *testing.T) {
	argument, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	returnType, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	artifactDigest := udf.PythonInlineArtifactDigest("add", "def add(ctx, value): return value")
	first, err := DefinitionFingerprint(
		udf.PythonDefinitionSchemaVersion,
		"add", ModeScalar, NullCallHandler,
		udf.PythonABIContract, udf.PythonAdapterVersion,
		artifactDigest, strings.Repeat("e", 64), udf.PythonSDKVersion,
		[]TypeDescriptor{argument}, returnType,
	)
	require.NoError(t, err)
	require.Equal(t, "d0348f99deac2a1518fdf0f32d37a4e7924acbce450c7328d1e83d67d5f7e635", first)
	require.True(t, udf.IsSHA256Digest(first))

	canonical, err := json.Marshal(definitionFingerprintBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "add",
		Mode:                    ModeScalar,
		NullPolicy:              NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		ArtifactDigest:          artifactDigest,
		EnvironmentDigest:       strings.Repeat("e", 64),
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []TypeDescriptor{argument},
		ReturnType:              &returnType,
	})
	require.NoError(t, err)
	require.NotContains(t, string(canonical), `"source"`)
	require.Contains(t, string(canonical), `"artifact_digest"`)
}
