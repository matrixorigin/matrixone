// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cnservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/stretchr/testify/require"
)

func TestBuildCNRuntimeDegradesPythonWhenArtifactServiceIsUnavailable(t *testing.T) {
	cfg := python.ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:50051",
	}
	runtime, err := buildCNRuntime(cfg, nil, nil)
	require.NoError(t, err)

	readiness, ok := runtime.(udf.RuntimeReadiness)
	require.True(t, ok)
	require.ErrorContains(t, readiness.CheckLanguageReady(context.Background(), udf.LanguagePython), "RESOURCE_UNAVAILABLE")

	status, ok := runtime.(udf.RuntimeStatusProvider)
	require.True(t, ok)
	snapshot := status.StatusSnapshot(context.Background(), udf.LanguagePython)
	require.True(t, snapshot.Enabled)
	require.False(t, snapshot.Ready)
	require.Equal(t, udf.RuntimeStatusUnavailable, snapshot.ErrorClass)
}

func TestBuildCNRuntimeDisabledDoesNotCreatePythonRuntime(t *testing.T) {
	runtime, err := buildCNRuntime(python.ClientConfig{}, nil, nil)
	require.NoError(t, err)
	readiness, ok := runtime.(udf.RuntimeReadiness)
	require.True(t, ok)
	require.ErrorContains(t, readiness.CheckLanguageReady(context.Background(), udf.LanguagePython), "not enabled")
}
