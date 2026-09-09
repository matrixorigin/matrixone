// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package udf

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type closableRuntime struct {
	language string
	closed   atomic.Int32
}

func (r *closableRuntime) Language() string { return r.language }

func (r *closableRuntime) Execute(
	context.Context,
	*Invocation,
	vector.FunctionResultWrapper,
	*mpool.MPool,
) error {
	return nil
}

func (r *closableRuntime) Close() error {
	r.closed.Add(1)
	return nil
}

func TestRuntimeRegistryClosesOwnedRuntimesOnce(t *testing.T) {
	python := &closableRuntime{language: LanguagePython}
	runtime, err := NewRuntime(python)
	require.NoError(t, err)
	closer, ok := runtime.(RuntimeCloser)
	require.True(t, ok)
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
	require.Equal(t, int32(1), python.closed.Load())
}
