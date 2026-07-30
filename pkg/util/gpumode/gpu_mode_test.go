// Copyright 2026 Matrix Origin
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

package gpumode

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEffectiveGpuMode_NilResolver: nil resolver falls back to GpuMode.
func TestEffectiveGpuMode_NilResolver(t *testing.T) {
	require.Equal(t, GpuMode, EffectiveGpuMode(nil))
}

// TestEffectiveGpuMode_Int8On: resolver returning int8(1) flips to true.
func TestEffectiveGpuMode_Int8On(t *testing.T) {
	r := func(string, bool, bool) (any, error) { return int8(1), nil }
	require.True(t, EffectiveGpuMode(r))
}

// TestEffectiveGpuMode_Int8Off: resolver returning int8(0) returns false.
func TestEffectiveGpuMode_Int8Off(t *testing.T) {
	r := func(string, bool, bool) (any, error) { return int8(0), nil }
	require.False(t, EffectiveGpuMode(r))
}

// TestEffectiveGpuMode_ResolverError: any resolver error falls back to GpuMode.
func TestEffectiveGpuMode_ResolverError(t *testing.T) {
	r := func(string, bool, bool) (any, error) { return nil, errors.New("nope") }
	require.Equal(t, GpuMode, EffectiveGpuMode(r))
}

// TestEffectiveGpuMode_NilValue: resolver returning (nil, nil) falls back to GpuMode.
func TestEffectiveGpuMode_NilValue(t *testing.T) {
	r := func(string, bool, bool) (any, error) { return nil, nil }
	require.Equal(t, GpuMode, EffectiveGpuMode(r))
}

// TestEffectiveGpuMode_UnexpectedType: non-int8 resolver value falls back to GpuMode.
func TestEffectiveGpuMode_UnexpectedType(t *testing.T) {
	r := func(string, bool, bool) (any, error) { return "true", nil }
	require.Equal(t, GpuMode, EffectiveGpuMode(r))
}

// TestGpuModeDefaultInt8 mirrors GpuMode's current value (build-tag driven).
func TestGpuModeDefaultInt8(t *testing.T) {
	got := GpuModeDefaultInt8()
	if GpuMode {
		require.Equal(t, int8(1), got)
	} else {
		require.Equal(t, int8(0), got)
	}
}
