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

package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeMemoryLimit(t *testing.T) {
	require.Equal(t, uint64(8), runtimeMemoryLimit(
		func() uint64 { return 16 },
		func() (uint64, error) { return 8, nil },
	))
	require.Equal(t, uint64(16), runtimeMemoryLimit(
		func() uint64 { return 16 },
		func() (uint64, error) { return 32, nil },
	))
	require.Equal(t, uint64(16), runtimeMemoryLimit(
		func() uint64 { return 16 },
		func() (uint64, error) { return 0, nil },
	))
	require.Equal(t, uint64(16), runtimeMemoryLimit(
		func() uint64 { return 16 },
		func() (uint64, error) { return 8, errors.New("no cgroup") },
	))
}
