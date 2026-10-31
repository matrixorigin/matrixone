//go:build !mo_m0

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

package readutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestM0DefaultLocationHookDoesNotProveMissingColumns(t *testing.T) {
	require.False(t, activeM0ReadLocationProofEnabled)
	proven, proof, err := activeM0ReadLocationProof(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	require.False(t, proven)
	require.Zero(t, proof)
}
