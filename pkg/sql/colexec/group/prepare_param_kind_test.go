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

package group

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPrepareParamKindStateCodec(t *testing.T) {
	tests := []struct {
		name    string
		kind    vector.PrepareParamKind
		seen    bool
		encoded byte
	}{
		{name: "unseen", kind: vector.PrepareParamNone, seen: false, encoded: 0},
		{name: "observed-string", kind: vector.PrepareParamNone, seen: true, encoded: 1},
		{name: "integer", kind: vector.PrepareParamInteger, seen: true, encoded: 2},
		{name: "float", kind: vector.PrepareParamFloat, seen: true, encoded: 3},
		{name: "decimal", kind: vector.PrepareParamDecimal, seen: true, encoded: 4},
		{name: "boolean", kind: vector.PrepareParamBoolean, seen: true, encoded: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodePrepareParamKindState(tc.kind, tc.seen)
			require.Equal(t, tc.encoded, encoded)
			require.Len(t, []byte{encoded}, 1)

			kind, seen, ok := decodePrepareParamKindState(encoded)
			require.True(t, ok)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.seen, seen)
		})
	}

	for _, encoded := range []byte{6, 255} {
		kind, seen, ok := decodePrepareParamKindState(encoded)
		require.False(t, ok)
		require.False(t, seen)
		require.Equal(t, vector.PrepareParamNone, kind)
	}
}
