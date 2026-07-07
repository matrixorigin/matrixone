// Copyright 2024 Matrix Origin
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

package objectio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewOfflineFSKind covers NewOfflineFS format selection, including the guard
// that an unknown kind is rejected rather than silently defaulting to a format:
// DISK and DISK-V2 are incompatible and can silently misread, so a typo must
// fail loudly.
func TestNewOfflineFSKind(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Known kinds construct a fileservice.
	for _, kind := range []string{OfflineKindLocal, OfflineKindLocal2} {
		fs, err := NewOfflineFS(ctx, dir, kind)
		require.NoErrorf(t, err, "kind %q", kind)
		require.NotNilf(t, fs, "kind %q", kind)
	}

	// Unknown / typo'd kind must error, not fall through to a default format.
	for _, bad := range []string{"", "disk", "loca1", "LOCAL3"} {
		_, err := NewOfflineFS(ctx, dir, bad)
		require.Errorf(t, err, "kind %q should be rejected", bad)
		require.Contains(t, err.Error(), "unknown offline fs kind")
	}
}
