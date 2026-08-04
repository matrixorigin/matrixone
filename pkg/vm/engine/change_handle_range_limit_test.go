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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangeRangeLimitContextRoundTrip(t *testing.T) {
	base := context.Background()
	require.False(t, ChangeRangeLimitFromContext(base).Enabled())
	require.True(t, WithChangeRangeLimit(base, ChangeRangeLimit{}) == base)

	want := ChangeRangeLimit{MaxInMemoryRows: 7, MaxInMemoryBytes: 11}
	ctx := WithChangeRangeLimit(base, want)
	require.Equal(t, want, ChangeRangeLimitFromContext(ctx))
}
