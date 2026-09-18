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

package cnservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

type autoIncrementLogtailBarrier struct {
	frontier timestamp.Timestamp
	err      error
}

func (b autoIncrementLogtailBarrier) AcquireLogtailReadBarrier(context.Context) (timestamp.Timestamp, error) {
	return b.frontier, b.err
}

func TestAcquireIncrLogtailReadBarrier(t *testing.T) {
	frontier := timestamp.Timestamp{PhysicalTime: 42}
	got, err := acquireIncrLogtailReadBarrier(t.Context(), autoIncrementLogtailBarrier{frontier: frontier})
	require.NoError(t, err)
	require.Equal(t, frontier, got)

	_, err = acquireIncrLogtailReadBarrier(t.Context(), struct{}{})
	require.ErrorContains(t, err, "logtail read barrier")
}
