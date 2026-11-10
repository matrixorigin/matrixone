// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestM0RemoteReadSnapshotWatermark(t *testing.T) {
	snapshot := timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 3}
	for _, tc := range []struct {
		name   string
		result timestamp.Timestamp
		ok     bool
	}{
		{name: "at_snapshot", result: snapshot, ok: true},
		{name: "ahead", result: snapshot.Next(), ok: true},
		{name: "applied_predecessor_missing", result: snapshot.Prev()},
		{name: "missing_waiter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &m0WatermarkClient{result: tc.result}
			err := waitM0RemoteReadSnapshot(context.Background(), c, snapshot)
			require.Equal(t, snapshot.Prev(), c.want)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.Error(t, waitM0RemoteReadSnapshot(context.Background(), nil, snapshot))
	require.Error(t, waitM0RemoteReadSnapshot(context.Background(), &m0WatermarkClient{}, timestamp.Timestamp{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waitM0RemoteReadSnapshot(ctx, &m0WatermarkClient{}, snapshot), context.Canceled)
}
