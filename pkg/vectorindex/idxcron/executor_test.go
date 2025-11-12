// Copyright 2022 Matrix Origin
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

package idxcron

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestCheckIndexUpdatable(t *testing.T) {

	type TestTask struct {
		jstr      string
		dsize     uint64
		nlists    int64
		ts        types.Timestamp
		createdAt types.Timestamp
		expected  bool
	}

	tasks := []TestTask{
		{
			// just CreatedAt and skip update
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Unix()),
			expected:  false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			expected:  true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
	"kmeans_max_iteration":{"t":"I", "v":4},
	"ivf_threads_build":{"t":"I", "v":8}
	}}`,
			dsize:    uint64(1000000),
			nlists:   int64(1000),
			ts:       types.UnixToTimestamp(0),
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-1 * time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: true,
		},
	}

	for _, ta := range tasks {

		m, err := sqlexec.NewMetadataFromJson(ta.jstr)
		require.Nil(t, err)

		info := IndexUpdateTaskInfo{
			DbName:       "db",
			TableName:    "table",
			IndexName:    "index",
			Action:       Action_Ivfflat_Reindex,
			AccountId:    uint32(0),
			TableId:      uint64(100),
			Metadata:     m,
			LastUpdateAt: &ta.ts,
			CreatedAt:    ta.createdAt,
		}

		ok, err := info.checkIndexUpdatable(context.Background(), ta.dsize, ta.nlists)
		require.NoError(t, err)
		require.Equal(t, ta.expected, ok)

	}

}
