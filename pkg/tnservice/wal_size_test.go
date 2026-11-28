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

package tnservice

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/stretchr/testify/require"
)

func TestEffectiveMaxWALRecordSize(t *testing.T) {
	// The reserve covers even maximum-width APPEND request fields and the
	// UserRecord header added by the client before sending the WAL payload.
	request := logservice.Request{
		RequestID: math.MaxUint64,
		Method:    logservice.APPEND,
		LogRequest: logservice.LogRequest{
			ShardID: math.MaxUint64,
			TNID:    math.MaxUint64,
		},
	}
	require.Less(t, request.ProtoSize()+len(logservice.NewUserLogRecord(math.MaxUint64, 0).Data), 1024)

	for _, tc := range []struct {
		configured, clientMax, receiverMax, want int
	}{
		{0, 100 * mpool.MB, 0, logservicedriver.DefaultMaxWALRecordSize},
		{0, 32 * mpool.MB, 0, 32*mpool.MB - 1024},
		{50 * mpool.MB, 32 * mpool.MB, 0, 32*mpool.MB - 1024},
		{20 * mpool.MB, 32 * mpool.MB, 0, 20 * mpool.MB},
		{0, 100 * mpool.MB, 32 * mpool.MB, 32*mpool.MB - 1024},
		{50 * mpool.MB, 100 * mpool.MB, 32 * mpool.MB, 32*mpool.MB - 1024},
	} {
		got, err := effectiveMaxWALRecordSize(tc.configured, tc.clientMax, tc.receiverMax)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	_, err := effectiveMaxWALRecordSize(0, 1024, 0)
	require.Error(t, err)
	_, err = effectiveMaxWALRecordSize(0, 100*mpool.MB, 1024)
	require.Error(t, err)
}
