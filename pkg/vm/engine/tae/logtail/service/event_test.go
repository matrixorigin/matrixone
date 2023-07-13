// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	event := NewNotifier(ctx, 10)
	eventNum := 5

	go func() {
		for i := 1; i <= eventNum; i++ {
			from, to := mockTimestamp(int64(i-1), 0), mockTimestamp(int64(i), 0)
			table := mockTable(uint64(i), uint64(i), uint64(i))
			err := event.NotifyLogtail(from, to, nil, mockLogtail(table, to))
			require.NoError(t, err)
		}
	}()

	for j := 1; j <= eventNum; j++ {
		e := <-event.C
		require.Equal(t, mockTimestamp(int64(j-1), 0), e.from)
		require.Equal(t, mockTimestamp(int64(j), 0), e.to)
		require.Equal(t, 1, len(e.logtails))
	}
}
