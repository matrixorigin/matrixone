// Copyright 2023 Matrix Origin
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

package logutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlow(t *testing.T) {
	done := Slow(time.Millisecond*10, "slow")
	time.Sleep(time.Millisecond * 50)
	require.True(t, done())

	// done before timeout
	done = Slow(time.Millisecond*500, "slow")
	require.False(t, done())
}
