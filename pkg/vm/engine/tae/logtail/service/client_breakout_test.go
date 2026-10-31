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

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBreakoutReceiveCoalescesFullChannel(t *testing.T) {
	c := &LogtailClient{breakChan: make(chan struct{}, 1)}
	c.BreakoutReceive()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.BreakoutReceive()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		// Unblock the goroutine even if the assertion fails.
		<-c.breakChan
		<-done
		t.Fatal("a pending breakout blocked another pause request")
	}
	_, err := c.Receive(context.Background())
	require.Error(t, err, "the pending breakout still interrupts Receive")
}
