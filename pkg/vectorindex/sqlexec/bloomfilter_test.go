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

package sqlexec

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

// mockMessage is a mock Message type to test continue branch
type mockMessage struct {
	tag int32
}

func (m mockMessage) Serialize() []byte {
	return nil
}

func (m mockMessage) Deserialize([]byte) message.Message {
	return nil
}

func (m mockMessage) NeedBlock() bool {
	return false
}

func (m mockMessage) GetMsgTag() int32 {
	return m.tag
}

func (m mockMessage) GetReceiverAddr() message.MessageAddress {
	return message.AddrBroadCastOnCurrentCN()
}

func (m mockMessage) DebugString() string {
	return "mock message"
}

func (m mockMessage) Destroy() {
}

func TestWaitBloomFilterForTableFunction(t *testing.T) {
	t.Run("return nil when RuntimeFilterSpecs is empty", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		sqlproc := NewSqlProcess(proc)
		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}
		result, err := WaitBloomFilter(sqlproc)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("return nil when UseBloomFilter is false", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		sqlproc := NewSqlProcess(proc)
		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            1,
				UseBloomFilter: false,
			},
		}
		result, err := WaitBloomFilter(sqlproc)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("return data when BLOOMFILTER message found", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(100)
		expectedData := []byte{1, 2, 3, 4, 5}

		// Send a BLOOMFILTER message before calling the function
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		// Use a goroutine to call the function since it blocks
		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		// Wait a bit for the message to be received
		<-done

		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})

	t.Run("return nil when no matching message found", func(t *testing.T) {
		// Create a new process with timeout context for this test
		testProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		testProc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(testProc)

		tag := int32(200)

		// Send a message with different tag
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag + 1, // different tag
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: []byte{1, 2, 3},
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		// Use context with cancel to avoid blocking forever
		ctx, cancel := context.WithCancel(context.Background())
		testProc.Ctx = ctx

		// Cancel context after timeout to avoid blocking forever
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done
		cancel() // Ensure cancel is called even if goroutine finishes early

		// Should return nil when context is done or no matching message
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("continue when message is not RuntimeFilterMessage", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(300)
		expectedData := []byte{6, 7, 8, 9, 10}

		// Send a mock message (not RuntimeFilterMessage)
		mockMsg := mockMessage{tag: tag}
		message.SendMessage(mockMsg, mb)

		// Also send a valid BLOOMFILTER message
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done

		// Should skip the mock message and return the BLOOMFILTER message
		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})

	t.Run("continue when message Typ is not BLOOMFILTER", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(400)
		expectedData := []byte{11, 12, 13, 14, 15}

		// Send a message with different type
		rtMsg1 := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_IN, // not BLOOMFILTER
			Data: []byte{99, 98, 97},
		}
		message.SendMessage(rtMsg1, mb)

		// Send a valid BLOOMFILTER message
		rtMsg2 := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg2, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done

		// Should skip the IN message and return the BLOOMFILTER message
		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})
}
