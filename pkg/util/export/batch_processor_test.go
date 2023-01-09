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

package export

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/stack"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:      zapcore.DebugLevel.String(),
		Format:     "console",
		Filename:   "",
		MaxSize:    512,
		MaxDays:    0,
		MaxBackups: 0,

		DisableStore: true,
	})
	if err := agent.Listen(agent.Options{}); err != nil {
		logutil.Errorf("listen gops agent failed: %s", err)
		return
	}
}

const NumType = "Num"

var _ batchpipe.HasName = (*Num)(nil)
var _ batchpipe.ItemBuffer[batchpipe.HasName, any] = &dummyBuffer{}
var _ batchpipe.PipeImpl[batchpipe.HasName, any] = &dummyPipeImpl{}

type Num int64

func newDummy(v int64) *Num {
	n := Num(v)
	return &n
}

func (n Num) GetName() string { return NumType }

var signalFunc = func() {}

type dummyBuffer struct {
	batchpipe.Reminder
	arr    []batchpipe.HasName
	mux    sync.Mutex
	signal func()
	ctx    context.Context
}

func (s *dummyBuffer) Add(item batchpipe.HasName) {
	s.mux.Lock()
	defer s.mux.Unlock()
	ctx := s.ctx
	s.arr = append(s.arr, item)
	if s.signal != nil {
		val := int(*item.(*Num))
		length := len(s.arr)
		logutil.Infof("accept: %v, len: %d", *item.(*Num), length)
		if (val <= 3 && val != length) && (val-3) != length {
			panic(moerr.NewInternalError(ctx, "len not rignt, elem: %d, len: %d", val, length))
		}
		s.signal()
	}
}
func (s *dummyBuffer) Reset() {
	s.mux.Lock()
	defer s.mux.Unlock()
	logutil.Infof("buffer reset, stack: %+v", stack.Callers(0))
	s.arr = s.arr[0:0]
}
func (s *dummyBuffer) IsEmpty() bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	return len(s.arr) == 0
}
func (s *dummyBuffer) ShouldFlush() bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	length := len(s.arr)
	should := length >= 3
	if should {
		logutil.Infof("buffer shouldFlush: %v", should)
	}
	return should
}
func (s *dummyBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	s.mux.Lock()
	defer s.mux.Unlock()
	if len(s.arr) == 0 {
		return ""
	}

	logutil.Infof("GetBatch, len: %d", len(s.arr))
	buf.Reset()
	for _, item := range s.arr {
		s, ok := item.(*Num)
		if !ok {
			panic("Not Num type")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", *s))
		buf.WriteString("),")
	}
	logutil.Infof("GetBatch: %s", buf.String())
	return string(buf.Next(buf.Len() - 1))
}

type dummyPipeImpl struct {
	ch       chan string
	duration time.Duration
}

func (n *dummyPipeImpl) NewItemBuffer(string) batchpipe.ItemBuffer[batchpipe.HasName, any] {
	return &dummyBuffer{Reminder: batchpipe.NewConstantClock(n.duration), signal: signalFunc, ctx: context.Background()}
}

func (n *dummyPipeImpl) NewItemBatchHandler(ctx context.Context) func(any) {
	return func(batch any) {
		n.ch <- batch.(string)
	}
}

var MOCollectorMux sync.Mutex

func TestNewMOCollector(t *testing.T) {
	MOCollectorMux.Lock()
	defer MOCollectorMux.Unlock()
	ctx := context.Background()
	ch := make(chan string, 3)
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		t.Logf("TestNewMOCollector::ErrorReport: %+v", err)
	})
	var signalC = make(chan struct{}, 16)
	var acceptSignal = func() { <-signalC }
	signalFunc = func() { signalC <- struct{}{} }

	collector := NewMOCollector(ctx)
	collector.Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector.Start()

	collector.Collect(ctx, newDummy(1))
	acceptSignal()
	collector.Collect(ctx, newDummy(2))
	acceptSignal()
	collector.Collect(ctx, newDummy(3))
	acceptSignal()
	got := <-ch
	require.Equal(t, `(1),(2),(3)`, got)
	collector.Collect(ctx, newDummy(4))
	acceptSignal()
	collector.Collect(ctx, newDummy(5))
	acceptSignal()
	collector.Stop(true)
	logutil.GetGlobalLogger().Sync()
	got = <-ch
	require.Equal(t, `(4),(5)`, got)
	for i := len(ch); i > 0; i-- {
		got = <-ch
		t.Logf("left ch: %s", got)
	}
}
