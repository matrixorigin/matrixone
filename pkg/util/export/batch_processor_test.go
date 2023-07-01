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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/google/gops/agent"
	"github.com/prashantv/gostub"
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
		logutil.Debugf("accept: %v, len: %d", *item.(*Num), length)
		if (val <= 3 && val != length) && (val-3) != length {
			panic(moerr.NewInternalError(ctx, "len not rignt, elem: %d, len: %d", val, length))
		}
		s.signal()
	}
}
func (s *dummyBuffer) Reset() {
	s.mux.Lock()
	defer s.mux.Unlock()
	logutil.Debugf("buffer reset, stack: %+v", stack.Callers(0))
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
		logutil.Debugf("buffer shouldFlush: %v", should)
	}
	return should
}

var waitGetBatchFinish = func() {}

func (s *dummyBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	s.mux.Lock()
	defer s.mux.Unlock()
	if len(s.arr) == 0 {
		return ""
	}

	logutil.Debugf("GetBatch, len: %d", len(s.arr))
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
	logutil.Debugf("GetBatch: %s", buf.String())
	if waitGetBatchFinish != nil {
		logutil.Debugf("wait BatchFinish")
		waitGetBatchFinish()
		logutil.Debugf("wait BatchFinish, Done")
	}
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
	// defer leaktest.AfterTest(t)()
	defer agent.Close()
	ctx := context.Background()
	ch := make(chan string, 3)
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		t.Logf("TestNewMOCollector::ErrorReport: %+v", err)
	})
	var signalC = make(chan struct{}, 16)
	var acceptSignal = func() { <-signalC }
	stub1 := gostub.Stub(&signalFunc, func() { signalC <- struct{}{} })
	defer stub1.Reset()

	collector := NewMOCollector(ctx)
	collector.Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector.Start()

	collector.Collect(ctx, newDummy(1))
	acceptSignal()
	collector.Collect(ctx, newDummy(2))
	acceptSignal()
	collector.Collect(ctx, newDummy(3))
	acceptSignal()
	got123 := <-ch
	collector.Collect(ctx, newDummy(4))
	acceptSignal()
	collector.Collect(ctx, newDummy(5))
	acceptSignal()
	collector.Stop(true)
	logutil.GetGlobalLogger().Sync()
	got45 := <-ch
	for i := len(ch); i > 0; i-- {
		got := <-ch
		t.Logf("left ch: %s", got)
	}
	require.Equal(t, `(1),(2),(3)`, got123)
	require.Equal(t, `(4),(5)`, got45)
}

func TestNewMOCollector_Stop(t *testing.T) {
	MOCollectorMux.Lock()
	defer MOCollectorMux.Unlock()
	defer agent.Close()
	ctx := context.Background()
	ch := make(chan string, 3)

	errorCnt := 0
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		errorCnt++
	})

	collector := NewMOCollector(ctx)
	collector.Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector.Start()
	collector.Stop(true)

	var N int = 1e3
	for i := 0; i < N; i++ {
		collector.Collect(ctx, newDummy(int64(i)))
	}
	length := len(collector.awakeCollect)
	t.Logf("channal len: %d, errorCnt: %d, totalElem: %d", length, errorCnt, N)
	require.Equal(t, N, errorCnt+length)
}

func TestNewMOCollector_BufferCnt(t *testing.T) {
	MOCollectorMux.Lock()
	defer MOCollectorMux.Unlock()
	ctx := context.Background()
	ch := make(chan string, 3)
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		t.Logf("TestNewMOCollector::ErrorReport: %+v", err)
	})
	var signalC = make(chan struct{}, 16)
	var acceptSignal = func() { <-signalC }
	stub1 := gostub.Stub(&signalFunc, func() { signalC <- struct{}{} })
	defer stub1.Reset()

	var batchFlowC = make(chan struct{})
	var signalBatchFinishC = make(chan struct{})
	var signalBatchFinish = func() {
		signalBatchFinishC <- struct{}{}
	}
	bhStub := gostub.Stub(&waitGetBatchFinish, func() {
		batchFlowC <- struct{}{}
		<-signalBatchFinishC
	})
	defer bhStub.Reset()

	cfg := &config.OBCollectorConfig{}
	cfg.SetDefaultValues()
	cfg.ShowStatsInterval.Duration = 5 * time.Second
	cfg.BufferCnt = 2
	collector := NewMOCollector(ctx, WithOBCollectorConfig(cfg))
	collector.Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector.Start()

	collector.Collect(ctx, newDummy(1))
	acceptSignal()
	collector.Collect(ctx, newDummy(2))
	acceptSignal()
	collector.Collect(ctx, newDummy(3))
	acceptSignal()
	// make 1/2 buffer hang.
	<-batchFlowC
	collector.Collect(ctx, newDummy(4))
	acceptSignal()
	collector.Collect(ctx, newDummy(5))
	acceptSignal()
	collector.Collect(ctx, newDummy(6))
	acceptSignal()

	// make 2/2 buffer hang.
	<-batchFlowC
	// send 7th elem, it will hang, wait for buffer slot
	go collector.Collect(ctx, newDummy(7))
	// reset
	bhStub.Reset()

	select {
	case <-signalC:
		t.Errorf("failed wait buffer released.")
		return
	case <-time.After(5 * time.Second):
		t.Logf("success: hang by buffer alloc: no slot.")
		// reset be normal flow
		signalBatchFinish()
		signalBatchFinish()
		acceptSignal()
		t.Logf("reset normally")
	}
	got123 := <-ch
	got456 := <-ch
	collector.Stop(true)
	got7 := <-ch
	got := []string{got123, got456, got7}
	sort.Strings(got)
	logutil.GetGlobalLogger().Sync()
	for i := len(ch); i > 0; i-- {
		got := <-ch
		t.Logf("left ch: %s", got)
	}
	require.Equal(t, []string{`(1),(2),(3)`, `(4),(5),(6)`, `(7)`}, got)
}

func Test_newBufferHolder_AddAfterStop(t *testing.T) {
	MOCollectorMux.Lock()
	defer MOCollectorMux.Unlock()
	type args struct {
		ctx    context.Context
		name   batchpipe.HasName
		impl   motrace.PipeImpl
		signal bufferSignalFunc
		c      *MOCollector
	}

	ch := make(chan string)
	triggerSignalFunc := func(holder *bufferHolder) {}

	cfg := &config.OBCollectorConfig{}
	cfg.SetDefaultValues()
	collector := NewMOCollector(context.TODO(), WithOBCollectorConfig(cfg))

	tests := []struct {
		name string
		args args
		want *bufferHolder
	}{
		{
			name: "callAddAfterStop",
			args: args{
				ctx:    context.TODO(),
				name:   newDummy(0),
				impl:   &dummyPipeImpl{ch: ch, duration: time.Hour},
				signal: triggerSignalFunc,
				c:      collector,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := newBufferHolder(tt.args.ctx, tt.args.name, tt.args.impl, tt.args.signal, tt.args.c)
			buf.Start()
			buf.Add(newDummy(1))
			buf.Stop()
			buf.Add(newDummy(2))
			buf.Add(newDummy(3))
			b, _ := buf.buffer.(*dummyBuffer)
			require.Equal(t, []batchpipe.HasName{newDummy(1)}, b.arr)
		})
	}
}

func TestMOCollector_DiscardableCollect(t *testing.T) {

	ctx := context.TODO()
	cfg := &config.OBCollectorConfig{}
	cfg.SetDefaultValues()
	collector := NewMOCollector(context.TODO(), WithOBCollectorConfig(cfg))
	elem := newDummy(1)
	for i := 0; i < defaultQueueSize; i++ {
		collector.Collect(ctx, elem)
	}
	require.Equal(t, defaultQueueSize, len(collector.awakeCollect))

	// check DisableStore will discard
	now := time.Now()
	collector.DiscardableCollect(ctx, elem)
	require.Equal(t, defaultQueueSize, len(collector.awakeCollect))
	require.True(t, time.Since(now) > discardCollectTimeout)
	t.Logf("DiscardableCollect accept")
}
