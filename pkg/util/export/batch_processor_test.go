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
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func init() {
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
}

func (s *dummyBuffer) Add(item batchpipe.HasName) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.arr = append(s.arr, item)
	if s.signal != nil {
		val := int(*item.(*Num))
		length := len(s.arr)
		logutil.Infof("accept: %v, len: %d", *item.(*Num), length)
		if (val <= 3 && val != length) && (val-3) != length {
			panic(moerr.NewInternalError("len not rignt, elem: %d, len: %d", val, length))
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
	return length >= 3
}
func (s *dummyBuffer) GetBatch(buf *bytes.Buffer) any {
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
	return &dummyBuffer{Reminder: batchpipe.NewConstantClock(n.duration), signal: signalFunc}
}

func (n *dummyPipeImpl) NewItemBatchHandler(ctx context.Context) func(any) {
	return func(batch any) {
		n.ch <- batch.(string)
	}
}

func Test_newBufferHolder(t *testing.T) {
	type args struct {
		name          batchpipe.HasName
		impl          batchpipe.PipeImpl[batchpipe.HasName, any]
		signal        bufferSignalFunc
		elems         []*Num
		elemsReminder []*Num
		interval      time.Duration
	}
	ch := make(chan string)
	byteBuf := new(bytes.Buffer)
	signalC := make(chan *bufferHolder, 1)
	var signal = func(b *bufferHolder) {
		signalC <- b
	}
	var signalAcceptableCh = make(chan struct{}, 1)
	go func() {
		for {
			<-signalAcceptableCh
			b, ok := <-signalC
			if !ok {
				break
			}
			if ok := b.StopAndGetBatch(byteBuf); !ok {
				t.Errorf("GenBatch failed by in readwrite mode")
			} else {
				content := b.batch
				b.batch = nil // aim to ignore FlushAndReset process
				ch <- (*content).(string)
			}
		}
	}()
	tests := []struct {
		name         string
		args         args
		want         string
		wantReminder string
	}{
		{
			name: "normal",
			args: args{
				name:          newDummy(0),
				impl:          &dummyPipeImpl{ch: ch, duration: 100 * time.Millisecond},
				signal:        signal,
				elems:         []*Num{newDummy(1), newDummy(2), newDummy(3)},
				elemsReminder: []*Num{newDummy(4), newDummy(5)},
				interval:      100 * time.Millisecond,
			},
			want:         `(1),(2),(3)`,
			wantReminder: `(4),(5)`,
		},
		{
			name: "emptyReminder",
			args: args{
				name:          newDummy(0),
				impl:          &dummyPipeImpl{ch: ch, duration: 100 * time.Millisecond},
				signal:        signal,
				elems:         []*Num{newDummy(1), newDummy(2), newDummy(3)},
				elemsReminder: []*Num{},
				interval:      100 * time.Millisecond,
			},
			want:         `(1),(2),(3)`,
			wantReminder: ``,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := newBufferHolder(tt.args.name, tt.args.impl, tt.args.signal)
			buf.Start()
			for _, v := range tt.args.elems {
				buf.Add(v)
			}
			signalAcceptableCh <- struct{}{}
			got := <-ch
			require.Equal(t, got, tt.want)
			buf.FlushAndReset()

			for _, v := range tt.args.elemsReminder {
				buf.Add(v)
			}
			signalAcceptableCh <- struct{}{}
			got = <-ch
			if got != tt.wantReminder {
				t.Errorf("newBufferHolder() = %v, want %v", got, tt.wantReminder)
			}
			buf.StopAndGetBatch(new(bytes.Buffer))
		})
	}
	//signalAcceptable.Done() // release started-lock
	close(signalC)
	signalAcceptableCh <- struct{}{}
}

func TestNewMOCollector(t *testing.T) {
	ch := make(chan string, 3)
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		t.Logf("TestNewMOCollector::ErrorReport: %+v", err)
	})
	var signalC = make(chan struct{}, 16)
	var acceptSignal = func() { <-signalC }
	signalFunc = func() { signalC <- struct{}{} }

	Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector := NewMOCollector()
	collector.Start()

	collector.Collect(DefaultContext(), newDummy(1))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(2))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(3))
	acceptSignal()
	got := <-ch
	require.Equal(t, `(1),(2),(3)`, got)
	collector.Collect(DefaultContext(), newDummy(4))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(5))
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

func TestMOCollector_HangBug(t *testing.T) {
	ch := make(chan string, 3)
	errutil.SetErrorReporter(func(ctx context.Context, err error, i int) {
		t.Logf("TestNewMOCollector::ErrorReport: %+v", err)
	})

	timeo := 30 * time.Second

	// prepare signalFunc
	var signalC = make(chan struct{}, 16)
	var acceptSignal = func() { <-signalC }
	_stubSignalFunc := gostub.Stub(&signalFunc, func() { signalC <- struct{}{} })
	defer _stubSignalFunc.Reset()
	// prepare awakeBuffer
	var ctrlC = make(chan *bufferHolder, 1)
	var ctrlTimer = time.NewTimer(timeo)
	var ctrlTimeoutCnt = 0
	_stubAwakeBuffer := gostub.Stub(&awakeBuffer, func(c *MOCollector) func(holder *bufferHolder) {
		return func(holder *bufferHolder) {
			//c.awakeGenerate <- holder
			select {
			case ctrlC <- holder:
			case <-ctrlTimer.C:
				ctrlTimeoutCnt += 1
				ctrlTimer = time.NewTimer(timeo)
			}
		}
	})
	defer _stubAwakeBuffer.Reset()

	// init Collector, with disabled-trigger
	Register(newDummy(0), &dummyPipeImpl{ch: ch, duration: time.Hour})
	collector := NewMOCollector()
	collector.Start()

	t.Logf("fill up the buffer")
	collector.Collect(DefaultContext(), newDummy(1))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(2))
	acceptSignal()
	t.Logf("trigger ShouldFlush calling")
	collector.Collect(DefaultContext(), newDummy(3))
	acceptSignal()
	// fill up the collect channel(awakeCollect) ==> fill up the Batch channel(awakeGenerate)
	collector.Collect(DefaultContext(), newDummy(4))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(5))
	acceptSignal()
	collector.Collect(DefaultContext(), newDummy(6))
	t.Logf("length collect channel: %d", len(collector.awakeCollect))
	t.Logf("length generate channel: %d", len(ctrlC))
	require.Equal(t, 1, len(collector.awakeCollect))
	require.Equal(t, 1, len(ctrlC))
	t.Logf("if Hang, caused by accept one generate req and lock itself")

	timer := time.NewTimer(2 * timeo)
	buf := new(bytes.Buffer)
	var jobs = 0
loop:
	for {
		select {
		case holder := <-ctrlC:
			holder.StopAndGetBatch(buf)
			jobs++
			t.Logf("job done count: %d, timeoutCnt: %d", jobs, ctrlTimeoutCnt)
			if jobs > 1 && ctrlTimeoutCnt == 0 {
				break loop
			}
		case <-timer.C:
			require.Equal(t, 0, len(collector.awakeGenerate), "meet timeout, means dead lock")
			require.Equal(t, 0, ctrlTimeoutCnt, "meet timeout, means dead lock")
			break loop
		}
	}

}
