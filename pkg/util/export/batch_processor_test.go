package export

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"testing"
	"time"
)

const NumType = "Num"

var _ batchpipe.HasName = (*Num)(nil)
var _ batchpipe.ItemBuffer[batchpipe.HasName, any] = &SizeBuffer{}
var _ batchpipe.PipeImpl[batchpipe.HasName, any] = &testNumPipeImpl{}

type Num int64

func newNum(v int64) *Num {
	n := Num(v)
	return &n
}

func (n Num) GetName() string { return NumType }

type SizeBuffer struct {
	batchpipe.Reminder
	arr []batchpipe.HasName
}

func (s *SizeBuffer) Add(item batchpipe.HasName) { s.arr = append(s.arr, item) }
func (s *SizeBuffer) Reset()                     { s.arr = s.arr[0:0] }
func (s *SizeBuffer) IsEmpty() bool              { return len(s.arr) == 0 }
func (s *SizeBuffer) ShouldFlush() bool          { return len(s.arr) >= 3 }
func (s *SizeBuffer) GetBatch(buf *bytes.Buffer) any {
	buf.Reset()
	if s.IsEmpty() {
		return ""
	}

	for _, item := range s.arr {
		s, ok := item.(*Num)
		if !ok {
			panic("Not Num type")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", *s))
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

type testNumPipeImpl struct {
	ch chan string
}

func (n *testNumPipeImpl) NewItemBuffer(string) batchpipe.ItemBuffer[batchpipe.HasName, any] {
	return &SizeBuffer{Reminder: batchpipe.NewConstantClock(100 * time.Millisecond)}
}

func (n *testNumPipeImpl) NewItemBatchHandler() func(any) {
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
	ch := make(chan string, 1)
	byteBuf := new(bytes.Buffer)
	var signal = func(b *bufferHolder) {
		b.Stop()
		ch <- b.buffer.GetBatch(byteBuf).(string)
	}
	tests := []struct {
		name         string
		args         args
		want         string
		wantReminder string
	}{
		{
			name: "normal",
			args: args{
				name:          newNum(0),
				impl:          &testNumPipeImpl{ch: ch},
				signal:        signal,
				elems:         []*Num{newNum(1), newNum(2), newNum(3)},
				elemsReminder: []*Num{newNum(4), newNum(5)},
				interval:      100 * time.Millisecond,
			},
			want:         `(1),(2),(3)`,
			wantReminder: `(4),(5)`,
		},
		{
			name: "emptyReminder",
			args: args{
				name:          newNum(0),
				impl:          &testNumPipeImpl{ch: ch},
				signal:        signal,
				elems:         []*Num{newNum(1), newNum(2), newNum(3)},
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
			for _, v := range tt.args.elems {
				buf.Add(v)
			}
			got := <-ch
			if got != tt.want {
				t.Errorf("newBufferHolder() = %v, want %v", got, tt.want)
			}
			buf.Reset()

			for _, v := range tt.args.elemsReminder {
				buf.Add(v)
			}
			time.Sleep(tt.args.interval)
			got = <-ch
			if got != tt.wantReminder {
				t.Errorf("newBufferHolder() = %v, want %v", got, tt.wantReminder)
			}
		})
	}
}
