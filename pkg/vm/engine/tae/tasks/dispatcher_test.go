package tasks

import (
	"context"
	"fmt"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type testTask struct{}

func (t testTask) OnExec(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t testTask) SetError(err error) {
	return
}

func (t testTask) GetError() error {
	//TODO implement me
	panic("implement me")
}

func (t testTask) WaitDone(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t testTask) Waitable() bool {
	//TODO implement me
	panic("implement me")
}

func (t testTask) GetCreateTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTask) GetStartTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTask) GetEndTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (t testTask) GetExecuteTime() int64 {
	//TODO implement me
	panic("implement me")
}

func (t testTask) AddObserver(observer iops.Observer) {
	//TODO implement me
	panic("implement me")
}

func (t testTask) ID() uint64 {
	//TODO implement me
	panic("implement me")
}

func (t testTask) Type() TaskType {
	return testType
}

func (t testTask) Cancel() error {
	//TODO implement me
	panic("implement me")
}

func (t testTask) Name() string {
	//TODO implement me
	panic("implement me")
}

type shortTask struct {
	testTask
	id uint64
	wg *sync.WaitGroup
}

func (t shortTask) ID() uint64 {
	return t.id
}

func (t shortTask) OnExec(ctx context.Context) error {
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("short task %d completed", t.id)
	t.wg.Done()
	return nil
}

func (t shortTask) GetExecuteTime() int64 {
	return int64(300 * time.Millisecond)
}

type longTask struct {
	testTask
	id uint64
	wg *sync.WaitGroup
}

func (t longTask) OnExec(ctx context.Context) error {
	time.Sleep(10 * time.Second)
	fmt.Printf("long task %d completed", t.id)
	t.wg.Done()
	return nil
}

func (t longTask) GetExecuteTime() int64 {
	return int64(10 * time.Second)
}

func (t longTask) ID() uint64 {
	return t.id
}

const testType TaskType = 0

func TestDispatch(t *testing.T) {
	ctx := context.Background()
	base := NewBaseDispatcher()
	handler := NewPoolHandler(ctx, 5)
	handler.Start()
	base.RegisterHandler(testType, handler)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		var task Task
		if i%3 == 0 {
			task = longTask{id: uint64(i), wg: &wg}
		} else {
			task = shortTask{id: uint64(i), wg: &wg}
		}
		base.Dispatch(task)
		t.Log("dispatch task", i)
	}
	wg.Wait()
	require.NoError(t, base.Close())
}
