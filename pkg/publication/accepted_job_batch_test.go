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

package publication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

var errAdmissionSealed = errors.New("admission sealed")

type controlledAcceptedJob struct {
	canceled chan struct{}
	release  chan struct{}
}

func newControlledAcceptedJob() *controlledAcceptedJob {
	return &controlledAcceptedJob{
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (c *controlledAcceptedJob) completeAfterCancellation(ctx context.Context, job Job) {
	go func() {
		<-ctx.Done()
		close(c.canceled)
		<-c.release
		failAcceptedJob(job, ctx.Err())
	}()
}

func assertCancelAndJoin(t *testing.T, canceled <-chan struct{}, release chan struct{}, returned <-chan error) {
	t.Helper()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("accepted job was not canceled")
	}
	select {
	case err := <-returned:
		t.Fatalf("owner returned before joining the accepted job: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-returned:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("owner did not return after the accepted job completed")
	}
}

type prefixRejectGetChunkWorker struct {
	controlled *controlledAcceptedJob
	chunks     int
}

func (w *prefixRejectGetChunkWorker) SubmitGetChunk(job Job) error {
	switch job := job.(type) {
	case *GetMetaJob:
		job.Execute()
		return nil
	case *GetChunkJob:
		w.chunks++
		if w.chunks > 1 {
			return errAdmissionSealed
		}
		w.controlled.completeAfterCancellation(job.ctx, job)
		return nil
	default:
		return errors.New("unexpected job type")
	}
}

func (*prefixRejectGetChunkWorker) Stop() {}

func newTwoChunkMetadataExecutor() SQLExecutor {
	return &mockSQLExecutorForJob{execFn: func(
		context.Context,
		*ActiveRoutine,
		uint32,
		string,
		bool,
		bool,
		time.Duration,
	) (*Result, context.CancelFunc, error) {
		return newMockResult(&mockScanner{
			nextFn: func() bool { return true },
			scanFn: func(dest ...interface{}) error {
				*dest[0].(*[]byte) = nil
				*dest[1].(*int64) = 2
				*dest[2].(*int64) = 0
				*dest[3].(*int64) = 2
				*dest[4].(*bool) = false
				return nil
			},
			closeFn: func() error { return nil },
			errFn:   func() error { return nil },
		}), noopCancel, nil
	}}
}

func TestGetObjectJoinsAcceptedPrefixWhenChunkAdmissionIsSealed(t *testing.T) {
	controlled := newControlledAcceptedJob()
	worker := &prefixRejectGetChunkWorker{controlled: controlled}
	returned := make(chan error, 1)
	go func() {
		_, err := GetObjectFromUpstreamWithWorker(
			context.Background(), newTwoChunkMetadataExecutor(), "object", worker, "account", "publication",
		)
		returned <- err
	}()

	assertCancelAndJoin(t, controlled.canceled, controlled.release, returned)
}

type resultFailureGetChunkWorker struct {
	controlled *controlledAcceptedJob
	chunks     int
}

func (w *resultFailureGetChunkWorker) SubmitGetChunk(job Job) error {
	switch job := job.(type) {
	case *GetMetaJob:
		job.Execute()
		return nil
	case *GetChunkJob:
		w.chunks++
		switch w.chunks {
		case 1:
			failAcceptedJob(job, errors.New("chunk failed"))
			return nil
		case 2:
			w.controlled.completeAfterCancellation(job.ctx, job)
			return nil
		default:
			// Reject the retry for chunk one. The already accepted second
			// chunk must still be canceled and joined before return.
			return errAdmissionSealed
		}
	default:
		return errors.New("unexpected job type")
	}
}

func (*resultFailureGetChunkWorker) Stop() {}

func TestGetObjectJoinsRemainingChunksAfterResultFailure(t *testing.T) {
	controlled := newControlledAcceptedJob()
	worker := &resultFailureGetChunkWorker{controlled: controlled}
	returned := make(chan error, 1)
	go func() {
		_, err := GetObjectFromUpstreamWithWorker(
			context.Background(), newTwoChunkMetadataExecutor(), "object", worker, "account", "publication",
		)
		returned <- err
	}()

	assertCancelAndJoin(t, controlled.canceled, controlled.release, returned)
}

type controlledFilterWorker struct {
	controlled      *controlledAcceptedJob
	accepted        int
	rejectAfter     int
	failFirstResult bool
}

func (w *controlledFilterWorker) SubmitFilterObject(job Job) error {
	w.accepted++
	if w.rejectAfter > 0 && w.accepted > w.rejectAfter {
		return errAdmissionSealed
	}
	filterJob := job.(*FilterObjectJob)
	if w.failFirstResult && w.accepted == 1 {
		failAcceptedJob(job, errors.New("filter failed"))
		return nil
	}
	w.controlled.completeAfterCancellation(filterJob.ctx, job)
	return nil
}

func (*controlledFilterWorker) Stop() {}

func makeFilterObjectMap(tombstone bool) map[objectio.ObjectId]*ObjectWithTableInfo {
	objects := make(map[objectio.ObjectId]*ObjectWithTableInfo, 2)
	for i := byte(1); i <= 2; i++ {
		var id objectio.ObjectId
		id[0] = i
		objects[id] = &ObjectWithTableInfo{
			IsTombstone: tombstone,
			DBName:      "db",
			TableName:   "table",
		}
	}
	return objects
}

func runApplyObjects(t *testing.T, objects map[objectio.ObjectId]*ObjectWithTableInfo, worker FilterObjectWorker) <-chan error {
	t.Helper()
	returned := make(chan error, 1)
	go func() {
		returned <- ApplyObjects(
			context.Background(), "task", 0, nil, objects,
			nil, nil, types.TS{}, nil, nil, nil, nil,
			worker, nil, nil, "account", "publication", nil, nil, nil,
		)
	}()
	return returned
}

func TestApplyObjectsJoinsAcceptedPrefixWhenFilterAdmissionIsSealed(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		name := "data"
		if tombstone {
			name = "tombstone"
		}
		t.Run(name, func(t *testing.T) {
			controlled := newControlledAcceptedJob()
			worker := &controlledFilterWorker{
				controlled:  controlled,
				rejectAfter: 1,
			}
			returned := runApplyObjects(t, makeFilterObjectMap(tombstone), worker)
			assertCancelAndJoin(t, controlled.canceled, controlled.release, returned)
		})
	}
}

func TestApplyObjectsJoinsRemainingJobsAfterResultFailure(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		name := "data"
		if tombstone {
			name = "tombstone"
		}
		t.Run(name, func(t *testing.T) {
			controlled := newControlledAcceptedJob()
			worker := &controlledFilterWorker{
				controlled:      controlled,
				failFirstResult: true,
			}
			returned := runApplyObjects(t, makeFilterObjectMap(tombstone), worker)
			assertCancelAndJoin(t, controlled.canceled, controlled.release, returned)
		})
	}
}
