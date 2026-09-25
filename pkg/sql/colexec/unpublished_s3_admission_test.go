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

package colexec

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestUnpublishedS3AdmissionBoundsUploadsAndRemoteReceipt(t *testing.T) {
	a := newUnpublishedS3Admission(2)
	require.NoError(t, a.reserveUpload("local"))
	require.Error(t, a.reserveUpload("local"), "duplicate object names must not borrow a ticket")
	require.NoError(t, a.reserveUpload("other"))
	require.Error(t, a.reserveUpload("third"), "a full CN must stop before Sync")
	require.Equal(t, 2, a.count())
	require.Equal(t, 2, a.highWater)
	require.Equal(t, uint64(1), a.failedReservations)

	charged, err := a.reserveReceived([]string{"local", "remote"})
	require.Error(t, err, "remote receipt must fail atomically when full")
	require.Empty(t, charged)
	require.Equal(t, 2, a.count())
	require.Equal(t, uint64(2), a.failedReservations)

	a.release("other")
	charged, err = a.reserveReceived([]string{"local", "remote", "remote"})
	require.NoError(t, err)
	require.Equal(t, []string{"remote"}, charged)
	require.Equal(t, 2, a.count())

	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	owner, err := newUnpublishedS3ObjectOwner(fs, a, charged, "remote")
	require.NoError(t, err)
	owner.Accept("remote")
	require.Equal(t, 1, a.count())
	owner.Accept("remote")
	require.Equal(t, 1, a.count(), "accepting twice must not release another owner's ticket")
	a.release("local")
	require.Zero(t, a.count())
}

func TestUnpublishedS3AdmissionConcurrentBound(t *testing.T) {
	const limit = 16
	a := newUnpublishedS3Admission(limit)
	var wg sync.WaitGroup
	for i := range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.reserveUpload(fmt.Sprintf("object-%d", i))
		}()
	}
	wg.Wait()
	require.Equal(t, limit, a.count())
	for name := range a.names {
		a.release(name)
	}
	require.Zero(t, a.count())
}

func TestUnpublishedS3AdmissionDefaultLimit(t *testing.T) {
	a := newUnpublishedS3Admission(defaultUnpublishedS3TicketLimit)
	for i := range defaultUnpublishedS3TicketLimit {
		require.NoError(t, a.reserveUpload(fmt.Sprintf("object-%d", i)))
	}
	require.Equal(t, defaultUnpublishedS3TicketLimit, a.count())
	require.Error(t, a.reserveUpload("overflow"))
	a.release("object-0")
	require.NoError(t, a.reserveUpload("overflow"))
}

func TestUnpublishedS3OwnerRetainsTicketUntilDelete(t *testing.T) {
	a := newUnpublishedS3Admission(1)
	baseFS, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	const name = "unpublished-object"
	require.NoError(t, baseFS.Write(context.Background(), fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Size: 1, Data: []byte("x")}},
	}))
	fs := &failOnceDeleteFileService{FileService: baseFS, failNextDelete: true}
	require.NoError(t, a.reserveUpload(name))
	owner, err := newUnpublishedS3ObjectOwner(fs, a, []string{name}, name)
	require.NoError(t, err)
	require.Error(t, a.reserveUpload("next"))
	require.Error(t, owner.Cleanup(context.Background()))
	require.Equal(t, 1, a.count(), "failed Delete must retain the cleanup ticket")
	require.Error(t, a.reserveUpload("next"))
	require.NoError(t, owner.Cleanup(context.Background()))
	require.Zero(t, a.count())
	require.NoError(t, a.reserveUpload("next"))
}

type failSecondDeleteBatchFS struct {
	fileservice.FileService
	batchSizes []int
	batchNames [][]string
}

func (fs *failSecondDeleteBatchFS) Delete(ctx context.Context, names ...string) error {
	fs.batchSizes = append(fs.batchSizes, len(names))
	fs.batchNames = append(fs.batchNames, append([]string(nil), names...))
	if len(fs.batchSizes) == 2 {
		return errors.New("second batch unavailable")
	}
	if fs.FileService != nil {
		return fs.FileService.Delete(ctx, names...)
	}
	return nil
}

func TestUnpublishedS3OwnerReleasesCompletedDeleteBatches(t *testing.T) {
	const objectCount = 1001
	admission := newUnpublishedS3Admission(objectCount)
	baseFS, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	names := make([]string, objectCount)
	for i := range names {
		names[i] = fmt.Sprintf("unpublished-%d", i)
		require.NoError(t, admission.reserveUpload(names[i]))
		require.NoError(t, baseFS.Write(context.Background(), fileservice.IOVector{
			FilePath: names[i],
			Entries:  []fileservice.IOEntry{{Size: 1, Data: []byte("x")}},
		}))
	}
	fs := &failSecondDeleteBatchFS{FileService: baseFS}
	owner, err := newUnpublishedS3ObjectOwner(fs, admission, names, names...)
	require.NoError(t, err)
	require.Error(t, admission.reserveUpload("new-upload"), "full admission must reject before another upload")

	require.ErrorContains(t, owner.Cleanup(context.Background()), "second batch unavailable")
	require.Equal(t, 1, admission.count(), "the completed batch no longer needs cleanup tickets")
	require.Equal(t, []int{1000, 1}, fs.batchSizes)
	_, err = baseFS.StatFile(context.Background(), fs.batchNames[0][0])
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "a confirmed batch must be physically absent: %v", err)
	_, err = baseFS.StatFile(context.Background(), fs.batchNames[1][0])
	require.NoError(t, err, "the unconfirmed batch must remain available for retry")
	require.True(t, owner.Pending(), "the failed batch keeps its cleanup owner")
	require.NoError(t, admission.reserveUpload("new-upload"),
		"confirmed deletion must reopen only its released capacity")
	require.Equal(t, 2, admission.count())
	admission.release("new-upload")

	require.NoError(t, owner.Cleanup(context.Background()))
	require.Zero(t, admission.count())
	require.Equal(t, []int{1000, 1, 1}, fs.batchSizes, "retry only the unconfirmed object")
	_, err = baseFS.StatFile(context.Background(), fs.batchNames[1][0])
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "the retried object must be physically absent: %v", err)
	require.False(t, owner.Pending())
}

func TestUnpublishedS3WriterRetryReleasesCompletedDeleteBatches(t *testing.T) {
	const objectCount = 1001
	serviceID := t.Name()
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	server := NewServer(serviceID)
	names := make([]string, objectCount)
	for i := range names {
		names[i] = fmt.Sprintf("writer-unpublished-%d", i)
		require.NoError(t, server.reserveUnpublishedS3Upload(names[i]))
	}
	fs := &failSecondDeleteBatchFS{}
	writer := &CNS3Writer{
		fs: fs, serviceID: serviceID, cleanupPending: true,
		ownedPersistedNames: names,
	}

	require.ErrorContains(t, writer.CloseWithCleanup(context.Background(), true), "second batch unavailable")
	require.Equal(t, 1, server.UnpublishedS3AdmissionStats().Used)
	require.Len(t, writer.ownedPersistedNames, 1)
	require.True(t, writer.PendingUnpublishedCleanup())
	require.Nil(t, writer.sinker, "the retry shell must not retain its sinker")

	require.NoError(t, writer.CloseWithCleanup(context.Background(), true))
	require.Zero(t, server.UnpublishedS3AdmissionStats().Used)
	require.False(t, writer.PendingUnpublishedCleanup())
	require.Equal(t, []int{1000, 1, 1}, fs.batchSizes)
}

type ambiguousDeleteBatchFS struct {
	fileservice.FileService
	deleteCalls int
}

func (fs *ambiguousDeleteBatchFS) Delete(_ context.Context, _ ...string) error {
	fs.deleteCalls++
	if fs.deleteCalls == 1 {
		return errors.New("response lost after physical deletion")
	}
	return nil
}

func TestUnpublishedS3OwnerKeepsAmbiguousDeleteBatchCharged(t *testing.T) {
	admission := newUnpublishedS3Admission(1)
	require.NoError(t, admission.reserveUpload("unpublished-object"))
	fs := &ambiguousDeleteBatchFS{}
	owner, err := newUnpublishedS3ObjectOwner(fs, admission,
		[]string{"unpublished-object"}, "unpublished-object")
	require.NoError(t, err)

	require.ErrorContains(t, owner.Cleanup(context.Background()), "response lost")
	require.Equal(t, 1, admission.count())
	require.True(t, owner.Pending())
	require.NoError(t, owner.Cleanup(context.Background()))
	require.Zero(t, admission.count())
	require.Equal(t, 2, fs.deleteCalls)
}

func TestUnpublishedS3AdmissionRejectsUnboundedNames(t *testing.T) {
	a := newUnpublishedS3Admission(1)
	require.Error(t, a.reserveUpload(""))
	require.Error(t, a.reserveUpload(string(make([]byte, maxUnpublishedS3ObjectNameLen+1))))
	require.Zero(t, a.count())
}

func TestUnpublishedS3AdmissionMetrics(t *testing.T) {
	serviceID := t.Name()
	a := newUnpublishedS3Admission(1, serviceID)
	require.NoError(t, a.reserveUpload("first"))
	require.Error(t, a.reserveUpload("second"))
	require.Equal(t, float64(1), promtestutil.ToFloat64(
		metricv2.UnpublishedS3TicketsGauge.WithLabelValues(serviceID, "used")))
	require.Equal(t, float64(1), promtestutil.ToFloat64(
		metricv2.UnpublishedS3TicketsGauge.WithLabelValues(serviceID, "high_water")))
	require.Equal(t, float64(1), promtestutil.ToFloat64(
		metricv2.UnpublishedS3AdmissionFailuresCounter.WithLabelValues(serviceID)))
	a.release("first")
	require.Zero(t, promtestutil.ToFloat64(
		metricv2.UnpublishedS3TicketsGauge.WithLabelValues(serviceID, "used")))
}

func TestCNS3WriterCloseReleasesAcceptedCloneTicket(t *testing.T) {
	serviceID := t.Name()
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	srv := NewServer(serviceID)
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, srv.reserveUnpublishedS3Upload("clone-object"))
	writer := &CNS3Writer{fs: fs, serviceID: serviceID, ownedPersistedNames: []string{"clone-object"}}
	require.NoError(t, writer.Close())
	require.Zero(t, srv.unpublishedS3Admission.count())
}

func BenchmarkUnpublishedS3AdmissionReserveRelease(b *testing.B) {
	a := newUnpublishedS3Admission(defaultUnpublishedS3TicketLimit, b.Name())
	names := make([]string, 1024)
	for i := range names {
		names[i] = fmt.Sprintf("bench-object-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		if err := a.reserveUpload(name); err != nil {
			b.Fatal(err)
		}
		a.release(name)
	}
}

func BenchmarkUnpublishedS3AdmissionRetainedBytes(b *testing.B) {
	const tickets = 65_536
	names := make([]string, tickets)
	for i := range names {
		names[i] = fmt.Sprintf("bench-object-%020d", i)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	a := newUnpublishedS3Admission(tickets)
	for _, name := range names {
		if err := a.reserveUpload(name); err != nil {
			b.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/tickets, "ledger-bytes/ticket")
	runtime.KeepAlive(a)
}
