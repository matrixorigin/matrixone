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

package arrowio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type identityMemoryFS struct {
	*fileservice.MemoryFS
	identity atomic.Pointer[fileservice.ObjectIdentity]
}

func newIdentityMemoryFS(fs *fileservice.MemoryFS, identity fileservice.ObjectIdentity) *identityMemoryFS {
	result := &identityMemoryFS{MemoryFS: fs}
	result.identity.Store(&identity)
	return result
}

func (f *identityMemoryFS) StatFileIdentity(ctx context.Context, _ string) (fileservice.ObjectIdentity, error) {
	if err := ctx.Err(); err != nil {
		return fileservice.ObjectIdentity{}, err
	}
	return *f.identity.Load(), nil
}

func (f *identityMemoryFS) OpenReadWithIdentity(
	ctx context.Context,
	path string,
	offset, size int64,
	expected fileservice.ObjectIdentity,
) (io.ReadCloser, error) {
	if current := *f.identity.Load(); current != expected {
		return nil, fileservice.ErrObjectChanged
	}
	var stream io.ReadCloser
	vector := &fileservice.IOVector{
		FilePath: path, Policy: fileservice.SkipAllCache,
		Entries: []fileservice.IOEntry{{Offset: offset, Size: size, ReadCloserForRead: &stream}},
	}
	if err := f.Read(ctx, vector); err != nil {
		vector.ReleaseReadResultOnError()
		return nil, err
	}
	return stream, nil
}

type testAdmission struct {
	reserved atomic.Int64
	pending  atomic.Int64
	released atomic.Int64
	active   atomic.Int64
	max      atomic.Int64
	reject   error
}

func (a *testAdmission) Reserve(_ context.Context, upper int64) (fileservice.CapacityReservation, error) {
	if a.reject != nil {
		return nil, a.reject
	}
	a.reserved.Add(upper)
	a.pending.Add(upper)
	for {
		current := a.max.Load()
		if upper <= current || a.max.CompareAndSwap(current, upper) {
			break
		}
	}
	return &testReservation{admission: a, upper: upper}, nil
}

type testReservation struct {
	admission *testAdmission
	upper     int64
	done      atomic.Bool
}

func (r *testReservation) Commit(actual int64) (fileservice.CapacityLease, error) {
	if actual < 0 || actual > r.upper || !r.done.CompareAndSwap(false, true) {
		return nil, errors.New("invalid reservation commit")
	}
	r.admission.pending.Add(-r.upper)
	r.admission.active.Add(actual)
	return &testCapacityLease{admission: r.admission, capacity: actual}, nil
}

func (r *testReservation) Abort() {
	if r.done.CompareAndSwap(false, true) {
		r.admission.pending.Add(-r.upper)
	}
}

type testCapacityLease struct {
	admission *testAdmission
	capacity  int64
	released  atomic.Bool
}

type testRangeLease struct {
	releases atomic.Int64
}

func (l *testRangeLease) Bytes() []byte   { return nil }
func (l *testRangeLease) Capacity() int64 { return 0 }
func (l *testRangeLease) Release()        { l.releases.Add(1) }

type panickingAllocator struct{}

func (panickingAllocator) Allocate(int) []byte           { panic("injected allocation panic") }
func (panickingAllocator) Reallocate(int, []byte) []byte { panic("injected reallocation panic") }
func (panickingAllocator) Free([]byte)                   {}

func (l *testCapacityLease) Release() {
	if l.released.CompareAndSwap(false, true) {
		l.admission.active.Add(-l.capacity)
		l.admission.released.Add(l.capacity)
	}
}

func TestIPCFileRangeReader(t *testing.T) {
	fileBytes, expected := makeIPC(t, ContainerFile)
	fs := writeMemoryFile(t, "arrow-file", fileBytes)
	admission := new(testAdmission)
	reader, err := Open(
		context.Background(), fs, "arrow-file", int64(len(fileBytes)), ContainerAuto, admission, Options{},
	)
	require.NoError(t, err)
	require.True(t, reader.Schema().Equal(expected[0].Schema()))

	for i := range expected {
		require.True(t, reader.Next())
		require.True(t, array.RecordEqual(expected[i], reader.RecordBatch()))
		require.Greater(t, admission.active.Load(), int64(0), "current record range must remain pinned")
	}
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	require.Zero(t, admission.active.Load())
	require.Greater(t, admission.reserved.Load(), int64(0))
	require.Less(t, admission.max.Load(), int64(len(fileBytes)), "file path must not pin the whole object")
	releaseRecords(expected)
}

func TestAdmissionAllocatorFailureAndForcedCleanup(t *testing.T) {
	t.Run("forced cleanup frees backing and capacity exactly once", func(t *testing.T) {
		admission := new(testAdmission)
		checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
		allocator := newAdmissionAllocator(context.Background(), admission)
		allocator.base = checked
		buffer := allocator.Allocate(127)
		require.NotEmpty(t, buffer)
		require.Positive(t, admission.active.Load())
		require.Zero(t, admission.pending.Load())

		allocator.releaseAll()
		require.Zero(t, admission.active.Load())
		require.Zero(t, admission.pending.Load())
		allocator.Free(buffer)
		require.Zero(t, admission.active.Load(), "late Arrow cleanup must be idempotent")
		checked.AssertSize(t, 0)
	})

	t.Run("failed generation cleanup preserves older live backing", func(t *testing.T) {
		admission := new(testAdmission)
		checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
		allocator := newAdmissionAllocator(context.Background(), admission)
		allocator.base = checked
		older := allocator.Allocate(64)
		checkpoint := allocator.checkpoint()
		abandoned := allocator.Allocate(128)
		require.Equal(t, int64(cap(older)+cap(abandoned)), admission.active.Load())

		allocator.releaseAfter(checkpoint)
		require.Equal(t, int64(cap(older)), admission.active.Load())
		allocator.Free(abandoned)
		require.Equal(t, int64(cap(older)), admission.active.Load(), "late cleanup must not release an older generation")
		allocator.Free(older)
		require.Zero(t, admission.active.Load())
		checked.AssertSize(t, 0)
	})

	t.Run("base allocation panic aborts reservation", func(t *testing.T) {
		admission := new(testAdmission)
		allocator := newAdmissionAllocator(context.Background(), admission)
		allocator.base = panickingAllocator{}
		require.PanicsWithValue(t, "injected allocation panic", func() {
			allocator.Allocate(64)
		})
		require.Zero(t, admission.pending.Load())
		require.Zero(t, admission.active.Load())
	})
}

func TestRangeLeaseAllocatorConcurrentForcedAndLateRelease(t *testing.T) {
	lease := new(testRangeLease)
	allocator := &rangeLeaseAllocator{lease: lease}

	const releasers = 32
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(releasers)
	for i := 0; i < releasers; i++ {
		go func(force bool) {
			defer wait.Done()
			<-start
			if force {
				allocator.release()
			} else {
				allocator.Free(nil)
			}
		}(i%2 == 0)
	}
	close(start)
	wait.Wait()
	require.Equal(t, int64(1), lease.releases.Load())
}

func TestRangeMessageReaderRetainCannotResurrectTerminalOwner(t *testing.T) {
	reader := new(rangeMessageReader)
	reader.refs.Store(1)
	reader.Retain()
	require.Equal(t, int64(2), reader.refs.Load())
	reader.Release()
	reader.Release()
	require.Zero(t, reader.refs.Load())
	require.Panics(t, reader.Retain)
	require.Zero(t, reader.refs.Load())
}

func TestStreamMessageReaderRetainCannotResurrectTerminalOwner(t *testing.T) {
	reader := new(streamMessageReader)
	reader.refs.Store(1)
	reader.Retain()
	require.Equal(t, int64(2), reader.refs.Load())
	reader.Release()
	reader.Release()
	require.Zero(t, reader.refs.Load())
	require.Panics(t, reader.Retain)
	require.Zero(t, reader.refs.Load())
}

func TestIPCFileRecordOutlivesReaderViaArrayData(t *testing.T) {
	fileBytes, expected := makeIPC(t, ContainerFile)
	fs := writeMemoryFile(t, "arrow-lifetime", fileBytes)
	admission := new(testAdmission)
	reader, err := Open(context.Background(), fs, "arrow-lifetime", int64(len(fileBytes)), ContainerFile, admission, Options{})
	require.NoError(t, err)
	require.True(t, reader.Next())
	record := reader.RecordBatch()
	record.Retain()
	require.NoError(t, reader.Close())
	require.Greater(t, admission.active.Load(), int64(0))
	require.True(t, array.RecordEqual(expected[0], record))
	record.Release()
	require.Zero(t, admission.active.Load())
	releaseRecords(expected)
}

func TestFailedIPCFileGenerationPreservesOlderRetainedRecord(t *testing.T) {
	payload, expected := makeIPC(t, ContainerFile)
	defer releaseRecords(expected)

	inspectionFS := writeMemoryFile(t, "arrow-generation-inspect", payload)
	inspectionAdmission := new(testAdmission)
	options, err := normalizeOptions(Options{Allocator: memory.NewGoAllocator()})
	require.NoError(t, err)
	records, _, err := readFooterBlocks(
		context.Background(), fileservice.NewLeasedRangeReader(inspectionFS),
		"arrow-generation-inspect", int64(len(payload)), inspectionAdmission, options,
	)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Zero(t, inspectionAdmission.active.Load())

	// The second record's String buffers are validity, offsets, and values.
	// Keep every descriptor in bounds while making the last logical offset
	// exceed the values buffer, so Arrow-Go fails only after ArrayData retain.
	malformed := append([]byte(nil), payload...)
	second := records[1]
	record := firstIPCRecordTable(t, malformed, second)
	buffersOffset := flatbuffers.UOffsetT(record.Offset(8))
	require.NotZero(t, buffersOffset)
	require.GreaterOrEqual(t, record.VectorLen(buffersOffset), 5)
	buffers := record.Vector(buffersOffset)
	offsetsDescriptor := buffers + flatbuffers.UOffsetT(3*16)
	valuesDescriptor := buffers + flatbuffers.UOffsetT(4*16)
	offsetsOffset := record.GetInt64(offsetsDescriptor)
	offsetsLength := record.GetInt64(offsetsDescriptor + 8)
	valuesLength := record.GetInt64(valuesDescriptor + 8)
	require.GreaterOrEqual(t, offsetsLength, int64(12))
	require.GreaterOrEqual(t, valuesLength, int64(0))
	bodyStart := int(second.offset + second.metadata)
	lastOffset := bodyStart + int(offsetsOffset) + int(offsetsLength) - 4
	require.LessOrEqual(t, lastOffset+4, len(malformed))
	binary.LittleEndian.PutUint32(malformed[lastOffset:], uint32(valuesLength+1))

	fs := writeMemoryFile(t, "arrow-generation", malformed)
	admission := new(testAdmission)
	reader, err := Open(
		context.Background(), fs, "arrow-generation", int64(len(malformed)),
		ContainerFile, admission, Options{},
	)
	require.NoError(t, err)
	require.True(t, reader.Next())
	retained := reader.RecordBatch()
	retained.Retain()
	olderActive := admission.active.Load()
	require.Positive(t, olderActive)

	require.False(t, reader.Next())
	require.Error(t, reader.Err())
	require.Zero(t, admission.pending.Load())
	require.Equal(t, olderActive, admission.active.Load(), "only the retained older generation may remain")
	require.NoError(t, reader.Close())
	require.Equal(t, olderActive, admission.active.Load())
	require.True(t, array.RecordEqual(expected[0], retained))
	retained.Release()
	require.Zero(t, admission.active.Load())
}

func TestMalformedIPCRecordMetadataReleasesRangeLease(t *testing.T) {
	original, expected := makeIPC(t, ContainerFile)
	releaseRecords(expected)
	inspectionFS := writeMemoryFile(t, "arrow-inspect", original)
	inspectionAdmission := new(testAdmission)
	records, _, err := readFooterBlocks(
		context.Background(), fileservice.NewLeasedRangeReader(inspectionFS), "arrow-inspect",
		int64(len(original)), inspectionAdmission, Options{
			MaxMetadataBytes: DefaultMaxMetadataBytes,
			MaxBodyBytes:     DefaultMaxBodyBytes,
			Allocator:        memory.NewGoAllocator(),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.Zero(t, inspectionAdmission.active.Load())

	block := records[0]
	for _, test := range []struct {
		name      string
		errorText string
		corrupt   func(*testing.T, flatbuffers.Table)
	}{
		{
			name:      "negative-row-count",
			errorText: "invalid row count",
			corrupt: func(t *testing.T, record flatbuffers.Table) {
				lengthOffset := flatbuffers.UOffsetT(record.Offset(4))
				require.NotZero(t, lengthOffset)
				binary.LittleEndian.PutUint64(record.Bytes[lengthOffset+record.Pos:], ^uint64(0))
			},
		},
		{
			name:      "null-count-exceeds-node-length",
			errorText: "invalid length",
			corrupt: func(t *testing.T, record flatbuffers.Table) {
				nodesOffset := flatbuffers.UOffsetT(record.Offset(6))
				require.NotZero(t, nodesOffset)
				nodes := record.Vector(nodesOffset)
				require.Positive(t, record.VectorLen(nodesOffset))
				length := binary.LittleEndian.Uint64(record.Bytes[nodes:])
				binary.LittleEndian.PutUint64(record.Bytes[nodes+8:], length+1)
			},
		},
		{
			name:      "buffer-exceeds-message-body",
			errorText: "exceeds message body",
			corrupt: func(t *testing.T, record flatbuffers.Table) {
				buffersOffset := flatbuffers.UOffsetT(record.Offset(8))
				require.NotZero(t, buffersOffset)
				buffers := record.Vector(buffersOffset)
				require.Positive(t, record.VectorLen(buffersOffset))
				binary.LittleEndian.PutUint64(record.Bytes[buffers+8:], uint64(block.body+1))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fileBytes := append([]byte(nil), original...)
			recordTable := firstIPCRecordTable(t, fileBytes, block)
			test.corrupt(t, recordTable)

			path := "arrow-malformed-" + test.name
			fs := writeMemoryFile(t, path, fileBytes)
			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), fs, path, int64(len(fileBytes)),
				ContainerFile, admission, Options{},
			)
			require.NoError(t, err)
			require.False(t, reader.Next())
			require.ErrorContains(t, reader.Err(), test.errorText)
			require.NoError(t, reader.Close())
			require.Zero(t, admission.pending.Load())
			require.Zero(t, admission.active.Load())
		})
	}
}

func firstIPCRecordTable(t *testing.T, fileBytes []byte, block fileBlock) flatbuffers.Table {
	t.Helper()
	metadata := fileBytes[int(block.offset):int(block.offset+block.metadata)]
	prefix := 4
	if binary.LittleEndian.Uint32(metadata) == ipcContinuationToken {
		prefix = 8
	}
	payload := metadata[prefix:]
	messageTable := flatbuffers.Table{Bytes: payload, Pos: flatbuffers.GetUOffsetT(payload)}
	headerOffset := flatbuffers.UOffsetT(messageTable.Offset(8))
	require.NotZero(t, headerOffset)
	var recordTable flatbuffers.Table
	messageTable.Union(&recordTable, headerOffset)
	return recordTable
}

func TestIPCFilePlanningAndIndependentRecordShard(t *testing.T) {
	payload, expected := makeIPC(t, ContainerFile)
	defer releaseRecords(expected)
	fs := writeMemoryFile(t, "arrow-shard", payload)
	admission := new(testAdmission)
	plan, err := InspectFile(
		context.Background(), fs, "arrow-shard", int64(len(payload)), admission, Options{},
	)
	require.NoError(t, err)
	require.True(t, plan.Schema.Equal(expected[0].Schema()))
	require.Equal(t, []RecordBatchInfo{
		{Index: 0, Rows: 2, WireBytes: plan.RecordBatches[0].WireBytes},
		{Index: 1, Rows: 2, WireBytes: plan.RecordBatches[1].WireBytes},
	}, plan.RecordBatches)
	require.Empty(t, plan.Dictionaries)
	require.Zero(t, admission.active.Load())

	shard, rows, wireBytes, err := plan.Shard(1, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows)
	require.Equal(t, plan.RecordBatches[1].WireBytes, wireBytes)
	reader, err := Open(
		context.Background(), fs, "arrow-shard", int64(len(payload)), ContainerFile,
		admission, Options{FileShard: &shard},
	)
	require.NoError(t, err)
	require.True(t, reader.Next())
	require.True(t, array.RecordEqual(expected[1], reader.RecordBatch()))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	require.Zero(t, admission.active.Load())

	shard.RequiredDictionaryBlockIndices = []int32{0}
	_, err = Open(
		context.Background(), fs, "arrow-shard", int64(len(payload)), ContainerFile,
		admission, Options{FileShard: &shard},
	)
	require.ErrorContains(t, err, "dictionary closure")
}

func TestIPCFilePlanningDictionaryClosure(t *testing.T) {
	payload, expected := makeDictionaryIPC(t, ContainerFile, false)
	defer releaseRecords(expected)
	fs := writeMemoryFile(t, "arrow-dictionary-shard", payload)
	admission := new(testAdmission)
	plan, err := InspectFile(
		context.Background(), fs, "arrow-dictionary-shard", int64(len(payload)), admission, Options{},
	)
	require.NoError(t, err)
	require.Len(t, plan.Dictionaries, 1)
	require.False(t, plan.Dictionaries[0].IsDelta)
	shard, rows, _, err := plan.Shard(1, 2)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)
	require.Equal(t, []int32{0}, shard.RequiredDictionaryBlockIndices)

	reader, err := Open(
		context.Background(), fs, "arrow-dictionary-shard", int64(len(payload)), ContainerFile,
		admission, Options{FileShard: &shard},
	)
	require.NoError(t, err)
	require.True(t, reader.Next())
	require.True(t, array.RecordEqual(expected[1], reader.RecordBatch()))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	require.Zero(t, admission.active.Load())
}

func TestDictionaryTransitionStateMachine(t *testing.T) {
	seen := make(map[int64]struct{})
	require.ErrorContains(t, acceptDictionaryTransition(seen, 7, true), "delta precedes")
	require.NoError(t, acceptDictionaryTransition(seen, 7, false))
	require.NoError(t, acceptDictionaryTransition(seen, 7, true))
	require.ErrorContains(t, acceptDictionaryTransition(seen, 7, false), "replacement base")
	require.NoError(t, acceptDictionaryTransition(seen, 8, false))
}

func TestIPCStreamReaderAndContainerValidation(t *testing.T) {
	streamBytes, expected := makeIPC(t, ContainerStream)
	fs := writeMemoryFile(t, "arrow-stream", streamBytes)
	reader, err := Open(context.Background(), fs, "arrow-stream", int64(len(streamBytes)), ContainerAuto, new(testAdmission), Options{})
	require.NoError(t, err)
	for i := range expected {
		require.True(t, reader.Next())
		require.True(t, array.RecordEqual(expected[i], reader.RecordBatch()))
	}
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	releaseRecords(expected)

	bad := writeMemoryFile(t, "bad-arrow", []byte("not arrow"))
	_, err = Open(context.Background(), bad, "bad-arrow", 9, ContainerAuto, new(testAdmission), Options{})
	require.Error(t, err)
}

func TestIPCStreamRequiresEOSMarker(t *testing.T) {
	payload, expected := makeIPC(t, ContainerStream)
	defer releaseRecords(expected)
	require.GreaterOrEqual(t, len(payload), 4)
	truncated := payload[:len(payload)-4]
	fs := writeMemoryFile(t, "arrow-stream-missing-eos", truncated)
	reader, err := Open(
		context.Background(), fs, "arrow-stream-missing-eos", int64(len(truncated)), ContainerStream,
		new(testAdmission), Options{},
	)
	require.NoError(t, err)
	for index := range expected {
		require.True(t, reader.Next(), "record %d: %v", index, reader.Err())
		require.True(t, array.RecordEqual(expected[index], reader.RecordBatch()))
	}
	require.False(t, reader.Next())
	require.Error(t, reader.Err(), "a stream without its EOS marker is truncated")
	require.NoError(t, reader.Close())
}

func TestIPCFileAndStreamCompression(t *testing.T) {
	for _, test := range []struct {
		name      string
		container Container
		codec     ipc.Option
	}{
		{name: "file lz4", container: ContainerFile, codec: ipc.WithLZ4()},
		{name: "file zstd", container: ContainerFile, codec: ipc.WithZstd()},
		{name: "stream lz4", container: ContainerStream, codec: ipc.WithLZ4()},
		{name: "stream zstd", container: ContainerStream, codec: ipc.WithZstd()},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, expected := makeIPCWithOptions(t, test.container, test.codec)
			defer releaseRecords(expected)
			fs := writeMemoryFile(t, "arrow-compressed", payload)
			admission := new(testAdmission)
			options := Options{MaxDecodedRecordBytes: 1024}
			if test.container == ContainerFile {
				plan, err := InspectFile(
					context.Background(), fs, "arrow-compressed", int64(len(payload)), admission, options,
				)
				require.NoError(t, err)
				require.Len(t, plan.RecordBatches, len(expected))
				require.Zero(t, admission.active.Load())
			}

			reader, err := Open(
				context.Background(), fs, "arrow-compressed", int64(len(payload)), test.container, admission, options,
			)
			require.NoError(t, err)
			for index := range expected {
				require.True(t, reader.Next(), "record %d: %v", index, reader.Err())
				require.True(t, array.RecordEqual(expected[index], reader.RecordBatch()))
			}
			require.False(t, reader.Next())
			require.NoError(t, reader.Err())
			require.NoError(t, reader.Close())
			require.Zero(t, admission.active.Load())
		})
	}
}

func TestIPCCompressionMetadataRejectedBeforeDecodeAllocation(t *testing.T) {
	for _, container := range []Container{ContainerFile, ContainerStream} {
		for _, test := range []struct {
			name      string
			errorText string
			corrupt   func(*testing.T, []byte, ipcRecordLocation)
		}{
			{
				name:      "decoded-size-exceeds-limit",
				errorText: "decoded record body exceeds limit",
				corrupt: func(t *testing.T, payload []byte, location ipcRecordLocation) {
					bufferOffset, _, _ := firstNonEmptyIPCBuffer(t, location.record)
					binary.LittleEndian.PutUint64(
						payload[location.bodyStart+int(bufferOffset):], uint64(1025),
					)
				},
			},
			{
				name:      "negative-decoded-size",
				errorText: "invalid decoded size",
				corrupt: func(t *testing.T, payload []byte, location ipcRecordLocation) {
					bufferOffset, _, _ := firstNonEmptyIPCBuffer(t, location.record)
					binary.LittleEndian.PutUint64(
						payload[location.bodyStart+int(bufferOffset):], ^uint64(1),
					)
				},
			},
			{
				name:      "missing-decoded-size-prefix",
				errorText: "shorter than its decoded-size prefix",
				corrupt: func(t *testing.T, _ []byte, location ipcRecordLocation) {
					_, _, descriptor := firstNonEmptyIPCBuffer(t, location.record)
					binary.LittleEndian.PutUint64(location.record.Bytes[descriptor+8:], uint64(7))
				},
			},
			{
				name:      "unsupported-codec",
				errorText: "compression codec 127 is unsupported",
				corrupt: func(t *testing.T, _ []byte, location ipcRecordLocation) {
					compressionOffset := flatbuffers.UOffsetT(location.record.Offset(10))
					require.NotZero(t, compressionOffset)
					compressionPosition := location.record.Indirect(compressionOffset + location.record.Pos)
					compression := flatbuffers.Table{Bytes: location.record.Bytes, Pos: compressionPosition}
					codecOffset := flatbuffers.UOffsetT(compression.Offset(4))
					require.NotZero(t, codecOffset, "ZSTD codec must be materialized in the flatbuffer")
					compression.Bytes[codecOffset+compression.Pos] = 127
				},
			},
		} {
			t.Run(containerName(container)+"/"+test.name, func(t *testing.T) {
				payload, expected := makeIPCWithOptions(t, container, ipc.WithZstd())
				releaseRecords(expected)
				location := locateFirstIPCRecord(t, payload, container)
				test.corrupt(t, payload, location)

				path := "arrow-compression-metadata-" + containerName(container) + "-" + test.name
				fs := writeMemoryFile(t, path, payload)
				admission := new(testAdmission)
				reader, err := Open(
					context.Background(), fs, path, int64(len(payload)), container, admission,
					Options{MaxDecodedRecordBytes: 1024},
				)
				require.NoError(t, err)
				require.False(t, reader.Next())
				require.ErrorContains(t, reader.Err(), test.errorText)
				require.NoError(t, reader.Close())
				require.Zero(t, admission.pending.Load())
				require.Zero(t, admission.active.Load())
			})
		}
	}
}

type ipcRecordLocation struct {
	record    flatbuffers.Table
	bodyStart int
}

func locateFirstIPCRecord(t *testing.T, payload []byte, container Container) ipcRecordLocation {
	t.Helper()
	if container == ContainerFile {
		fs := writeMemoryFile(t, "arrow-compression-location", payload)
		admission := new(testAdmission)
		options, err := normalizeOptions(Options{Allocator: memory.NewGoAllocator()})
		require.NoError(t, err)
		records, _, err := readFooterBlocks(
			context.Background(), fileservice.NewLeasedRangeReader(fs),
			"arrow-compression-location", int64(len(payload)), admission, options,
		)
		require.NoError(t, err)
		require.NotEmpty(t, records)
		require.Zero(t, admission.active.Load())
		return ipcRecordLocation{
			record:    firstIPCRecordTable(t, payload, records[0]),
			bodyStart: int(records[0].offset + records[0].metadata),
		}
	}

	for cursor := 0; cursor+4 <= len(payload); {
		metadataLength := binary.LittleEndian.Uint32(payload[cursor:])
		cursor += 4
		if metadataLength == ipcContinuationToken {
			require.LessOrEqual(t, cursor+4, len(payload))
			metadataLength = binary.LittleEndian.Uint32(payload[cursor:])
			cursor += 4
		}
		if metadataLength == 0 {
			break
		}
		require.GreaterOrEqual(t, metadataLength, uint32(4))
		require.LessOrEqual(t, uint64(cursor)+uint64(metadataLength), uint64(len(payload)))
		metadata := payload[cursor : cursor+int(metadataLength)]
		message := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.GetUOffsetT(metadata)}
		headerTypeOffset := flatbuffers.UOffsetT(message.Offset(6))
		require.NotZero(t, headerTypeOffset)
		headerType := message.GetByte(headerTypeOffset + message.Pos)
		bodyLengthOffset := flatbuffers.UOffsetT(message.Offset(10))
		var bodyLength int64
		if bodyLengthOffset != 0 {
			bodyLength = message.GetInt64(bodyLengthOffset + message.Pos)
		}
		require.GreaterOrEqual(t, bodyLength, int64(0))
		bodyStart := cursor + int(metadataLength)
		require.LessOrEqual(t, uint64(bodyStart)+uint64(bodyLength), uint64(len(payload)))
		if headerType == byte(ipc.MessageRecordBatch) {
			headerOffset := flatbuffers.UOffsetT(message.Offset(8))
			require.NotZero(t, headerOffset)
			var record flatbuffers.Table
			message.Union(&record, headerOffset)
			return ipcRecordLocation{record: record, bodyStart: bodyStart}
		}
		cursor = bodyStart + int(bodyLength)
	}
	t.Fatal("Arrow IPC Stream has no record batch")
	return ipcRecordLocation{}
}

func firstNonEmptyIPCBuffer(t *testing.T, record flatbuffers.Table) (int64, int64, flatbuffers.UOffsetT) {
	t.Helper()
	buffersOffset := flatbuffers.UOffsetT(record.Offset(8))
	require.NotZero(t, buffersOffset)
	buffers := record.Vector(buffersOffset)
	for index := 0; index < record.VectorLen(buffersOffset); index++ {
		descriptor := buffers + flatbuffers.UOffsetT(index*16)
		offset := record.GetInt64(descriptor)
		length := record.GetInt64(descriptor + 8)
		if length > 0 {
			return offset, length, descriptor
		}
	}
	t.Fatal("Arrow record has no non-empty body buffer")
	return 0, 0, 0
}

func TestCompressedIPCRecordOutlivesReaderAndOwnsAllocation(t *testing.T) {
	for _, test := range []struct {
		name      string
		container Container
		codec     ipc.Option
	}{
		{name: "file-lz4", container: ContainerFile, codec: ipc.WithLZ4()},
		{name: "file-zstd", container: ContainerFile, codec: ipc.WithZstd()},
		{name: "stream-lz4", container: ContainerStream, codec: ipc.WithLZ4()},
		{name: "stream-zstd", container: ContainerStream, codec: ipc.WithZstd()},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, expected := makeIPCWithOptions(t, test.container, test.codec)
			defer releaseRecords(expected)
			path := "arrow-compressed-lifetime-" + test.name
			fs := writeMemoryFile(t, path, payload)
			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), fs, path, int64(len(payload)), test.container, admission, Options{},
			)
			require.NoError(t, err)
			require.True(t, reader.Next())
			record := reader.RecordBatch()
			record.Retain()
			require.NoError(t, reader.Close())
			require.Positive(t, admission.active.Load(), "retained record must keep decoded or source buffers admitted")
			require.True(t, array.RecordEqual(expected[0], record))
			record.Release()
			require.Zero(t, admission.pending.Load())
			require.Zero(t, admission.active.Load())
		})
	}
}

func TestIPCLocalDiskFileAndStream(t *testing.T) {
	for _, container := range []Container{ContainerFile, ContainerStream} {
		t.Run(containerName(container), func(t *testing.T) {
			payload, expected := makeIPC(t, container)
			defer releaseRecords(expected)
			fs, err := fileservice.NewLocalFS2(
				context.Background(), "arrow-disk", t.TempDir(), fileservice.DisabledCacheConfig, nil,
			)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			path := "arrow-disk:input.arrow"
			require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
				FilePath: path,
				Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
			}))

			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), fs, path, int64(len(payload)), ContainerAuto, admission, Options{},
			)
			require.NoError(t, err)
			for index := range expected {
				require.True(t, reader.Next(), "record %d: %v", index, reader.Err())
				require.True(t, array.RecordEqual(expected[index], reader.RecordBatch()))
			}
			require.False(t, reader.Next())
			require.NoError(t, reader.Err())
			require.NoError(t, reader.Close())
			require.Zero(t, admission.pending.Load())
			require.Zero(t, admission.active.Load())
		})
	}
}

func containerName(container Container) string {
	switch container {
	case ContainerFile:
		return "file"
	case ContainerStream:
		return "stream"
	default:
		return "unknown"
	}
}

func TestIPCFileLimitsCancellationAndAdmission(t *testing.T) {
	fileBytes, expected := makeIPC(t, ContainerFile)
	defer releaseRecords(expected)
	fs := writeMemoryFile(t, "arrow-limits", fileBytes)
	_, err := Open(context.Background(), fs, "arrow-limits", int64(len(fileBytes)), ContainerFile, new(testAdmission), Options{MaxMetadataBytes: 4})
	require.Error(t, err)
	_, err = Open(context.Background(), fs, "arrow-limits", int64(len(fileBytes)), ContainerFile, new(testAdmission), Options{MaxDecodedRecordBytes: -1})
	require.ErrorContains(t, err, "invalid Arrow IPC size limits")

	reject := errors.New("range quota exceeded")
	_, err = Open(context.Background(), fs, "arrow-limits", int64(len(fileBytes)), ContainerFile, &testAdmission{reject: reject}, Options{})
	require.ErrorIs(t, err, reject)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Open(ctx, fs, "arrow-limits", int64(len(fileBytes)), ContainerFile, new(testAdmission), Options{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestIPCBodyLimitForFileAndStream(t *testing.T) {
	for _, container := range []Container{ContainerFile, ContainerStream} {
		t.Run(containerName(container), func(t *testing.T) {
			payload, expected := makeIPC(t, container)
			defer releaseRecords(expected)
			fs := writeMemoryFile(t, "arrow-body-limit", payload)
			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), fs, "arrow-body-limit", int64(len(payload)), container,
				admission, Options{MaxBodyBytes: 1},
			)
			if err == nil {
				require.False(t, reader.Next())
				require.Error(t, reader.Err())
				require.NoError(t, reader.Close())
			} else {
				require.Nil(t, reader)
			}
			require.Zero(t, admission.pending.Load())
			require.Zero(t, admission.active.Load())
		})
	}
}

func TestIPCDictionaryReplayForFileAndStreamDelta(t *testing.T) {
	for _, test := range []struct {
		name      string
		container Container
		delta     bool
	}{
		{name: "file base dictionary", container: ContainerFile},
		{name: "stream dictionary delta", container: ContainerStream, delta: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, expected := makeDictionaryIPC(t, test.container, test.delta)
			defer releaseRecords(expected)
			fs := writeMemoryFile(t, "arrow-dictionary", payload)
			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), fs, "arrow-dictionary", int64(len(payload)), test.container, admission, Options{},
			)
			require.NoError(t, err)
			for index := range expected {
				require.True(t, reader.Next(), "record %d: %v", index, reader.Err())
				require.True(t, array.RecordEqual(expected[index], reader.RecordBatch()))
			}
			require.False(t, reader.Next())
			require.NoError(t, reader.Err())
			require.NoError(t, reader.Close())
			require.Zero(t, admission.active.Load())
		})
	}
}

func TestIPCConditionalIdentityPreventsMixedFileVersions(t *testing.T) {
	payload, expectedRecords := makeIPC(t, ContainerFile)
	defer releaseRecords(expectedRecords)
	base := writeMemoryFile(t, "arrow-identity", payload)
	planned := fileservice.ObjectIdentity{ETag: "etag-v1", Size: int64(len(payload))}
	fs := newIdentityMemoryFS(base, planned)
	reader, err := Open(
		context.Background(), fs, "arrow-identity", int64(len(payload)), ContainerFile,
		new(testAdmission), Options{ExpectedIdentity: &planned},
	)
	require.NoError(t, err)

	changed := fileservice.ObjectIdentity{ETag: "etag-v2", Size: int64(len(payload))}
	fs.identity.Store(&changed)
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), fileservice.ErrObjectChanged)
	require.NoError(t, reader.Close())
}

func TestIPCConditionalIdentitySupportsSingleStreamGET(t *testing.T) {
	payload, expectedRecords := makeIPC(t, ContainerStream)
	defer releaseRecords(expectedRecords)
	base := writeMemoryFile(t, "arrow-stream-identity", payload)
	planned := fileservice.ObjectIdentity{VersionID: "version-1", Size: int64(len(payload))}
	fs := newIdentityMemoryFS(base, planned)
	reader, err := Open(
		context.Background(), fs, "arrow-stream-identity", int64(len(payload)), ContainerStream,
		new(testAdmission), Options{ExpectedIdentity: &planned},
	)
	require.NoError(t, err)
	for index := range expectedRecords {
		require.True(t, reader.Next())
		require.True(t, array.RecordEqual(expectedRecords[index], reader.RecordBatch()))
	}
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
}

func makeIPC(t testing.TB, container Container) ([]byte, []arrow.RecordBatch) {
	return makeIPCWithOptions(t, container)
}

func makeIPCWithOptions(t testing.TB, container Container, options ...ipc.Option) ([]byte, []arrow.RecordBatch) {
	t.Helper()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	records := make([]arrow.RecordBatch, 0, 2)
	for batchIndex := 0; batchIndex < 2; batchIndex++ {
		builder := array.NewRecordBuilder(alloc, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues(
			[]int64{int64(batchIndex*2 + 1), int64(batchIndex*2 + 2)}, nil,
		)
		builder.Field(1).(*array.StringBuilder).AppendValues(
			[]string{"a payload longer than twenty three bytes", "short"}, []bool{true, batchIndex == 0},
		)
		records = append(records, builder.NewRecordBatch())
		builder.Release()
	}
	var output bytes.Buffer
	writerOptions := append([]ipc.Option{
		ipc.WithSchema(schema),
		ipc.WithAllocator(alloc),
	}, options...)
	if container == ContainerFile {
		writer, err := ipc.NewFileWriter(&output, writerOptions...)
		require.NoError(t, err)
		for _, record := range records {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.Close())
	} else {
		writer := ipc.NewWriter(&output, writerOptions...)
		for _, record := range records {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.Close())
	}
	return output.Bytes(), records
}

func makeDictionaryIPC(t testing.TB, container Container, delta bool) ([]byte, []arrow.RecordBatch) {
	t.Helper()
	alloc := memory.NewGoAllocator()
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "name", Type: dictionaryType}}, nil)
	valueSets := [][]string{{"alpha"}, {"alpha"}}
	indices := []int8{0, 0}
	if delta {
		valueSets[1] = []string{"alpha", "beta"}
		indices[1] = 1
	}
	records := make([]arrow.RecordBatch, 0, len(valueSets))
	for recordIndex, valueSet := range valueSets {
		indexBuilder := array.NewInt8Builder(alloc)
		indexBuilder.Append(indices[recordIndex])
		indexArray := indexBuilder.NewArray()
		indexBuilder.Release()
		valueBuilder := array.NewStringBuilder(alloc)
		valueBuilder.AppendValues(valueSet, nil)
		valueArray := valueBuilder.NewArray()
		valueBuilder.Release()
		dictionary := array.NewDictionaryArray(dictionaryType, indexArray, valueArray)
		records = append(records, array.NewRecordBatch(schema, []arrow.Array{dictionary}, 1))
		dictionary.Release()
		indexArray.Release()
		valueArray.Release()
	}

	var output bytes.Buffer
	if container == ContainerFile {
		writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
		require.NoError(t, err)
		for _, record := range records {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.Close())
	} else {
		writer := ipc.NewWriter(
			&output,
			ipc.WithSchema(schema),
			ipc.WithAllocator(alloc),
			ipc.WithDictionaryDeltas(delta),
		)
		for _, record := range records {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.Close())
	}
	return output.Bytes(), records
}

func writeMemoryFile(t testing.TB, path string, data []byte) *fileservice.MemoryFS {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("arrow-test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}},
	}))
	return fs
}

func releaseRecords(records []arrow.RecordBatch) {
	for _, record := range records {
		record.Release()
	}
}

func FuzzArrowIPCPlanningAndOpenNeverPanicOrLeak(f *testing.F) {
	filePayload, fileRecords := makeIPC(f, ContainerFile)
	releaseRecords(fileRecords)
	streamPayload, streamRecords := makeIPC(f, ContainerStream)
	releaseRecords(streamRecords)
	f.Add(append([]byte(nil), filePayload...))
	f.Add(append([]byte(nil), streamPayload...))
	f.Add([]byte("not arrow"))
	f.Add([]byte{})
	if len(filePayload) > 16 {
		f.Add(append([]byte(nil), filePayload[len(filePayload)-16:]...))
	}

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 2<<20 {
			return
		}
		// Keep concurrent fuzz workers below the process memory ceiling. The
		// production default is protected by statement/global admission, whereas
		// this intentionally simple test admission accepts every reservation.
		options := Options{
			MaxMetadataBytes:      1 << 20,
			MaxBodyBytes:          2 << 20,
			MaxDecodedRecordBytes: 2 << 20,
		}
		fs := writeMemoryFile(t, "arrow-fuzz", append([]byte(nil), payload...))
		admission := new(testAdmission)
		plan, _ := InspectFile(
			context.Background(), fs, "arrow-fuzz", int64(len(payload)), admission, options,
		)
		if plan != nil {
			require.NotNil(t, plan.Schema)
		}
		require.Zero(t, admission.active.Load())

		reader, openErr := Open(
			context.Background(), fs, "arrow-fuzz", int64(len(payload)), ContainerAuto, admission, options,
		)
		var readErr error
		if reader != nil {
			for records := 0; records < 1024 && reader.Next(); records++ {
				require.NotNil(t, reader.RecordBatch())
			}
			readErr = reader.Err()
			require.NoError(t, reader.Close())
		}
		require.Zero(t, admission.active.Load(),
			"reader=%T openErr=%v readErr=%v reserved=%d released=%d pending=%d max=%d",
			reader, openErr, readErr, admission.reserved.Load(), admission.released.Load(),
			admission.pending.Load(), admission.max.Load())
	})
}
