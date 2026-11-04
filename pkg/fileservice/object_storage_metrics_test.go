// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestObjectStorageMetricsWriteMultipartParallel(t *testing.T) {
	name := t.Name()
	upstream := &mockParallelObjectStorage{supports: true}
	wrapped := newObjectStorageMetrics(upstream, name)
	gauge := metric.FSObjectStorageOperations.WithLabelValues(name, "write")
	before := testutil.ToFloat64(gauge)

	err := wrapped.WriteMultipartParallel(context.Background(), "key", strings.NewReader("data"), nil, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, testutil.ToFloat64(gauge))
	require.Equal(t, "key", upstream.key)
}

func TestObjectStorageMetricsWriteMultipartParallelNotSupported(t *testing.T) {
	name := t.Name()
	wrapped := newObjectStorageMetrics(dummyObjectStorage{}, name)
	gauge := metric.FSObjectStorageOperations.WithLabelValues(name, "write")
	before := testutil.ToFloat64(gauge)

	err := wrapped.WriteMultipartParallel(context.Background(), "key", strings.NewReader("data"), nil, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.Equal(t, before+1, testutil.ToFloat64(gauge))
}

func TestObjectStorageMetricsDelegates(t *testing.T) {
	name := t.Name()
	upstream := &recordingObjectStorage{}
	wrapped := newObjectStorageMetrics(upstream, name)

	require.NoError(t, wrapped.Delete(context.Background(), "a"))
	_, _ = wrapped.Exists(context.Background(), "b")
	seq := wrapped.List(context.Background(), "c")
	seq(func(_ *DirEntry, _ error) bool { return true })
	rc, err := wrapped.Read(context.Background(), "d", nil, nil)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	_, _ = wrapped.Stat(context.Background(), "e")
	require.NoError(t, wrapped.Write(context.Background(), "f", strings.NewReader("x"), nil, nil))

	require.ElementsMatch(t, []string{
		"delete", "exists", "list", "read", "stat", "write",
	}, upstream.calls)

	// gauges incremented
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "delete")) >= 1)
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "exists")) >= 1)
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "list")) >= 1)
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "read")) >= 1)
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "stat")) >= 1)
	require.True(t, testutil.ToFloat64(metric.FSObjectStorageOperations.WithLabelValues(name, "write")) >= 1)
}

func TestObjectStorageMetricsCopyObject(t *testing.T) {
	name := t.Name()
	src := &testObjectCopyStorage{}
	upstream := &testObjectCopyStorage{}
	wrapped := newObjectStorageMetrics(upstream, name)
	writeGauge := metric.FSObjectStorageOperations.WithLabelValues(name, "write")
	before := testutil.ToFloat64(writeGauge)

	copied, err := wrapped.CopyObject(context.Background(), src, "source", "destination")
	require.NoError(t, err)
	require.True(t, copied)
	require.Equal(t, before+1, testutil.ToFloat64(writeGauge))
	require.Same(t, src, upstream.src)
	require.Equal(t, "source", upstream.srcKey)
	require.Equal(t, "destination", upstream.dstKey)

	copied, err = newObjectStorageMetrics(dummyObjectStorage{}, t.Name()+"-unsupported").CopyObject(
		context.Background(), src, "source", "destination",
	)
	require.NoError(t, err)
	require.False(t, copied)
}

func TestObjectStorageMetricsReadLifecycle(t *testing.T) {
	for _, conditional := range []bool{false, true} {
		name := "ordinary"
		if conditional {
			name = "identity"
		}
		t.Run(name, func(t *testing.T) {
			for _, closeErr := range []error{nil, errors.New("close failed")} {
				label := "success"
				if closeErr != nil {
					label = "close-error"
				}
				t.Run(label, func(t *testing.T) {
					var closes int
					var wrapped *objectStorageMetrics
					var readBefore, activeBefore float64
					body := &readCloser{r: strings.NewReader("abc"), closeFunc: func() error { closes++; return closeErr }}
					source := &readTestObjectStorage{open: func(ctx context.Context, key string, min, max *int64, expected *ObjectIdentity) (io.ReadCloser, error) {
						require.Equal(t, "key", key)
						require.Equal(t, int64(1), *min)
						require.Equal(t, int64(4), *max)
						if conditional {
							require.Equal(t, &ObjectIdentity{ETag: "version1", Size: 8}, expected)
						} else {
							require.Nil(t, expected)
						}
						require.Equal(t, readBefore+1, testutil.ToFloat64(wrapped.numRead))
						require.Equal(t, activeBefore+1, testutil.ToFloat64(wrapped.numActiveRead))
						return body, nil
					}}
					wrapped = newObjectStorageMetrics(source, t.Name())
					readBefore, activeBefore = testutil.ToFloat64(wrapped.numRead), testutil.ToFloat64(wrapped.numActiveRead)
					reader, err := openTestObject(t.Context(), wrapped, conditional)
					require.NoError(t, err)
					t.Cleanup(func() { _ = reader.Close() })
					data, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.Equal(t, "abc", string(data))
					require.Equal(t, activeBefore+1, testutil.ToFloat64(wrapped.numActiveRead), "EOF does not close the body")
					require.ErrorIs(t, reader.Close(), closeErr)
					require.ErrorIs(t, reader.Close(), closeErr)
					require.Equal(t, 1, closes)
					require.Equal(t, activeBefore, testutil.ToFloat64(wrapped.numActiveRead))
				})
			}
			openErr := errors.New("open failed")
			wrapped := newObjectStorageMetrics(&readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
				return nil, openErr
			}}, t.Name()+"/open-error")
			readBefore, activeBefore := testutil.ToFloat64(wrapped.numRead), testutil.ToFloat64(wrapped.numActiveRead)
			reader, err := openTestObject(t.Context(), wrapped, conditional)
			require.Nil(t, reader)
			require.ErrorIs(t, err, openErr)
			require.Equal(t, readBefore+1, testutil.ToFloat64(wrapped.numRead))
			require.Equal(t, activeBefore, testutil.ToFloat64(wrapped.numActiveRead))
		})
	}
}
func TestObjectStorageMetricsReadConcurrentClose(t *testing.T) {
	var wrapped *objectStorageMetrics
	testObjectStorageReadConcurrentClose(t, func(body io.ReadCloser) io.ReadCloser {
		wrapped = newObjectStorageMetrics(&readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
			return body, nil
		}}, t.Name())
		reader, err := openTestObject(t.Context(), wrapped, false)
		require.NoError(t, err)
		return reader
	}, func() { require.Zero(t, testutil.ToFloat64(wrapped.numActiveRead)) })
}

func BenchmarkObjectStorageReadLifecycle(b *testing.B) {
	for _, kind := range []string{"semaphore", "metrics", "stack"} {
		for _, conditional := range []bool{false, true} {
			name := "ordinary"
			if conditional {
				name = "identity"
			}
			b.Run(kind+"/"+name, func(b *testing.B) {
				source := &readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("x")), nil
				}}
				var storage ObjectStorage = source
				if kind != "metrics" {
					storage = newObjectStorageSemaphore(storage, 1)
				}
				if kind != "semaphore" {
					storage = newObjectStorageMetrics(storage, b.Name())
				}
				buf := make([]byte, 1)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					reader, err := openTestObject(ctx, storage, conditional)
					if err != nil {
						b.Fatal(err)
					}
					if _, err = reader.Read(buf); err != nil {
						b.Fatal(err)
					}
					if err = reader.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	b.Run("closed-stack", func(b *testing.B) {
		storage := newObjectStorageMetrics(newObjectStorageSemaphore(dummyObjectStorage{}, 1), b.Name())
		reader, err := storage.Read(context.Background(), "key", nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err = reader.Close(); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { storage.numActiveRead.Set(0) })
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err = reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestObjectStorageReadUnsupportedIdentity(t *testing.T) {
	for _, kind := range []string{"semaphore", "metrics", "stack"} {
		t.Run(kind, func(t *testing.T) {
			var storage ObjectStorage = dummyObjectStorage{}
			var sem *objectStorageSemaphore
			var metrics *objectStorageMetrics
			if kind != "metrics" {
				sem = newObjectStorageSemaphore(storage, 1)
				storage = sem
			}
			if kind != "semaphore" {
				metrics = newObjectStorageMetrics(storage, t.Name())
				storage = metrics
			}
			var readBefore, activeBefore float64
			if metrics != nil {
				readBefore = testutil.ToFloat64(metrics.numRead)
				activeBefore = testutil.ToFloat64(metrics.numActiveRead)
			}
			reader, err := openTestObject(t.Context(), storage, true)
			require.Nil(t, reader)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			if sem != nil {
				require.Empty(t, sem.semaphore)
			}
			if metrics != nil {
				delta := float64(0)
				if kind == "stack" {
					delta = 1
				}
				require.Equal(t, readBefore+delta, testutil.ToFloat64(metrics.numRead))
				require.Equal(t, activeBefore, testutil.ToFloat64(metrics.numActiveRead))
			}
		})
	}
}
