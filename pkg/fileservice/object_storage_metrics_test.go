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

func TestObjectStorageMetricsReadCloseDecrementsActive(t *testing.T) {
	name := t.Name()
	upstream := &recordingObjectStorage{}
	wrapped := newObjectStorageMetrics(upstream, name)
	active := metric.FSObjectStorageOperations.WithLabelValues(name, "active-read")
	before := testutil.ToFloat64(active)

	r, err := wrapped.Read(context.Background(), "key", nil, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, testutil.ToFloat64(active))
	require.NoError(t, r.Close())
	require.Equal(t, before, testutil.ToFloat64(active))
}
