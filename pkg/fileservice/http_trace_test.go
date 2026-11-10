// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func traceSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	m := new(dto.Metric)
	require.NoError(t, observer.(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}

func TestHTTPTraceTimingPairs(t *testing.T) {
	info := newTraceInfo()
	observers := []prometheus.Observer{metric.S3GetConnDurationHistogram, metric.S3GotFirstResponseDurationHistogram, metric.S3DNSResolveDurationHistogram, metric.S3ConnectDurationHistogram, metric.S3TLSHandshakeDurationHistogram}
	counts := make([]uint64, len(observers))
	for i, o := range observers {
		counts[i] = traceSampleCount(t, o)
	}
	info.GotConn(httptrace.GotConnInfo{})
	info.GotFirstResponseByte()
	info.DNSDone(httptrace.DNSDoneInfo{})
	info.ConnectDone("tcp", "missing", nil)
	info.TLSHandshakeDone(tls.ConnectionState{}, nil)
	for i, o := range observers {
		require.Equal(t, counts[i], traceSampleCount(t, o), "unmatched phase %d", i)
	}
	info = newTraceInfo()
	info.GetConn("host")
	info.GotConn(httptrace.GotConnInfo{})
	info.GotFirstResponseByte()
	info.DNSStart(httptrace.DNSStartInfo{})
	info.DNSDone(httptrace.DNSDoneInfo{})
	info.TLSHandshakeStart()
	info.TLSHandshakeDone(tls.ConnectionState{}, nil)
	info.ConnectStart("tcp", "v4")
	info.ConnectStart("tcp", "v6")
	info.ConnectDone("tcp", "v6", nil)
	info.ConnectDone("tcp", "v4", nil)
	for i, o := range observers {
		want := counts[i] + 1
		if i == 3 {
			want++
		}
		require.Equal(t, want, traceSampleCount(t, o))
	}
	before := traceSampleCount(t, metric.S3GotFirstResponseDurationHistogram)
	info.GetConn("retry")
	info.GotConn(httptrace.GotConnInfo{})
	info.GotFirstResponseByte()
	require.Equal(t, before, traceSampleCount(t, metric.S3GotFirstResponseDurationHistogram))
	for _, phase := range []struct {
		start, finish func()
		observer      prometheus.Observer
	}{
		{func() { info.DNSStart(httptrace.DNSStartInfo{}) }, func() { info.DNSDone(httptrace.DNSDoneInfo{}) }, metric.S3DNSResolveDurationHistogram},
		{info.TLSHandshakeStart, func() { info.TLSHandshakeDone(tls.ConnectionState{}, nil) }, metric.S3TLSHandshakeDurationHistogram},
		{func() { info.ConnectStart("tcp", "overlap") }, func() { info.ConnectDone("tcp", "overlap", nil) }, metric.S3ConnectDurationHistogram},
	} {
		count := traceSampleCount(t, phase.observer)
		phase.start()
		phase.start()
		phase.finish()
		phase.finish()
		require.Equal(t, count, traceSampleCount(t, phase.observer), "ambiguous timing")
	}
}

// Keep callbacks alive past RoundTrip, then overlap another request and other
// hook types. Channels control ownership; no real network or timing race is
// needed to exercise httptrace's asynchronous callback contract.
func TestHTTPTraceLateAndConcurrentCallbacks(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "error"}[failed], func(t *testing.T) {
			var got []*httptrace.ClientTrace
			wantErr := errors.New("upstream failed")
			transport := wrapRoundTripper(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				tr := httptrace.ContextClientTrace(req.Context())
				require.NotNil(t, tr)
				got = append(got, tr)
				tr.GetConn("host")
				tr.GotConn(httptrace.GotConnInfo{})
				if failed {
					return nil, wantErr
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("value")), Header: make(http.Header)}, nil
			}))
			var parent [2]atomic.Int64
			for i := range 2 {
				ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotFirstResponseByte: func() { parent[i].Add(1) }})
				req, err := http.NewRequestWithContext(withHTTPTrace(ctx), http.MethodGet, "http://local/key", nil)
				require.NoError(t, err)
				resp, err := transport.RoundTrip(req)
				if failed {
					require.ErrorIs(t, err, wantErr)
				} else {
					require.NoError(t, err)
					data, readErr := io.ReadAll(resp.Body)
					require.NoError(t, resp.Body.Close())
					require.NoError(t, readErr)
					require.Equal(t, "value", string(data))
				}
			}
			require.NotSame(t, got[0], got[1])
			var wg sync.WaitGroup
			for i := range 2 {
				wg.Go(func() {
					for range 100 {
						got[i].GetConn("retry")
						got[i].GotConn(httptrace.GotConnInfo{})
						got[i].DNSStart(httptrace.DNSStartInfo{})
						got[i].DNSDone(httptrace.DNSDoneInfo{})
						got[i].ConnectStart("tcp", "addr")
						got[i].ConnectDone("tcp", "addr", nil)
						got[i].TLSHandshakeStart()
						got[i].TLSHandshakeDone(tls.ConnectionState{}, nil)
						got[i].GotFirstResponseByte()
					}
				})
			}
			// The same request's callback fields can also be accessed concurrently.
			wg.Go(func() {
				for range 100 {
					got[0].GotFirstResponseByte()
					got[0].DNSDone(httptrace.DNSDoneInfo{})
				}
			})
			wg.Wait()
			require.Equal(t, int64(200), parent[0].Load())
			require.Equal(t, int64(100), parent[1].Load())
		})
	}
}

type traceListStorage struct {
	dummyObjectStorage
	request func(context.Context)
}

func (s traceListStorage) List(ctx context.Context, _ string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		for range 2 {
			s.request(ctx)
			if !yield(&DirEntry{Name: "entry"}, nil) {
				return
			}
		}
	}
}

func TestHTTPTraceLazyListUsesRequestScope(t *testing.T) {
	calls := 0
	var got []*httptrace.ClientTrace
	transport := wrapRoundTripper(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		tr := httptrace.ContextClientTrace(req.Context())
		require.NotNil(t, tr)
		got = append(got, tr)
		calls++
		tr.GetConn("host")
		tr.GotConn(httptrace.GotConnInfo{})
		tr.GotFirstResponseByte()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	}))
	storage := traceListStorage{request: func(ctx context.Context) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://local/list", nil)
		require.NoError(t, err)
		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}}
	seq := newObjectStorageHTTPTrace(storage).List(context.Background(), "")
	require.Zero(t, calls)
	for entry, err := range seq {
		require.NoError(t, err)
		require.Equal(t, "entry", entry.Name)
		break
	}
	require.Equal(t, 1, calls, "early stop must not fetch another page")
	for range seq {
	}
	require.Equal(t, 3, calls)
	require.NotSame(t, got[0], got[1])
	require.NotSame(t, got[1], got[2])
}

func BenchmarkHTTPTraceRoundTrip(b *testing.B) {
	for _, marked := range []bool{false, true} {
		b.Run(map[bool]string{false: "unmarked", true: "traced"}[marked], func(b *testing.B) {
			transport := wrapRoundTripper(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if tr := httptrace.ContextClientTrace(req.Context()); tr != nil {
					tr.GetConn("host")
					tr.GotConn(httptrace.GotConnInfo{})
					tr.GotFirstResponseByte()
				}
				return nil, nil
			}))
			ctx := context.Background()
			if marked {
				ctx = withHTTPTrace(ctx)
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://local/key", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = transport.RoundTrip(req)
			}
		})
	}
}

func TestHTTPTraceDefaultAndCustomTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "body") }))
	defer server.Close()
	for _, maxConnections := range []int{0, 2} {
		client := newHTTPClient(ObjectStorageArguments{MaxConnsPerHost: maxConnections})
		var calls atomic.Int64
		ctx := httptrace.WithClientTrace(withHTTPTrace(context.Background()), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { calls.Add(1) }})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		data, readErr := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, readErr)
		require.Equal(t, "body", string(data))
		require.Equal(t, int64(1), calls.Load())
		client.CloseIdleConnections()
	}
}
