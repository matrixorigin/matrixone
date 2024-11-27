// Copyright 2024 Matrix Origin
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
	"io"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"

	"github.com/google/pprof/profile"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

var connectionsProfiler = malloc.NewProfiler[connectionsProfileSample]()

func init() {
	http.HandleFunc("/debug/http_conns", func(w http.ResponseWriter, req *http.Request) {
		connectionsProfiler.Write(w)
	})
}

func WriteHTTPConnsProfile(w io.Writer) error {
	return connectionsProfiler.Write(w)
}

type connectionsProfileSample struct {
	All   malloc.ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	Alive malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
}

var _ malloc.SampleValues[connectionsProfileSample] = new(connectionsProfileSample)

func (c *connectionsProfileSample) DefaultSampleType() string {
	return "all"
}

func (c *connectionsProfileSample) Init() {
	c.All = *malloc.NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0))
	c.Alive = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
}

func (c *connectionsProfileSample) SampleTypes() []*profile.ValueType {
	return []*profile.ValueType{
		{
			Type: "all",
			Unit: "conn",
		},
		{
			Type: "alive",
			Unit: "conn",
		},
	}
}

func (c *connectionsProfileSample) Values() []int64 {
	return []int64{
		int64(c.All.Load()),
		c.Alive.Load(),
	}
}

type dialContextFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

func wrapDialContext(upstream dialContextFunc) dialContextFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := upstream(ctx, network, addr)
		if err != nil {
			return conn, err
		}
		v := ctx.Value(CtxKeyConnsProfileSample)
		if v == nil {
			return conn, err
		}
		sample := v.(*connectionsProfileSample)
		sample.All.Add(1)
		sample.Alive.Add(1)
		return &profilerConn{
			Conn:   conn,
			sample: sample,
		}, nil
	}
}

type profilerConn struct {
	net.Conn
	sample *connectionsProfileSample
}

func (p *profilerConn) Close() error {
	p.sample.Alive.Add(-1)
	return p.Conn.Close()
}

func wrapRoundTripper[T http.RoundTripper](upstream T) http.RoundTripper {
	return &profilerRoundTripper[T]{
		upstream: upstream,
	}
}

type profilerRoundTripper[T http.RoundTripper] struct {
	upstream T
}

type ctxKeyConnsProfileSample struct{}

var CtxKeyConnsProfileSample ctxKeyConnsProfileSample

func (p *profilerRoundTripper[T]) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.WithContext(
		context.WithValue(
			req.Context(),
			CtxKeyConnsProfileSample,
			connectionsProfiler.Sample(1, 1),
		),
	)
	return p.upstream.RoundTrip(req)
}
