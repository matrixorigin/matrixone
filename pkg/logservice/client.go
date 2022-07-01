// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	ErrDeadlineNotSet  = moerr.NewError(moerr.INVALID_INPUT, "deadline not set")
	ErrInvalidDeadline = moerr.NewError(moerr.INVALID_INPUT, "invalid deadline")
	// ErrIncompatibleClient is returned when write requests are made on read-only clients.
	ErrIncompatibleClient = moerr.NewError(moerr.INVALID_INPUT, "incompatible client")
)

const (
	connectionTimeout      = 5 * time.Second
	defaultWriteSocketSize = 64 * 1024
)

// IsTempError returns a boolean value indicating whether the specified error is a temp
// error that worth to be retried, e.g. timeouts, temp network issues, operation can be
// completed as Raft leader is being elected. Non-temp error means the error is caused
// by program logics rather than some external factors.
func IsTempError(err error) bool {
	return isTempError(err)
}

type LogServiceClientConfig struct {
	ReadOnly  bool
	ShardID   uint64
	ReplicaID uint64
	// LogService nodes service addresses
	ServiceAddresses []string
}

type Client interface {
	Close() error
	Config() LogServiceClientConfig
	Append(ctx context.Context, rec pb.LogRecord) (Lsn, error)
	Read(ctx context.Context, firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error)
	Truncate(ctx context.Context, index Lsn) error
	GetTruncatedIndex(ctx context.Context) (Lsn, error)
}

type client struct {
	cfg    LogServiceClientConfig
	client morpc.RPCClient
	addr   string
	req    *RPCRequest
	pool   *sync.Pool
}

var _ Client = (*client)(nil)

func CreateClient(ctx context.Context,
	name string, cfg LogServiceClientConfig) (Client, error) {
	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCResponse{pool: pool}
	}
	c := &client{
		cfg:  cfg,
		req:  &RPCRequest{},
		pool: pool,
	}
	var e error
	for _, addr := range cfg.ServiceAddresses {
		cc, err := getRPCClient(ctx, addr, c.pool)
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		if cfg.ReadOnly {
			if err := c.connectReadOnly(ctx); err == nil {
				return c, nil
			} else {
				e = err
			}
		} else {
			if err := c.connectReadWrite(ctx); err == nil {
				return c, nil
			} else {
				e = err
			}
		}
	}
	return nil, e
}

func (c *client) Close() error {
	return c.client.Close()
}

func (c *client) Config() LogServiceClientConfig {
	return c.cfg
}

func (c *client) Append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	if c.readOnly() {
		return 0, ErrIncompatibleClient
	}
	// TODO: check piggybacked hint on whether we are connected to the leader node
	return c.append(ctx, rec)
}

func (c *client) Read(ctx context.Context,
	firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	return c.read(ctx, firstIndex, maxSize)
}

func (c *client) Truncate(ctx context.Context, lsn Lsn) error {
	if c.readOnly() {
		return ErrIncompatibleClient
	}
	return c.truncate(ctx, lsn)
}

func (c *client) GetTruncatedIndex(ctx context.Context) (Lsn, error) {
	return c.getTruncatedIndex(ctx)
}

func (c *client) readOnly() bool {
	return c.cfg.ReadOnly
}

func (c *client) connectReadWrite(ctx context.Context) error {
	if c.readOnly() {
		panic(ErrIncompatibleClient)
	}
	return c.connect(ctx, pb.CONNECT)
}

func (c *client) connectReadOnly(ctx context.Context) error {
	return c.connect(ctx, pb.CONNECT_RO)
}

func (c *client) request(ctx context.Context,
	mt pb.MethodType, payload []byte, index Lsn,
	maxSize uint64) (pb.Response, []pb.LogRecord, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return pb.Response{}, nil, err
	}
	req := pb.Request{
		Method:  mt,
		Timeout: int64(timeout),
		LogRequest: pb.LogRequest{
			ShardID: c.cfg.ShardID,
			DNID:    c.cfg.ReplicaID,
			Index:   index,
			MaxSize: maxSize,
		},
	}
	c.req.Request = req
	c.req.payload = payload
	future, err := c.client.Send(ctx,
		c.addr, c.req, morpc.SendOptions{Timeout: time.Duration(timeout)})
	if err != nil {
		return pb.Response{}, nil, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.Response{}, nil, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	var recs pb.LogRecordResponse
	if len(response.payload) > 0 {
		MustUnmarshal(&recs, response.payload)
	}
	err = toError(response.Response)
	if err != nil {
		return pb.Response{}, nil, err
	}
	return resp, recs.Records, nil
}

func (c *client) connect(ctx context.Context, mt pb.MethodType) error {
	_, _, err := c.request(ctx, mt, nil, 0, 0)
	return err
}

func (c *client) append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.APPEND, rec.Data, 0, 0)
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Index, nil
}

func (c *client) read(ctx context.Context,
	firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	resp, recs, err := c.request(ctx, pb.READ, nil, firstIndex, maxSize)
	if err != nil {
		return nil, 0, err
	}
	return recs, resp.LogResponse.LastIndex, nil
}

func (c *client) truncate(ctx context.Context, lsn Lsn) error {
	_, _, err := c.request(ctx, pb.TRUNCATE, nil, lsn, 0)
	return err
}

func (c *client) getTruncatedIndex(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.GET_TRUNCATE, nil, 0, 0)
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Index, nil
}

func getRPCClient(ctx context.Context, target string, pool *sync.Pool) (morpc.RPCClient, error) {
	mf := func() morpc.Message {
		return pool.Get().(*RPCResponse)
	}
	codec := morpc.NewMessageCodec(mf, defaultWriteSocketSize)
	bf := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendConnectWhenCreate(),
		morpc.WithBackendConnectTimeout(connectionTimeout))
	return morpc.NewClient(bf,
		morpc.WithClientInitBackends([]string{target}, []int{1}),
		morpc.WithClientMaxBackendPerHost(1),
		morpc.WithClientDisableCreateTask())
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, ErrDeadlineNotSet
	}
	now := time.Now()
	if now.After(d) {
		return 0, ErrInvalidDeadline
	}
	return d.Sub(now), nil
}
