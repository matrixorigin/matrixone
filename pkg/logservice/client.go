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
	"net"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type LogServiceClientConfig struct {
	ReadOnly  bool
	ShardID   uint64
	ReplicaID uint64
	// LogService nodes service addresses
	ServiceAddresses []string
}

type Client interface {
	Close() error
	Append(ctx context.Context, rec pb.LogRecord) (Lsn, error)
	Read(ctx context.Context, firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error)
	Truncate(ctx context.Context, index Lsn) error
	GetTruncatedIndex(ctx context.Context) (Lsn, error)
}

type client struct {
	conn net.Conn
	cfg  LogServiceClientConfig
	buf  []byte
}

var _ Client = (*client)(nil)

func CreateClient(ctx context.Context,
	name string, cfg LogServiceClientConfig) (Client, error) {
	c := &client{
		cfg: cfg,
		buf: make([]byte, reqBufSize),
	}
	var e error
	for _, addr := range cfg.ServiceAddresses {
		conn, err := getConnection(ctx, addr)
		if err != nil {
			e = err
			continue
		}
		c.conn = conn
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
	return sendPoison(c.conn, poisonNumber[:])
}

func (c *client) Append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	// TODO: check piggybacked hint on whether we are connected to the leader node
	return c.append(ctx, rec)
}

func (c *client) Read(ctx context.Context,
	firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	return c.read(ctx, firstIndex, maxSize)
}

func (c *client) Truncate(ctx context.Context, lsn Lsn) error {
	return c.truncate(ctx, lsn)
}

func (c *client) GetTruncatedIndex(ctx context.Context) (Lsn, error) {
	return c.getTruncatedIndex(ctx)
}

func (c *client) connectReadWrite(ctx context.Context) error {
	return c.connect(ctx, pb.MethodType_CONNECT)
}

func (c *client) connectReadOnly(ctx context.Context) error {
	return c.connect(ctx, pb.MethodType_CONNECT_RO)
}

func (c *client) request(ctx context.Context,
	mt pb.MethodType, payload []byte, index Lsn,
	maxSize uint64) (pb.Response, []pb.LogRecord, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return pb.Response{}, nil, err
	}
	req := pb.Request{
		Method:      mt,
		ShardID:     c.cfg.ShardID,
		DNID:        c.cfg.ReplicaID,
		Timeout:     int64(timeout),
		Index:       index,
		MaxSize:     maxSize,
		PayloadSize: uint64(len(payload)),
	}
	if err := writeRequest(c.conn, req, c.buf, payload); err != nil {
		return pb.Response{}, nil, err
	}
	resp, recs, err := readResponse(c.conn, c.buf)
	if err != nil {
		return pb.Response{}, nil, err
	}
	err = toError(resp)
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
	resp, _, err := c.request(ctx, pb.MethodType_APPEND, rec.Data, 0, 0)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}

func (c *client) read(ctx context.Context,
	firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	resp, recs, err := c.request(ctx, pb.MethodType_READ, nil, firstIndex, maxSize)
	if err != nil {
		return nil, 0, err
	}
	return recs, resp.LastIndex, nil
}

func (c *client) truncate(ctx context.Context, lsn Lsn) error {
	_, _, err := c.request(ctx, pb.MethodType_TRUNCATE, nil, lsn, 0)
	return err
}

func (c *client) getTruncatedIndex(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.MethodType_GET_TRUNCATE, nil, 0, 0)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}
