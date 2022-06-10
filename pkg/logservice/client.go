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
			}
		} else {
			if err := c.connectReadWrite(ctx); err == nil {
				return c, nil
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

func (c *client) getRequest(ctx context.Context, mt pb.MethodType) (pb.Request, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return pb.Request{}, err
	}
	return pb.Request{
		Method:  mt,
		ShardID: c.cfg.ShardID,
		DNID:    c.cfg.ReplicaID,
		Timeout: int64(timeout),
	}, nil
}

func (c *client) connect(ctx context.Context, mt pb.MethodType) error {
	req, err := c.getRequest(ctx, mt)
	if err != nil {
		return err
	}
	if err := writeRequest(c.conn, req, c.buf, nil); err != nil {
		return err
	}
	resp, _, err := readResponse(c.conn, c.buf)
	if err != nil {
		return err
	}
	return toError(resp)
}

func (c *client) append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	req, err := c.getRequest(ctx, pb.MethodType_APPEND)
	if err != nil {
		return 0, err
	}
	if err := writeRequest(c.conn, req, c.buf, rec.Data); err != nil {
		return 0, err
	}
	resp, _, err := readResponse(c.conn, c.buf)
	if err != nil {
		return 0, err
	}
	err = toError(resp)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}

func (c *client) read(ctx context.Context,
	firstIndex Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	req, err := c.getRequest(ctx, pb.MethodType_READ)
	if err != nil {
		return nil, 0, err
	}
	req.Index = firstIndex
	req.MaxSize = maxSize
	if err := writeRequest(c.conn, req, c.buf, nil); err != nil {
		return nil, 0, err
	}
	resp, records, err := readResponse(c.conn, c.buf)
	if err != nil {
		return nil, 0, err
	}
	err = toError(resp)
	if err != nil {
		return nil, 0, err
	}
	return records.Records, resp.LastIndex, nil
}

func (c *client) truncate(ctx context.Context, lsn Lsn) error {
	req, err := c.getRequest(ctx, pb.MethodType_TRUNCATE)
	if err != nil {
		return err
	}
	req.Index = lsn
	if err := writeRequest(c.conn, req, c.buf, nil); err != nil {
		return err
	}
	resp, _, err := readResponse(c.conn, c.buf)
	if err != nil {
		return err
	}
	return toError(resp)
}

func (c *client) getTruncatedIndex(ctx context.Context) (Lsn, error) {
	req, err := c.getRequest(ctx, pb.MethodType_APPEND)
	if err != nil {
		return 0, err
	}
	if err := writeRequest(c.conn, req, c.buf, nil); err != nil {
		return 0, err
	}
	resp, _, err := readResponse(c.conn, c.buf)
	if err != nil {
		return 0, err
	}
	err = toError(resp)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}
