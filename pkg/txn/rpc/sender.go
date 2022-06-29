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

package rpc

import (
	"context"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

type sender struct {
	logger *zap.Logger
	client morpc.RPCClient
}

// NewSender create a txn sender
func NewSender(logger *zap.Logger) (TxnSender, error) {
	logger = logutil.Adjust(logger)
	codec := morpc.NewMessageCodec(func() morpc.Message { return &txn.TxnResponse{} }, 0)
	bf := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendConnectWhenCreate(),
		morpc.WithBackendLogger(logger))
	client, err := morpc.NewClient(bf, morpc.WithClientLogger(logger))
	if err != nil {
		return nil, err
	}
	return &sender{logger: logger, client: client}, nil
}

func (s *sender) Close() error {
	return s.client.Close()
}

func (s *sender) Send(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	if len(requests) == 1 {
		requests[0].RequestID = s.getUUID()
		resp, err := s.doSend(ctx, requests[0])
		if err != nil {
			return nil, err
		}
		return []txn.TxnResponse{resp}, nil
	}

	responses := make([]txn.TxnResponse, len(requests))
	executors := make(map[string]*executor, len(requests))
	defer func() {
		for dn, st := range executors {
			if err := st.close(); err != nil {
				s.logger.Error("close stream failed",
					zap.String("dn", dn),
					zap.Error(err))
			}
		}
	}()

	for idx, req := range requests {
		dn := req.GetTargetDN()
		exec, ok := executors[dn.Address]
		if !ok {
			st, err := s.client.NewStream(dn.Address, len(requests))
			if err != nil {
				return nil, err
			}
			exec, err = newExecutor(ctx, responses, st)
			if err != nil {
				return nil, err
			}
			executors[dn.Address] = exec
		}

		req.RequestID = exec.stream.ID()
		if err := exec.execute(req, idx); err != nil {
			return nil, err
		}
	}

	for _, se := range executors {
		if err := se.waitCompleted(); err != nil {
			return nil, err
		}
	}
	return responses, nil
}

func (s *sender) doSend(ctx context.Context, request txn.TxnRequest) (txn.TxnResponse, error) {
	f, err := s.client.Send(ctx, request.GetTargetDN().Address, &request, morpc.SendOptions{})
	if err != nil {
		return txn.TxnResponse{}, err
	}
	defer f.Close()

	v, err := f.Get()
	if err != nil {
		return txn.TxnResponse{}, err
	}
	return *(v.(*txn.TxnResponse)), nil
}

func (s *sender) getUUID() []byte {
	id := uuid.New()
	return id[:]
}

type executor struct {
	ctx       context.Context
	stream    morpc.Stream
	responses []txn.TxnResponse
	indexes   []int
	c         chan morpc.Message
}

func newExecutor(ctx context.Context, responses []txn.TxnResponse, stream morpc.Stream) (*executor, error) {
	c, err := stream.Receive()
	if err != nil {
		return nil, err
	}
	return &executor{
		ctx:       ctx,
		stream:    stream,
		responses: responses,
		c:         c,
	}, nil
}

func (se *executor) execute(req txn.TxnRequest, index int) error {
	se.indexes = append(se.indexes, index)
	req.RequestID = se.stream.ID()
	return se.stream.Send(&req, morpc.SendOptions{})
}

func (se *executor) close() error {
	return se.stream.Close()
}

func (se *executor) waitCompleted() error {
	for _, idx := range se.indexes {
		select {
		case <-se.ctx.Done():
			return se.ctx.Err()
		case v, ok := <-se.c:
			if !ok {
				return moerr.NewError(moerr.ErrStreamClosed, "stream closed")
			}
			se.responses[idx] = *(v.(*txn.TxnResponse))
		}
	}
	return nil
}
