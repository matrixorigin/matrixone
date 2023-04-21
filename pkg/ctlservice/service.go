// Copyright 2022 Matrix Origin
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

package ctlservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type service struct {
	serviceID string
	cluster   clusterservice.MOCluster
	client    morpc.RPCClient
	h         morpc.MessageHandler[*pb.Request, *pb.Response]
	pool      morpc.MessagePool[*pb.Request, *pb.Response]
}

// NewCtlService new ctl service to send ctl message to another service or handle ctl request from
// other ctl service.
func NewCtlService(
	serviceID string,
	address string,
	cfg morpc.Config) (CtlService, error) {
	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.Response { return &pb.Response{} })

	client, err := cfg.NewClient("ctlservice",
		getLogger().RawLogger(),
		func() morpc.Message { return pool.AcquireResponse() })
	if err != nil {
		return nil, err
	}

	h, err := morpc.NewMessageHandler(
		"ctlservice",
		address,
		cfg,
		pool)
	if err != nil {
		return nil, err
	}
	return &service{
		serviceID: serviceID,
		client:    client,
		h:         h,
		cluster:   clusterservice.GetMOCluster(),
		pool:      pool,
	}, nil
}

func (s *service) AddHandleFunc(
	method pb.CmdMethod,
	h func(context.Context, *pb.Request, *pb.Response) error,
	async bool) {
	s.h.RegisterHandleFunc(uint32(method), h, async)
}

func (s *service) SendCtlMessage(
	ctx context.Context,
	serviceType metadata.ServiceType,
	serviceID string,
	req *pb.Request) (*pb.Response, error) {
	if s.isSelf(serviceID) {
		return s.unwrapResponseError(s.h.Handle(ctx, req))
	}

	addr, err := s.resolveService(serviceType, serviceID)
	if err != nil {
		return nil, err
	}

	f, err := s.client.Send(ctx, addr, req)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	v, err := f.Get()
	if err != nil {
		return nil, err
	}
	resp := v.(*pb.Response)
	return s.unwrapResponseError(resp)
}

func (s *service) NewRequest(method pb.CmdMethod) *pb.Request {
	req := s.pool.AcquireRequest()
	req.CMDMethod = method
	return req
}

func (s *service) Release(resp *pb.Response) {
	s.pool.ReleaseResponse(resp)
}

func (s *service) Start() error {
	return s.h.Start()
}

func (s *service) Close() error {
	if err := s.client.Close(); err != nil {
		return err
	}
	return s.h.Close()
}

func (s *service) isSelf(serviceID string) bool {
	return s.serviceID == serviceID
}

func (s *service) resolveService(
	serviceType metadata.ServiceType,
	serviceID string) (string, error) {
	address := ""
	switch serviceType {
	case metadata.ServiceType_CN:
		s.cluster.GetCNService(
			clusterservice.NewServiceIDSelector(serviceID),
			func(s metadata.CNService) bool {
				address = s.CtlAddress
				return false
			})
	case metadata.ServiceType_DN:
		s.cluster.GetDNService(
			clusterservice.NewServiceIDSelector(serviceID),
			func(s metadata.DNService) bool {
				address = s.CtlAddress
				return false
			})
	default:
		return "", moerr.NewNotSupportedNoCtx("not support ctl request to %s",
			serviceType.String())
	}
	if address == "" {
		return "", moerr.NewInvalidInputNoCtx("service %s:%s not found",
			serviceType.String(),
			serviceID)
	}
	return address, nil
}

func (s *service) unwrapResponseError(resp *pb.Response) (*pb.Response, error) {
	if err := resp.UnwrapError(); err != nil {
		s.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}
