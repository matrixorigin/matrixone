// Copyright 2023 Matrix Origin
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

package ctlservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// GetCtlService get ctl cluster from process level runtime
func GetCtlService() CtlService {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.CtlService)
	if !ok {
		panic("no ctl service")
	}
	return v.(CtlService)
}

// CtlService is used to send ctl request to another service or handle request
// from another service.
type CtlService interface {
	// AddHandleFunc add ctl message handler
	AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.Response) error, async bool)
	// SendCtlMessage send ctl message to a service
	SendCtlMessage(ctx context.Context, serviceType metadata.ServiceType, serviceID string, req *pb.Request) (*pb.Response, error)
	// NewRequest new a request by cmd method
	NewRequest(pb.CmdMethod) *pb.Request
	// Release release response
	Release(*pb.Response)
	// Start start ctl service
	Start() error
	// Close close the service
	Close() error
}
