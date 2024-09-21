// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util"
)

func handleCoreDumpConfig(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
	if req.CoreDumpConfig == nil {
		return nil
	}
	switch strings.ToLower(req.CoreDumpConfig.Action) {
	case "enable":
		util.EnableCoreDump()
	case "disable":
		util.DisableCoreDump()
	}
	resp.CoreDumpConfig = &pb.CoreDumpConfigResponse{}
	return nil
}
