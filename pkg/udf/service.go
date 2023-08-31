// Copyright 2023 Matrix Origin
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

package udf

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
)

// Service handle non-sql udf in cn
type Service interface {
	AddPythonUdfClient(pc pythonservice.PythonUdfClient)
	RunPythonUdf(ctx context.Context, request *pythonservice.PythonUdfRequest) (*pythonservice.PythonUdfResponse, error)
}

func NewService() Service {
	return &service{}
}

type service struct {
	pc pythonservice.PythonUdfClient
}

func (s *service) AddPythonUdfClient(pc pythonservice.PythonUdfClient) {
	s.pc = pc
}

func (s *service) RunPythonUdf(ctx context.Context, request *pythonservice.PythonUdfRequest) (*pythonservice.PythonUdfResponse, error) {
	if s.pc == nil {
		return nil, moerr.NewInternalError(ctx, "missing python udf client")
	}
	return s.pc.Run(ctx, request)
}
