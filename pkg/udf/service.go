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
)

func NewService(services ...Service) (Service, error) {
	srv := &service{}
	srv.mapping = make(map[string]Service, len(services))
	for _, s := range services {
		if srv.mapping[s.Language()] == nil {
			srv.mapping[s.Language()] = s
		} else {
			return nil, moerr.NewInternalErrorNoCtx("too many " + s.Language() + " udf service")
		}
	}
	return srv, nil
}

type service struct {
	mapping map[string]Service
}

func (s *service) Language() string {
	return "multiple"
}

func (s *service) Run(ctx context.Context, request *Request) (*Response, error) {
	srv := s.mapping[request.Language]
	if srv == nil {
		return nil, moerr.NewInternalError(ctx, "missing "+request.Language+" udf service")
	}
	return srv.Run(ctx, request)
}
