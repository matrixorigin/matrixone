// Copyright 2021 Matrix Origin
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

package objectio

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TmpNewFileservice(ctx context.Context, dir string) fileservice.FileService {
	return tmpNewFileservice(ctx, defines.LocalFileServiceName, dir)
}

func TmpNewSharedFileservice(ctx context.Context, dir string) fileservice.FileService {
	return tmpNewFileservice(ctx, defines.SharedFileServiceName, dir)
}

func tmpNewFileservice(ctx context.Context, kind string, dir string) fileservice.FileService {
	c := fileservice.Config{
		Name:    kind,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	if err != nil {
		err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("NewFileService failed: %s", err.Error()))
		panic(any(err))
	}
	return service
}
