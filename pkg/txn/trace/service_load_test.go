// Copyright 2024 Matrix Origin
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

package trace

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_writeToMO(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "insert into t1 values") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.writeToMO(loadAction{
		sql: "insert into t1 values (1)",
	})
	assert.Error(t, err)
}

func Test_writeToS3(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "insert into t1 values") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	tDir := os.TempDir()
	dir := path.Join(tDir, "/local")
	assert.NoError(t, os.RemoveAll(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	c := fileservice.Config{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer fs.Close()

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
		logger:   logger,
	}
	serv.options.fs = fs

	tPath := tDir + "/" + "test"
	assert.NoError(t, os.RemoveAll(tPath))
	defer func() {
		_ = os.RemoveAll(tPath)
	}()

	err = os.WriteFile(tPath, []byte("abc"), 0755)
	assert.NoError(t, err)

	err = serv.writeToS3(loadAction{
		file: tPath,
	})
	assert.NoError(t, err)
}
