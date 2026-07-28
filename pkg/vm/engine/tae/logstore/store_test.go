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

package logstore

import (
	"context"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"

	"github.com/stretchr/testify/assert"
)

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	return service, &ccfg
}

func TestNewClientFailed(t *testing.T) {

	service, ccfg := initTest(t)
	defer service.Close()

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectWALReplayFailed("new client")
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	idx := 0
	mockFactory := func() (logservice.Client, error) {

		if idx > 10 {
			rmFn()
		}
		idx++
		var client logservice.Client
		var err error
		if msg, injected := objectio.WALReplayFailedExecutorInjected(); injected && msg == "new client" {
			err = moerr.NewInternalErrorNoCtx("mock error")
		} else {

			client, err = logservice.NewClient(ctx, "", *ccfg)
		}
		return client, err
	}
	logservicedriver.NewClient(mockFactory, 100, 100, time.Millisecond*100, time.Minute*5)
}
