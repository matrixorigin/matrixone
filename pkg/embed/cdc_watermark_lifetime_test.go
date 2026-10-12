// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type watermarkRetirementProbeResult struct {
	ie.InternalExecResult
	err error
}

func (r watermarkRetirementProbeResult) Error() error { return r.err }

type watermarkRetirementProbeExecutor struct {
	ie.InternalExecutor
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	outcome chan string
}

func (p *watermarkRetirementProbeExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (result ie.InternalExecResult) {
	p.once.Do(func() {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	})
	// Observe the real frontend executor without allowing a background panic
	// to prevent fixture cleanup. No production behavior is replaced.
	defer func() {
		outcome := ""
		if value := recover(); value != nil {
			outcome = fmt.Sprintf("%v\n%s", value, debug.Stack())
			result = watermarkRetirementProbeResult{err: fmt.Errorf("executor panic: %v", value)}
		}
		select {
		case p.outcome <- outcome:
		default:
		}
	}()
	return p.InternalExecutor.Query(ctx, sql, opts)
}

func TestCDCWatermarkUpdaterRuntimeRetirement(t *testing.T) {
	c, err := NewCluster(WithTesting(), WithCNCount(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	executor := &watermarkRetirementProbeExecutor{
		InternalExecutor: frontend.NewInternalExecutor(cn.ServiceID()),
		entered:          make(chan struct{}), release: make(chan struct{}), outcome: make(chan string, 4),
	}
	rt := moruntime.ServiceRuntime(cn.ServiceID())
	cdc.CDCWatermarkUpdaterFromRuntime(rt).Stop()
	updater := cdc.InitCDCWatermarkUpdater(rt, executor)
	require.NoError(t, updater.Start(context.Background()))
	var release sync.Once
	unblock := func() { release.Do(func() { close(executor.release) }) }
	defer unblock()
	select {
	case <-executor.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("real watermark background query was not admitted")
	}
	// A background executor query is admitted. Its owner must cancel/join it
	// before deleting the runtime used by frontend session creation.
	require.NoError(t, cn.Close())
	t.Logf("runtime retained after successful CN close: %v", moruntime.ServiceRuntime(cn.ServiceID()) != nil)
	unblock()
	select {
	case panicText := <-executor.outcome:
		require.Empty(t, panicText, "admitted CDC watermark query panicked after successful CN close")
	case <-time.After(10 * time.Second):
		t.Fatal("watermark query did not return after release")
	}
}
