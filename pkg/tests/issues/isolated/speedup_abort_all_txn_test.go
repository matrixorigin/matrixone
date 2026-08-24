// Copyright 2026 Matrix Origin
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

package isolated

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestSpeedupAbortAllTxn(t *testing.T) {
	c, err := embed.StartTestCluster(
		embed.WithPreStart(
			func(so embed.ServiceOperator) {
				if so.ServiceType() == metadata.ServiceType_CN {
					so.Adjust(
						func(sc *embed.ServiceConfig) {
							sc.CN.Txn.MaxActive = 1
						},
					)
				}
			},
		),
	)
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)

	op, err := c.GetCNService(0)
	require.NoError(t, err)

	waitC := make(chan struct{}, 1)
	cn := op.RawService().(cnservice.Service)
	eng := cn.GetEngine().(*disttae.Engine)
	logtailClient := eng.PushClient()
	logtailClient.SetReconnectHandler(func() {
		select {
		case waitC <- struct{}{}:
		default:
		}
	})

	c1 := make(chan struct{})
	c2 := make(chan struct{})
	actionC := make(chan struct{})
	errC := make(chan error, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)

	taskservice.DebugCtlTaskFramework(true)
	defer taskservice.DebugCtlTaskFramework(false)

	go func() {
		defer wg.Done()

		exec := cn.GetSQLExecutor()
		err := exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				res, err := txn.Exec(
					"create database TestSpeedupAbortAllTxn",
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()
				close(c1)

				select {
				case <-c2:
				case <-ctx.Done():
					return ctx.Err()
				}
				close(actionC)

				select {
				case <-waitC:
				case <-ctx.Done():
					return ctx.Err()
				}

				for !eng.PushClient().IsSubscriberReady() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(10 * time.Millisecond):
					}
				}

				return nil
			},
			executor.Options{}.WithDatabase("mo_catalog").WithUserTxn(),
		)
		errC <- err
	}()

	go func() {
		defer wg.Done()

		select {
		case <-c1:
		case <-ctx.Done():
			errC <- ctx.Err()
			return
		}

		tc := cn.GetTxnClient()
		var notifyActive sync.Once
		_, err := tc.New(
			ctx,
			timestamp.Timestamp{},
			client.WithUserTxn(),
			client.WithWaitActiveHandle(
				func() {
					notifyActive.Do(func() {
						close(c2)
					})
				},
			),
		)
		errC <- err
	}()

	select {
	case <-actionC:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.NoError(t, logtailClient.Disconnect())

	wg.Wait()
	close(errC)
	for err := range errC {
		require.NoError(t, err)
	}
	require.NoError(t, checkLogtailResumed(ctx, cn))
}

func checkLogtailResumed(ctx context.Context, cn cnservice.Service) error {
	exec := cn.GetSQLExecutor()
	execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := exec.Exec(
		execCtx,
		"select * from mo_tables",
		executor.Options{}.WithDatabase("mo_catalog"),
	)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}
