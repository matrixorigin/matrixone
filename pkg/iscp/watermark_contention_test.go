// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type watermarkSQLProbe struct {
	executor.SQLExecutor
	held, resume chan struct{}
	once         sync.Once
}

func (p *watermarkSQLProbe) Exec(ctx context.Context, statement string, opts executor.Options) (executor.Result, error) {
	if opts.Txn() == nil || opts.Txn().TxnOptions().SessionID != "flush watermark for all tables" {
		return p.SQLExecutor.Exec(ctx, statement, opts)
	}
	// A private copy of the catalog row layout keeps the live daemon from
	// consuming these synthetic scheduler jobs. SQL compilation and row locking
	// still use the real service executor and the production statement options.
	statement = strings.ReplaceAll(statement, "mo_catalog.mo_iscp_log", "watermark_contention.jobs")
	result, err := p.SQLExecutor.Exec(ctx, statement, opts)
	if err == nil && result.AffectedRows == 1 && strings.Contains(statement, "AND table_id = 3 ") {
		p.once.Do(func() {
			close(p.held)
			select {
			case <-p.resume:
			case <-ctx.Done():
			}
		})
	}
	return result, err
}

// Publisher owns B, retirement owns A, then each requests the other row. The
// background owner must abort its entire batch instead of completing the cycle.
func TestWatermarkBatchYieldsToRetirement(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), 0), 30*time.Second)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	peer, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	for _, statement := range []string{
		"create database watermark_contention",
		"create table watermark_contention.jobs(account_id int unsigned,table_id bigint unsigned,job_name varchar(100),job_id bigint unsigned,job_state int,watermark varchar(100),job_status json,drop_at timestamp null,primary key(account_id,table_id,job_name,job_id))",
		`insert into watermark_contention.jobs values(1,3,'first',1,3,'1-0','{"LSN":5,"Stage":1}',null),(1,4,'second',1,3,'1-0','{"LSN":5,"Stage":1}',null)`,
	} {
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	service := cn.RawService().(cnservice.Service)
	peerService := peer.RawService().(cnservice.Service)
	// Cross-CN sessions do not share a commit frontier. Fence each handoff
	// before creating the reader's transaction, using this test's own budget.
	syncCommit := func(reader, writer cnservice.Service) {
		t.Helper()
		commitTS := writer.GetTxnClient().GetLatestCommitTS()
		require.False(t, commitTS.IsEmpty())
		snapshot, err := reader.GetTxnClient().WaitLogTailAppliedAt(ctx, commitTS)
		require.NoError(t, err)
		require.True(t, snapshot.Greater(commitTS))
	}
	syncCommit(service, peerService)
	rt := moruntime.ServiceRuntime(cn.ServiceID())
	original, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	probe := &watermarkSQLProbe{SQLExecutor: original.(executor.SQLExecutor), held: make(chan struct{}), resume: make(chan struct{})}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, probe)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.InternalSQLExecutor, original) })
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(probe.resume) }) }
	defer resume()
	flush, persisted := iscp.NewWatermarkBatchForTest(ctx, cn.ServiceID(), service.GetEngine(), service.GetTxnClient())
	flushed := make(chan struct{})
	var flushErr error
	go func() { flushErr = flush(); close(flushed) }()
	defer func() { cancel(); resume(); <-flushed }()
	select {
	case <-probe.held:
	case <-flushed:
		t.Fatalf("publisher exited before acquiring its first row: %v", flushErr)
	case <-ctx.Done():
		t.Fatal("publisher did not acquire its first row", ctx.Err())
	}
	retirement, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer retirement.Rollback()
	result, err := retirement.ExecContext(ctx, "update watermark_contention.jobs set drop_at=now() where account_id=1 and table_id=4 and job_name='second' and job_id=1")
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	resume()
	result, err = retirement.ExecContext(ctx, "update watermark_contention.jobs set drop_at=now() where account_id=1 and table_id=3 and job_name='first' and job_id=1")
	require.NoError(t, err, "retirement must acquire the row released by the publisher")
	affected, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.NoError(t, retirement.Commit())
	<-flushed
	require.ErrorContains(t, flushErr, "wait policy is fast fail")
	require.Equal(t, []types.TS{types.BuildTS(1, 0), types.BuildTS(1, 0)}, persisted())
	var rows int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from watermark_contention.jobs where watermark='1-0' and cast(json_extract(job_status,'$.LSN') as unsigned)=5 and drop_at is not null").Scan(&rows))
	require.Equal(t, 2, rows, "no prefix of the failed batch may become durable")
	// A subsequent normal flush commits both rows through the same owner.
	syncCommit(service, peerService)
	require.NoError(t, flush())
	syncCommit(peerService, service)
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from watermark_contention.jobs where watermark='2-0' and cast(json_extract(job_status,'$.LSN') as unsigned)=6").Scan(&rows))
	require.Equal(t, 2, rows)
	_, err = db.ExecContext(ctx, "drop database watermark_contention")
	require.NoError(t, err)
}
