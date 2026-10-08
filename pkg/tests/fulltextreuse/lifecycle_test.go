//go:build fulltext2_base_file_reuse

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltextreuse

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/stretchr/testify/require"
)

// This is a real embedded SQL/CN lifecycle test, not an owner helper test.
// CloseComplete on the actual CN includes the experimental owner's empty-resource
// check. TN/LogService remain alive while the CN closes and restarts.
func TestSQLWarmSearchCNCloseRestart(t *testing.T) {
	c, err := embed.NewCluster(embed.WithTesting(), embed.WithCNCount(1), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
		}
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	waitDiscovery := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cluster, err := clusterservice.GetMOClusterWithContext(ctx, cn.ServiceID())
		require.NoError(t, err)
		refresh, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			require.NoError(t, refresh.Refresh(ctx))
			ready := false
			err := clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster, clusterservice.NewServiceIDSelector(cn.ServiceID()), func(cn metadata.CNService) bool {
				ready = cn.QueryAddress != ""
				return false
			})
			require.NoError(t, err)
			if ready {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("CN query address did not become discoverable")
			case <-tick.C:
			}
		}
	}
	waitDiscovery()
	open := func() *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=60s&writeTimeout=60s", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db := open()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Minute)
	err = testutils.WaitSystemBootstrap(readyCtx, db)
	readyCancel()
	require.NoError(t, err)
	waitISCP := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			if executor, ok := iscp.GetExecutorRuntime(cn.ServiceID()); ok && executor != nil {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("CN ISCP executor did not become ready")
			case <-tick.C:
			}
		}
	}
	waitISCP()
	exec := func(statement string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec("set experimental_fulltext2_index = 1")
	exec("create database ft2_owner_lifecycle")
	exec("use ft2_owner_lifecycle")
	exec("create table docs(id bigint primary key, body text)")
	exec("insert into docs values (1,'alpha beta gamma'),(2,'beta gamma delta'),(3,'gamma delta epsilon'),(4,'alpha zeta')")
	exec("create fulltext2 index ft on docs(body) max_index_capacity 2")
	relation := "docs"
	query := func() map[int64]uint32 {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		rows, err := db.QueryContext(ctx, "select id, match(body) against('alpha' in bm25 mode) from "+relation+" where match(body) against('alpha' in bm25 mode) order by id")
		require.NoError(t, err)
		defer rows.Close()
		result := make(map[int64]uint32)
		for rows.Next() {
			var pk int64
			var score float64
			require.NoError(t, rows.Scan(&pk, &score))
			result[pk] = math.Float32bits(float32(score))
		}
		require.NoError(t, rows.Err())
		require.Len(t, result, 2)
		return result
	}
	resident := func() map[string]*fulltext2.Fulltext2Search {
		found := make(map[string]*fulltext2.Fulltext2Search)
		veccache.Cache.IndexMap.Range(func(key, value any) bool {
			entry, ok := value.(*veccache.VectorIndexSearch)
			if !ok {
				return true
			}
			entry.Mutex.RLock()
			defer entry.Mutex.RUnlock()
			if search, ok := entry.Algo.(*fulltext2.Fulltext2Search); ok && search.CacheServiceID() == cn.ServiceID() && entry.Status.Load() == veccache.STATUS_LOADED {
				found[key.(string)] = search
			}
			return true
		})
		return found
	}
	want := query()
	first := resident()
	require.NotEmpty(t, first, "SQL must leave a real experimental Search resident")
	require.Equal(t, want, query())
	require.Equal(t, first, resident(), "warm query must reuse Search handles")
	// Drive a real CDC Tail, not a no-op MERGE of an unchanged source. Keep
	// alpha TF and document length unchanged so its independent score oracle
	// remains valid after the update is made live.
	require.Len(t, first, 1)
	var storage string
	for key := range first {
		storage = key
	}
	quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
	var metadataTable string
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		err := db.QueryRowContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where name='ft' and algo_table_type='ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='docs')").Scan(&metadataTable)
		require.NoError(t, err)
	}()
	baseIDs := func() []string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		rows, err := db.QueryContext(ctx, "select index_id from "+quote(metadataTable)+" where not prefix_eq(index_id, 'cdc_tail:') order by index_id")
		require.NoError(t, err)
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.NotEmpty(t, ids)
		return ids
	}
	beforeMerge := baseIDs()
	exec("update docs set body='alpha beta changed' where id=1")
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			var n int64
			err := db.QueryRowContext(ctx, "select count(*) from "+quote(storage)+" where tag=1").Scan(&n)
			require.NoError(t, err)
			if n > 0 {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("CDC Tail was not persisted")
			case <-ticker.C:
			}
		}
	}()
	require.Equal(t, first, resident(), "CDC flush must not evict the warm Search")
	// Release only this service's observed entries, not the process-wide cache.
	for key := range first {
		veccache.Cache.Remove(key)
	}
	require.Equal(t, want, query())
	for key, search := range resident() {
		require.NotSame(t, first[key], search)
	}
	// Rebuild and merge must select newly published metadata, not a prior Base.
	exec("alter table docs alter reindex ft fulltext2 merge force_sync")
	require.NotEqual(t, beforeMerge, baseIDs(), "MERGE must publish a new Base build")
	require.Equal(t, want, query())
	exec("alter table docs alter reindex ft fulltext2 force_sync")
	require.Equal(t, want, query())
	exec("create snapshot ft2_owner_snapshot for table ft2_owner_lifecycle docs")
	require.NotEmpty(t, resident())
	require.NoError(t, db.Close())
	old := cn.RawService().(interface {
		Close() error
		CloseComplete() bool
	})
	require.NoError(t, cn.Close())
	require.True(t, old.CloseComplete(), "normal warm residence must not leave a pinned owner")
	require.Empty(t, resident())
	require.NoError(t, cn.Start())
	waitDiscovery()
	waitISCP()
	require.NoError(t, old.Close(), "old service repeated close must not close the new generation")
	db = open()
	exec("set experimental_fulltext2_index = 1")
	exec("use ft2_owner_lifecycle")
	require.Equal(t, want, query())
	require.NotEmpty(t, resident())
	// Same SQL table/index names with different contents are a new incarnation.
	exec("drop table docs")
	exec("create table docs(id bigint primary key, body text)")
	exec("insert into docs values (11,'alpha beta'),(12,'alpha gamma'),(13,'delta')")
	exec("create fulltext2 index ft on docs(body) max_index_capacity 2")
	got := query()
	require.Contains(t, got, int64(11))
	require.Contains(t, got, int64(12))
	require.NotContains(t, got, int64(1))
	relation = "docs{snapshot='ft2_owner_snapshot'}"
	require.Equal(t, want, query(), "historical index must not use the recreated table's Base")
	relation = "docs"
	exec("restore table ft2_owner_lifecycle.docs{snapshot='ft2_owner_snapshot'}")
	got = query()
	require.Equal(t, want, got, "restored table must use its restored index metadata")
	exec("drop snapshot ft2_owner_snapshot")
	// A second real account uses the same logical names but a disjoint corpus.
	// The sessions stay distinct; switching db below only selects the client
	// used by the test helpers, never an execution identity override.
	exec("create account ft2_owner_tenant admin_name 'root' identified by '111'")
	sysDB := db
	tenantDB, err := sql.Open("mysql", fmt.Sprintf("ft2_owner_tenant#root#accountadmin:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=60s&writeTimeout=60s", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = tenantDB.Close() })
	tenantDB.SetMaxOpenConns(1)
	db = tenantDB
	exec("set experimental_fulltext2_index = 1")
	exec("create database ft2_owner_lifecycle")
	exec("use ft2_owner_lifecycle")
	exec("create table docs(id bigint primary key, body text)")
	exec("insert into docs values (21,'alpha beta'),(22,'alpha gamma'),(23,'delta')")
	exec("create fulltext2 index ft on docs(body) max_index_capacity 2")
	tenantResult := query()
	require.Contains(t, tenantResult, int64(21))
	require.Contains(t, tenantResult, int64(22))
	require.NotContains(t, tenantResult, int64(11))
	db = sysDB
	require.Equal(t, got, query())
	require.NoError(t, tenantDB.Close())
	require.NoError(t, db.Close())
	require.NoError(t, cn.Close())
	require.Empty(t, resident())
}
