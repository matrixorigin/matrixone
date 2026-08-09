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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

const (
	issue26465BenchmarkDepartmentIndexes = 768
	issue26465BenchmarkUserIndexes       = 732
	issue26465BenchmarkUserFKeys         = 349
	issue26465BenchmarkCandidateFKeys    = 702
)

// BenchmarkIssue26465AlterCopyQAShape runs the same logical no-op COPY ALTER
// that triggered the QA allocation profile, against the decoded QA cardinality.
// Setup is intentionally excluded from timing. The fixture does not claim to
// reproduce QA's unavailable full CREATE SQL. New DDL is canonicalized by
// #26330, so it cannot recreate QA's 348 legacy RefChildTbl IDs; the legacy
// serialization fan-out is covered by the engine microbenchmark.
//
// This is a manual benchmark. Start with one measured ALTER per scenario:
// go test -run '^$' -bench '^BenchmarkIssue26465AlterCopyQAShape$' -benchmem -benchtime=1x ./pkg/embed
func BenchmarkIssue26465AlterCopyQAShape(b *testing.B) {
	restoreMemProfileRate := issue26465DisableSetupAllocationProfiling()
	defer restoreMemProfileRate()

	RunBaseClusterTests(b, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		if err != nil {
			b.Fatal(err)
		}
		dsn := fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/",
			cn.GetServiceConfig().CN.Frontend.Port,
		)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		benchmarkIssue26465DepartmentsCopy(b, db)
		benchmarkIssue26465UsersCopy(b, db)
		benchmarkIssue26465CandidatesCopy(b, db)
	})
}

func TestIssue26465AlterCopyPreservesParentReference(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		if err != nil {
			t.Fatal(err)
		}
		dsn := fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/",
			cn.GetServiceConfig().CN.Frontend.Port,
		)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		ctx := context.Background()
		const database = "issue26465_parent_reference"
		if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+database); err != nil {
			t.Fatal(err)
		}
		defer db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+database)
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		for _, statement := range []string{
			"USE " + database,
			`CREATE TABLE parent (id INT PRIMARY KEY)`,
			`CREATE TABLE child (
				id INT PRIMARY KEY,
				parent_id INT,
				CONSTRAINT child_parent_fk_one FOREIGN KEY (parent_id) REFERENCES parent(id),
				CONSTRAINT child_parent_fk_two FOREIGN KEY (parent_id) REFERENCES parent(id)
			)`,
			`ALTER TABLE child ADD COLUMN payload INT NULL`,
		} {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}

		if _, err := conn.ExecContext(ctx, `DROP TABLE parent`); err == nil {
			t.Fatal("parent table was dropped after ALTER COPY despite child foreign keys")
		}
	})
}

func benchmarkIssue26465DepartmentsCopy(b *testing.B, db *sql.DB) {
	b.Run("departments_parent_copy_current_canonical_ref_child", func(b *testing.B) {
		ctx, conn, cleanup := setupIssue26465DepartmentAndUsers(b, db, "issue26465_departments")
		defer func() {
			b.StopTimer()
			cleanup()
		}()

		b.ResetTimer()
		stopAllocationProfile := issue26465StartMeasuredAllocationProfiling()
		b.StartTimer()
		for range b.N {
			issue26465Exec(b, ctx, conn, `ALTER TABLE departments CHANGE name name VARCHAR(100) NOT NULL COMMENT '部门名称'`)
		}
		b.StopTimer()
		stopAllocationProfile()
		b.ReportMetric(issue26465BenchmarkDepartmentIndexes, "department-indexes/op")
		b.ReportMetric(issue26465BenchmarkUserIndexes, "user-indexes/op")
		b.ReportMetric(issue26465BenchmarkUserFKeys, "user-fks/op")
	})
}

func benchmarkIssue26465UsersCopy(b *testing.B, db *sql.DB) {
	b.Run("users_child_copy_349_fks_to_one_parent", func(b *testing.B) {
		ctx, conn, cleanup := setupIssue26465DepartmentAndUsers(b, db, "issue26465_users")
		defer func() {
			b.StopTimer()
			cleanup()
		}()

		b.ResetTimer()
		stopAllocationProfile := issue26465StartMeasuredAllocationProfiling()
		b.StartTimer()
		for range b.N {
			issue26465Exec(b, ctx, conn, `ALTER TABLE users CHANGE department_id department_id INT NULL COMMENT '所属部门ID（关联 departments 表）'`)
		}
		b.StopTimer()
		stopAllocationProfile()
		b.ReportMetric(issue26465BenchmarkDepartmentIndexes, "department-indexes/op")
		b.ReportMetric(issue26465BenchmarkUserIndexes, "user-indexes/op")
		b.ReportMetric(issue26465BenchmarkUserFKeys, "user-fks/op")
	})
}

func benchmarkIssue26465CandidatesCopy(b *testing.B, db *sql.DB) {
	b.Run("candidates_child_copy_702_fks_to_three_parents", func(b *testing.B) {
		ctx, conn, cleanup := setupIssue26465Candidates(b, db, "issue26465_candidates")
		defer func() {
			b.StopTimer()
			cleanup()
		}()

		b.ResetTimer()
		stopAllocationProfile := issue26465StartMeasuredAllocationProfiling()
		b.StartTimer()
		for range b.N {
			issue26465Exec(b, ctx, conn, `ALTER TABLE candidates CHANGE payload payload INT NULL`)
		}
		b.StopTimer()
		stopAllocationProfile()
		b.ReportMetric(issue26465BenchmarkCandidateFKeys, "candidate-fks/op")
		b.ReportMetric(3, "unique-parents/op")
	})
}

func issue26465DisableSetupAllocationProfiling() func() {
	if os.Getenv("MO_ISSUE_26465_PROFILE_ALLOCATIONS") != "1" {
		return func() {}
	}
	previousRate := runtime.MemProfileRate
	runtime.MemProfileRate = 0
	return func() {
		runtime.MemProfileRate = previousRate
	}
}

func issue26465StartMeasuredAllocationProfiling() func() {
	if os.Getenv("MO_ISSUE_26465_PROFILE_ALLOCATIONS") != "1" {
		return func() {}
	}
	// This rate keeps the profile representative without distorting the DDL
	// benchmark by recording every small allocation.
	runtime.MemProfileRate = 64 * 1024
	return func() {
		runtime.MemProfileRate = 0
	}
}

func setupIssue26465DepartmentAndUsers(
	b *testing.B,
	db *sql.DB,
	database string,
) (context.Context, *sql.Conn, func()) {
	ctx, conn := issue26465SetupDatabase(b, db, database)
	issue26465Exec(b, ctx, conn, issue26465DepartmentsDDL())
	issue26465Exec(b, ctx, conn, issue26465UsersDDL())
	return ctx, conn, func() {
		conn.Close()
		issue26465DropDatabase(b, db, database)
	}
}

func setupIssue26465Candidates(
	b *testing.B,
	db *sql.DB,
	database string,
) (context.Context, *sql.Conn, func()) {
	ctx, conn := issue26465SetupDatabase(b, db, database)
	issue26465Exec(b, ctx, conn, `CREATE TABLE jobs (id INT PRIMARY KEY)`)
	issue26465Exec(b, ctx, conn, `CREATE TABLE talent_pools (id INT PRIMARY KEY)`)
	issue26465Exec(b, ctx, conn, `CREATE TABLE users (id INT PRIMARY KEY)`)
	issue26465Exec(b, ctx, conn, issue26465CandidatesDDL())
	return ctx, conn, func() {
		conn.Close()
		issue26465DropDatabase(b, db, database)
	}
}

func issue26465SetupDatabase(b *testing.B, db *sql.DB, database string) (context.Context, *sql.Conn) {
	b.StopTimer()
	issue26465DropDatabase(b, db, database)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	issue26465Exec(b, ctx, conn, "CREATE DATABASE "+database)
	issue26465Exec(b, ctx, conn, "USE "+database)
	return ctx, conn
}

func issue26465Exec(b *testing.B, ctx context.Context, conn *sql.Conn, statement string) {
	b.Helper()
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		b.Fatalf("%s: %v", statement, err)
	}
}

func issue26465DropDatabase(b *testing.B, db *sql.DB, database string) {
	b.Helper()
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + database); err != nil {
		b.Fatal(err)
	}
}

func issue26465DepartmentsDDL() string {
	defs := []string{
		"id INT NOT NULL AUTO_INCREMENT",
		"name VARCHAR(100) NOT NULL COMMENT '部门名称'",
		"marker INT NULL",
		"PRIMARY KEY (id)",
	}
	for i := range issue26465BenchmarkDepartmentIndexes {
		defs = append(defs, fmt.Sprintf("KEY departments_idx_%04d (marker)", i))
	}
	return "CREATE TABLE departments (" + strings.Join(defs, ",") + ")"
}

func issue26465UsersDDL() string {
	defs := []string{
		"id INT NOT NULL AUTO_INCREMENT",
		"department_id INT NULL COMMENT '所属部门ID（关联 departments 表）'",
		"marker INT NULL",
		"PRIMARY KEY (id)",
	}
	for i := range issue26465BenchmarkUserIndexes {
		defs = append(defs, fmt.Sprintf("KEY users_idx_%04d (marker)", i))
	}
	for i := range issue26465BenchmarkUserFKeys {
		defs = append(defs, fmt.Sprintf(
			"CONSTRAINT users_department_fk_%04d FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL ON UPDATE CASCADE",
			i,
		))
	}
	return "CREATE TABLE users (" + strings.Join(defs, ",") + ")"
}

func issue26465CandidatesDDL() string {
	defs := []string{
		"id INT NOT NULL AUTO_INCREMENT",
		"job_id INT NULL",
		"talent_pool_id INT NULL",
		"created_by INT NULL",
		"payload INT NULL",
		"PRIMARY KEY (id)",
		"KEY candidates_payload_idx_0 (payload)",
		"KEY candidates_payload_idx_1 (payload)",
		"KEY candidates_payload_idx_2 (payload)",
	}
	for i := range issue26465BenchmarkCandidateFKeys / 3 {
		defs = append(defs,
			fmt.Sprintf("CONSTRAINT candidates_jobs_fk_%04d FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE SET NULL ON UPDATE CASCADE", i),
			fmt.Sprintf("CONSTRAINT candidates_talent_pools_fk_%04d FOREIGN KEY (talent_pool_id) REFERENCES talent_pools(id) ON DELETE SET NULL ON UPDATE CASCADE", i),
			fmt.Sprintf("CONSTRAINT candidates_users_fk_%04d FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE", i),
		)
	}
	return "CREATE TABLE candidates (" + strings.Join(defs, ",") + ")"
}
