// reproduce.go — 复现 data branch create table + DROP DATABASE 竞态条件
//
// 根因：事务 commit 时 defer LIFO 顺序导致 unlock 在 updateLastCommitTS 之前执行。
//   1. data branch txn commit → doWrite
//   2. defer tc.unlock(ctx)  — 释放 Shared lock
//   3. defer { closeLocked(); mu.Unlock() } — 触发 updateLastCommitTS
//   由于 defer LIFO，unlock 先执行，closeLocked 后执行。
//   在 unlock 和 closeLocked 之间的窗口中：
//     - Shared lock 已释放，DROP 可以获取 Exclusive lock
//     - latestCommitTS 还没更新，v1 fix 的 GetLatestCommitTS() 看到旧值
//     - v1 fix 不触发，Relations() 用旧 snapshot 查询，漏掉新创建的表
//
// 源表索引结构（匹配线上 gate_019ca107 场景）：
//   类型A: infra_configs 风格 — 1 UNIQUE + 3 SECONDARY = 1主表+4索引表 = 5 relations
//   类型B: agent_events 风格 — 9 SECONDARY + 1 FULLTEXT = 1主表+10索引表 = 11 relations
//   类型C: 向量表风格 — 1 UNIQUE + 1 SECONDARY + 1 IVF-FLAT(3表) = 1主表+5索引表 = 6 relations
//
// 使用步骤：
//   1. 编译 MO（包含 operator.go 中的 MO_DELAY_AFTER_UNLOCK_MS hack）
//   2. 启动 MO 时设置环境变量：
//      export MO_DELAY_AFTER_UNLOCK_MS=500
//   3. 运行本工具：go run reproduce.go -host 127.0.0.1 -port 6001

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host   = flag.String("host", "127.0.0.1", "MO host")
	port   = flag.Int("port", 6001, "MO port")
	user   = flag.String("user", "root", "MO user")
	pass   = flag.String("pass", "111", "MO password")
	rounds = flag.Int("rounds", 200, "number of test rounds")
	tables = flag.Int("tables", 20, "number of tables to branch concurrently")
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=30s&readTimeout=60s&writeTimeout=60s&interpolateParams=true",
		*user, *pass, *host, *port)
}

func mustExec(db *sql.DB, q string) {
	if _, err := db.Exec(q); err != nil {
		log.Printf("WARN: %s — %v", q, err)
	}
}

// checkOrphans 检查 database 是否已删除但 mo_tables 中仍有残留记录
func checkOrphans(conn *sql.DB, dbName string) (dbExists bool, orphans []string, err error) {
	var cnt int
	if err = conn.QueryRow(
		"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname=? AND account_id=0",
		dbName).Scan(&cnt); err != nil {
		return
	}
	dbExists = cnt > 0

	rows, err := conn.Query(
		"SELECT relname, rel_id FROM mo_catalog.mo_tables WHERE reldatabase=? AND account_id=0",
		dbName)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var id uint64
		if err = rows.Scan(&name, &id); err != nil {
			return
		}
		orphans = append(orphans, fmt.Sprintf("%s(id=%d)", name, id))
	}
	err = rows.Err()
	return
}

func runOneRound(roundID int, pool *sql.DB) (found bool, msg string) {
	dbName := fmt.Sprintf("race_test_%d", roundID)
	srcDB := "race_src"

	// 清理
	mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	time.Sleep(10 * time.Millisecond)

	// 创建目标数据库
	if _, err := pool.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbName)); err != nil {
		return false, fmt.Sprintf("create db err: %v", err)
	}

	var wg sync.WaitGroup
	var dropErr error

	// 核心分析：为什么不好复现？
	//
	// doWrite commit 路径中 defer 执行顺序（LIFO）：
	//   1. defer B: unlock(ctx) → 等 logtail → lockService.Unlock → [500ms delay]
	//   2. defer A: closeLocked() → updateLastCommitTS
	//   3. doWrite 返回 → 客户端收到 commit 响应
	//
	// 客户端收到响应时，updateLastCommitTS 已经执行完了！
	// 所以 branchDone 信号发出时，latestCommitTS 已经包含了这个 branch 的 commitTS。
	//
	// 线上能出问题的场景：
	//   - 多个 branch 并发，Branch A 的 unlock 释放了 Shared lock（步骤 1 完成）
	//   - 但 Branch A 的 closeLocked 还没执行（在 500ms delay 中）
	//   - 同时 Branch B 也释放了 Shared lock
	//   - DROP 获取 Exclusive lock，v1 fix 看到的 latestCommitTS 不包含 Branch A 的 commitTS
	//   - 但可能包含 Branch B 的（如果 B 的 closeLocked 已执行）
	//   - Relations() 漏掉 Branch A 创建的表
	//
	// 复现策略：
	//   启动多个 branch 并发，同时启动 DROP。
	//   DROP 会被 Shared lock 阻塞，等所有 branch 的 Shared lock 释放。
	//   最后一个 branch 释放 Shared lock 后，DROP 获取 Exclusive lock。
	//   此时最后一个 branch 在 500ms delay 中，latestCommitTS 还没更新。
	//   但倒数第二个 branch 可能也在 delay 中（如果它们几乎同时 commit）。
	//   v1 fix 看到的 latestCommitTS 可能不包含这些 branch 的 commitTS。
	//
	// 关键：需要多个 branch 几乎同时 commit，这样它们的 unlock 几乎同时释放 Shared lock，
	// DROP 在最后一个 unlock 后立即获取 Exclusive lock，此时多个 branch 的 closeLocked 都还没执行。

	branchCount := 2 + (roundID % 4) // 2~5 个 branch

	for i := 0; i < branchCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := sql.Open("mysql", dsn())
			if err != nil {
				return
			}
			defer c.Close()
			c.SetMaxOpenConns(1)

			srcTbl := fmt.Sprintf("src_tbl_%03d", idx)
			dstTbl := fmt.Sprintf("tbl_%03d", idx)
			ddl := fmt.Sprintf(
				"data branch create table `%s`.`%s` from `%s`.`%s`",
				dbName, dstTbl, srcDB, srcTbl)
			_, _ = c.Exec(ddl)
		}(i)
	}

	// DROP：立即并发发起，不等 branch 完成。
	// DROP 会被 Shared lock 阻塞，直到所有 branch 的 Shared lock 释放。
	// 最后一个 branch unlock 后，DROP 获取 Exclusive lock。
	// 此时如果有 branch 的 closeLocked 还没执行（在 500ms delay 中），
	// v1 fix 的 GetLatestCommitTS() 看不到这些 branch 的 commitTS。
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := sql.Open("mysql", dsn())
		if err != nil {
			dropErr = err
			return
		}
		defer c.Close()
		c.SetMaxOpenConns(1)
		_, dropErr = c.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	}()

	wg.Wait()

	if dropErr != nil {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, ""
	}

	time.Sleep(500 * time.Millisecond)

	dbExists, orphans, err := checkOrphans(pool, dbName)
	if err != nil {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, fmt.Sprintf("check err: %v", err)
	}

	if dbExists {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, ""
	}

	if len(orphans) > 0 {
		return true, fmt.Sprintf("孤儿表! db=%s 已删除但 mo_tables 残留 %d 条: %v",
			dbName, len(orphans), orphans)
	}
	return false, ""
}

func setupSrcDB(pool *sql.DB) error {
	srcDB := "race_src"
	mustExec(pool, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", srcDB))

	// 创建三种源表，模拟线上 gate_019ca107 场景：
	//
	// 类型 A: infra_configs 风格 — 1 UNIQUE + 3 SECONDARY = 4 索引表 + 1 主表 = 5 relations
	//   线上日志: txn BADE44, 表 476218 + 索引 476214(unique), 476215-476217(secondary)
	//
	// 类型 B: agent_events 风格 — 9 SECONDARY + 1 FULLTEXT = 10 索引表 + 1 主表 = 11 relations
	//   线上日志: txn BADEA1, 表 476230 + 索引 476219-476229, AffectedRows=21
	//
	// 类型 C: 向量表风格 — 1 UNIQUE + 2 SECONDARY + 1 IVF-FLAT(3个索引表) = 6 索引表 + 1 主表 = 7 relations
	//   IVF-FLAT 会创建 metadata/centroids/entries 三个索引表

	for i := 0; i < *tables; i++ {
		tbl := fmt.Sprintf("src_tbl_%03d", i)
		switch i % 3 {
		case 0:
			// 类型 A: infra_configs 风格
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
					"id BIGINT PRIMARY KEY, "+
					"key_name VARCHAR(255), "+
					"scope_type VARCHAR(64), "+
					"scope_user_id VARCHAR(128), "+
					"config_value TEXT, "+
					"UNIQUE KEY uk_name_scope (key_name, scope_type, scope_user_id), "+
					"KEY idx_scope_type (scope_type), "+
					"KEY idx_scope_user (scope_user_id), "+
					"KEY idx_key_name (key_name))",
				srcDB, tbl)
			if _, err := pool.Exec(ddl); err != nil {
				return fmt.Errorf("create src table %s: %w", tbl, err)
			}
			pool.Exec(fmt.Sprintf(
				"INSERT IGNORE INTO `%s`.`%s` (id, key_name, scope_type, scope_user_id, config_value) "+
					"VALUES (1,'k1','global','','v1'),(2,'k2','user','u1','v2'),(3,'k3','user','u2','v3')",
				srcDB, tbl))

		case 1:
			// 类型 B: agent_events 风格 (含 fulltext 索引)
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
					"id BIGINT PRIMARY KEY, "+
					"event_type VARCHAR(64), "+
					"agent_id VARCHAR(128), "+
					"session_id VARCHAR(128), "+
					"user_id VARCHAR(128), "+
					"content TEXT, "+
					"status VARCHAR(32), "+
					"source VARCHAR(64), "+
					"priority INT, "+
					"category VARCHAR(64), "+
					"created_at DATETIME, "+
					"KEY idx_event_type (event_type), "+
					"KEY idx_agent_id (agent_id), "+
					"KEY idx_session_id (session_id), "+
					"KEY idx_user_id (user_id), "+
					"KEY idx_status (status), "+
					"KEY idx_source (source), "+
					"KEY idx_priority (priority), "+
					"KEY idx_category (category), "+
					"KEY idx_created_at (created_at), "+
					"FULLTEXT INDEX ft_content_session (content, session_id) WITH PARSER ngram)",
				srcDB, tbl)
			if _, err := pool.Exec(ddl); err != nil {
				return fmt.Errorf("create src table %s: %w", tbl, err)
			}
			pool.Exec(fmt.Sprintf(
				"INSERT IGNORE INTO `%s`.`%s` "+
					"(id, event_type, agent_id, session_id, user_id, content, status, source, priority, category, created_at) VALUES "+
					"(1,'chat','a1','s1','u1','hello world','done','web',1,'general','2026-01-01 00:00:00'),"+
					"(2,'tool','a2','s2','u2','test content','running','api',2,'debug','2026-01-02 00:00:00'),"+
					"(3,'plan','a3','s3','u3','plan step','pending','cli',3,'plan','2026-01-03 00:00:00')",
				srcDB, tbl))

		case 2:
			// 类型 C: 向量表风格 (含 IVF-FLAT 索引)
			// IVF-FLAT 会创建 3 个索引表 (metadata, centroids, entries)
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
					"id BIGINT PRIMARY KEY, "+
					"doc_id VARCHAR(128), "+
					"title VARCHAR(255), "+
					"embedding vecf32(3), "+
					"UNIQUE KEY uk_doc_id (doc_id), "+
					"KEY idx_title (title), "+
					"KEY idx_vec USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops')",
				srcDB, tbl)
			if _, err := pool.Exec(ddl); err != nil {
				return fmt.Errorf("create src table %s: %w", tbl, err)
			}
			pool.Exec(fmt.Sprintf(
				"INSERT IGNORE INTO `%s`.`%s` (id, doc_id, title, embedding) VALUES "+
					"(1,'d1','hello','[1.0, 2.0, 3.0]'),"+
					"(2,'d2','world','[4.0, 5.0, 6.0]'),"+
					"(3,'d3','test','[7.0, 8.0, 9.0]')",
				srcDB, tbl))
		}
	}
	return nil
}

func main() {
	flag.Parse()

	log.Println("=== data branch create table + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d", *host, *port, *rounds)
	log.Println("")
	log.Println("根因: 事务 commit 时 defer LIFO 导致 unlock 在 updateLastCommitTS 之前执行")
	log.Println("  unlock → Shared lock 释放 → DROP 获取 Exclusive lock")
	log.Println("  此时 latestCommitTS 还没更新 → v1 fix 不触发 → Relations() 用旧 snapshot")
	log.Println("")
	log.Println("源表索引结构 (匹配线上 gate_019ca107):")
	log.Println("  i%3==0: infra_configs 风格 — 1 UNIQUE + 3 SECONDARY = 5 relations/表")
	log.Println("  i%3==1: agent_events 风格 — 9 SECONDARY + 1 FULLTEXT = 11 relations/表")
	log.Println("  i%3==2: 向量表风格 — 1 UNIQUE + 1 SECONDARY + 1 IVF-FLAT(3表) = 6 relations/表")
	log.Println("")
	log.Println("请确保:")
	log.Println("  1. MO 编译时包含 operator.go 中的 MO_DELAY_AFTER_UNLOCK_MS hack")
	log.Println("  2. 启动 MO 时设置: export MO_DELAY_AFTER_UNLOCK_MS=500")
	log.Println("")

	pool, err := sql.Open("mysql", dsn())
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(5)
	if err := pool.Ping(); err != nil {
		log.Fatalf("Ping 失败: %v", err)
	}

	log.Println("初始化源数据库 race_src (infra_configs + agent_events + IVF-FLAT 风格索引) ...")
	if err := setupSrcDB(pool); err != nil {
		log.Fatalf("初始化源数据库失败: %v", err)
	}
	log.Println("源数据库就绪")
	log.Println("")

	total := 0
	for i := 1; i <= *rounds; i++ {
		found, msg := runOneRound(i, pool)
		if found {
			total++
			log.Printf("!!! 第 %d/%d 轮: %s", i, *rounds, msg)
		} else if i%10 == 0 {
			log.Printf("    第 %d/%d 轮: OK (累计孤儿: %d)", i, *rounds, total)
		}
	}

	log.Printf("\n=== 完成: %d/%d 轮发现孤儿表 ===", total, *rounds)
	if total > 0 {
		log.Printf("BUG 已复现! %d 轮出现孤儿表", total)
	} else {
		log.Println("未复现。尝试增大 tables 数量或检查 MO_DELAY_AFTER_UNLOCK_MS 是否生效。")
	}
}
