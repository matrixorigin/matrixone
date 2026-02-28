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
	"sync/atomic"
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
	var branchDone atomic.Int32
	var dropErr error

	// 并发执行 data branch create table
	for i := 0; i < *tables; i++ {
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
			_, err = c.Exec(ddl)
			if err != nil {
				return
			}
			branchDone.Add(1)
		}(i)
	}

	// DROP：等几张表创建成功后立即 drop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if branchDone.Load() >= 2 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
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
	for i := 0; i < 20; i++ {
		tbl := fmt.Sprintf("src_tbl_%03d", i)
		ddl := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s`.`%s` (id BIGINT PRIMARY KEY, name VARCHAR(255))",
			srcDB, tbl)
		if _, err := pool.Exec(ddl); err != nil {
			return fmt.Errorf("create src table %s: %w", tbl, err)
		}
		pool.Exec(fmt.Sprintf(
			"INSERT IGNORE INTO `%s`.`%s` (id, name) VALUES (1,'a'),(2,'b'),(3,'c')",
			srcDB, tbl))
	}
	return nil
}

func main() {
	flag.Parse()

	log.Println("=== data branch create table + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d, 每轮表数=%d", *host, *port, *rounds, *tables)
	log.Println("")
	log.Println("根因: 事务 commit 时 defer LIFO 导致 unlock 在 updateLastCommitTS 之前执行")
	log.Println("  unlock → Shared lock 释放 → DROP 获取 Exclusive lock")
	log.Println("  此时 latestCommitTS 还没更新 → v1 fix 不触发 → Relations() 用旧 snapshot")
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

	log.Println("初始化源数据库 race_src ...")
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
