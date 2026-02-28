// reproduce.go — 复现 data branch create table + DROP DATABASE 竞态条件
//
// 场景：应用程序对数据库执行 data branch create table（内部转为 CLONE），
// 同时另一个连接 DROP 该数据库。由于 lock service bug（Exclusive lock 获取后
// 锁模式仍为 Shared），CREATE TABLE 的 lockMoDatabase(Shared) 可以绕过
// DROP 的 Exclusive lock。
//
// v1 fix 在 lock 之后刷新 snapshot 到 latestCommitTS，但由于 lock service bug，
// 新的 CREATE TABLE 可以在 v1 fix 之后继续进来并提交，Relations() 仍然会漏掉。
//
// 使用步骤：
//   1. 编译 MO（包含 ddl.go 中的 MO_DELAY_BEFORE_RELATIONS_MS hack）
//   2. 启动 MO 时设置环境变量：
//      export MO_DELAY_BEFORE_RELATIONS_MS=500
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
	delay  = flag.Int("delay", 500, "suggested MO_DELAY_BEFORE_RELATIONS_MS value (ms)")
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

	// 并发执行 data branch create table（模拟 data branch 操作）
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
				// 可能因为 db 已被 drop 而失败，正常
				return
			}
			branchDone.Add(1)
		}(i)
	}

	// DROP：等几张表创建成功后立即 drop
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 等至少 2 张表创建成功
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

	// 等 logtail 追上
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
			"CREATE TABLE IF NOT EXISTS `%s`.`%s` (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
				"val TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			srcDB, tbl)
		if _, err := pool.Exec(ddl); err != nil {
			return fmt.Errorf("create src table %s: %w", tbl, err)
		}
		// 插入一些数据
		ins := fmt.Sprintf(
			"INSERT IGNORE INTO `%s`.`%s` (id, name, val) VALUES (1,'a','x'),(2,'b','y'),(3,'c','z')",
			srcDB, tbl)
		pool.Exec(ins)
	}
	return nil
}

func main() {
	flag.Parse()

	log.Println("=== data branch create table + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d, 每轮表数=%d", *host, *port, *rounds, *tables)
	log.Println("")
	log.Println("请确保:")
	log.Printf("  1. MO 编译时包含 ddl.go 中的 MO_DELAY_BEFORE_RELATIONS_MS hack")
	log.Printf("  2. 启动 MO 时设置: export MO_DELAY_BEFORE_RELATIONS_MS=%d", *delay)
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
		log.Println("未复现。尝试增大 delay 或 tables 数量。")
	}
}
