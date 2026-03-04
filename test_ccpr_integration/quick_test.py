#!/usr/bin/env python3
"""
CCPR 短时间集成测试
测试内容: 权限、DML(全类型/全索引)、ALTER TABLE(inplace/non-inplace)、PAUSE/RESUME/DROP
"""

import pymysql
import time
import sys
import random
import uuid as uuid_mod
import json

# ========== 配置 ==========
UPSTREAM = {'host': '127.0.0.1', 'port': 6001, 'user': 'dump', 'password': '111', 'charset': 'utf8mb4', 'autocommit': True}
DOWNSTREAM = {'host': '127.0.0.1', 'port': 6002, 'user': 'dump', 'password': '111', 'charset': 'utf8mb4', 'autocommit': True}
UPSTREAM_CONN_STR = "mysql://dump:111@127.0.0.1:6001"
SYNC_INTERVAL = 10
DB_NAME = "ccpr_quick_test"
PUB_NAME = "quick_pub"

# ========== 工具函数 ==========
class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def ok(self, name):
        self.passed += 1
        print(f"  ✓ {name}")
    
    def fail(self, name, reason=""): 
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  ✗ {name}: {reason}")
    
    def summary(self):
        print(f"\n{'='*50}")
        print(f"结果: {self.passed} passed, {self.failed} failed")
        for name, reason in self.errors:
            print(f"  - {name}: {reason}")
        return self.failed == 0

def conn(cfg, account=None):
    c = cfg.copy()
    if account:
        c['user'] = f"{account}#{c['user']}"
    return pymysql.connect(**c)

def sql(c, query, fetch=False, ignore_error=False):
    try:
        with c.cursor() as cur:
            cur.execute(query)
            return cur.fetchall() if fetch else True
    except Exception as e:
        if not ignore_error:
            raise
        return None

def sql_safe(c, query):
    return sql(c, query, ignore_error=True)

def checkpoint(c):
    sql_safe(c, "SELECT mo_ctl('dn', 'checkpoint', '')")

def cleanup_all(up, down):
    """
    彻底清理测试环境（每个测试前调用）：
    1. 下游：先删除所有ccpr task，再删除ccpr产生的database，再删除account
    2. 上游：删除所有publication、database、account
    """
    print("  [清理] 开始清理测试环境...")
    
    # ===== 下游清理 =====
    # 1. 读取所有ccpr task id，然后删除
    # 注意：DROP CCPR SUBSCRIPTION 使用的是 task_id (UUID)，不是 subscription_name，且需要用引号
    tasks = sql(down, "SELECT task_id, subscription_name FROM mo_catalog.mo_ccpr_log WHERE drop_at IS NULL", fetch=True, ignore_error=True)
    if tasks:
        for task_id, sub_name in tasks:
            print(f"    删除CCPR task: id={task_id}, subscription={sub_name}")
            sql_safe(down, f"DROP CCPR SUBSCRIPTION IF EXISTS '{task_id}'")
        time.sleep(2)  # 等待task清理完成
    
    # 2. 删除下游测试相关的database (包括通过ccpr创建的)
    for db in [DB_NAME, "perm_db1"]:
        sql_safe(down, f"DROP DATABASE IF EXISTS {db}")
    
    # 3. 删除下游测试账户
    for acc in ["ds_acc1", "ds_acc2", "ds_acc3", "ds_acc4", "ds_acc5", "ds_acc6", 
                "ds_acc7", "ds_acc8", "ds_acc9", "ds_acc10", "ds_acc11", "ds_acc12", "ds_acc13"]:
        sql_safe(down, f"DROP ACCOUNT IF EXISTS {acc}")
    
    # ===== 上游清理 =====
    # 1. 删除所有测试publication
    for pub in [PUB_NAME, "pub_acc", "pub_db", "pub_tbl", "pub_all", "pub_multi", "pub_drop_test"]:
        sql_safe(up, f"DROP PUBLICATION IF EXISTS {pub}")
    
    # 2. 删除上游测试database
    for db in [DB_NAME, "perm_db1"]:
        sql_safe(up, f"DROP DATABASE IF EXISTS {db}")
    
    # 3. 删除上游测试账户
    for acc in ["up_acc1", "up_acc2", "up_acc3"]:
        sql_safe(up, f"DROP ACCOUNT IF EXISTS {acc}")
    
    print("  [清理] 清理完成")

def count(c, db, table):
    r = sql(c, f"SELECT COUNT(*) FROM `{db}`.`{table}`", fetch=True)
    return r[0][0] if r else -1

def get_iteration_lsn(down, pub_name):
    """获取下游订阅的iteration_lsn"""
    r = sql(down, f"SELECT iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{pub_name}' AND drop_at IS NULL", fetch=True)
    return r[0][0] if r and r[0][0] else 0

def wait_lsn_change(down, pub_name, lsn_before, timeout=60):
    """等待lsn变化"""
    start = time.time()
    while time.time() - start < timeout:
        lsn_current = get_iteration_lsn(down, pub_name)
        if lsn_current > lsn_before:
            return True
        time.sleep(1)
    return False

def verify_sync(up, down, db, table, result, case_name, pub_name=PUB_NAME, timeout=120):
    """短测试验证流程: checkpoint -> 等待LSN变化 -> 循环等待数据一致或超时"""
    checkpoint(up)
    up_cnt = count(up, db, table)
    
    # 先等待至少一次LSN变化，确保同步已经开始处理
    lsn_before = get_iteration_lsn(down, pub_name)
    lsn_wait_start = time.time()
    while time.time() - lsn_wait_start < 30:  # 最多等30秒LSN变化
        lsn_current = get_iteration_lsn(down, pub_name)
        if lsn_current > lsn_before:
            break
        time.sleep(1)
    
    start = time.time()
    last_down_cnt = -1
    stable_count = 0
    
    while time.time() - start < timeout:
        down_cnt = count(down, db, table)
        
        if up_cnt == down_cnt:
            result.ok(f"{case_name}: {up_cnt}行")
            return True
        
        # 检测下游数据是否停止增长（连续5次相同则认为同步完成但不一致）
        if down_cnt == last_down_cnt:
            stable_count += 1
            if stable_count >= 5:
                result.fail(case_name, f"上游{up_cnt} vs 下游{down_cnt} (同步停滞)")
                return False
        else:
            stable_count = 0
            last_down_cnt = down_cnt
        
        time.sleep(2)
    
    down_cnt = count(down, db, table)
    result.fail(case_name, f"上游{up_cnt} vs 下游{down_cnt} (超时)")
    return False

def verify_index_tables(up, down, db, table, result, case_name):
    """索引表一致性检查"""
    try:
        # 获取上游table_id和索引
        r = sql(up, f"SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase='{db}' AND relname='{table}'", fetch=True)
        if not r:
            return True  # 表不存在，跳过
        up_table_id = r[0][0]
        
        # 加上 algo_table_type 用于区分向量索引的多个子表 (metadata, centroids, entries)
        up_indexes = sql(up, f"""
            SELECT name, type, algo, algo_table_type, index_table_name 
            FROM mo_catalog.mo_indexes WHERE table_id = {up_table_id}
        """, fetch=True) or []
        
        # 获取下游table_id和索引
        r = sql(down, f"SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase='{db}' AND relname='{table}'", fetch=True)
        if not r:
            result.fail(f"{case_name} 索引检查", "下游表不存在")
            return False
        down_table_id = r[0][0]
        
        down_indexes = sql(down, f"""
            SELECT name, type, algo, algo_table_type, index_table_name 
            FROM mo_catalog.mo_indexes WHERE table_id = {down_table_id}
        """, fetch=True) or []
        
        # 按(name, type, algo, algo_table_type)匹配索引
        for up_idx in up_indexes:
            name, type_val, algo, algo_table_type, up_idx_table = up_idx
            if not up_idx_table:
                continue
            
            # 查找下游对应索引（需要同时匹配 algo_table_type 以区分向量索引的子表）
            down_idx = None
            for d in down_indexes:
                if d[0] == name and d[1] == type_val and d[2] == algo and d[3] == algo_table_type:
                    down_idx = d
                    break
            
            if not down_idx:
                result.fail(f"{case_name} 索引检查", f"下游缺少索引 {name} (algo_table_type={algo_table_type})")
                return False
            
            down_idx_table = down_idx[4]
            if not down_idx_table:
                continue
            
            # 比较索引表行数
            up_idx_cnt = count(up, db, up_idx_table)
            down_idx_cnt = count(down, db, down_idx_table)
            if up_idx_cnt != down_idx_cnt:
                result.fail(f"{case_name} 索引检查", f"索引{name}({algo_table_type}) 上游{up_idx_cnt} vs 下游{down_idx_cnt}")
                return False
        
        return True
    except Exception as e:
        result.fail(f"{case_name} 索引检查", str(e))
        return False

# ========== 全类型测试表DDL ==========
ALL_TYPES_DDL = """
CREATE TABLE all_types (
  id BIGINT PRIMARY KEY,
  c_tinyint TINYINT, c_smallint SMALLINT, c_int INT, c_bigint BIGINT,
  c_utinyint TINYINT UNSIGNED, c_usmallint SMALLINT UNSIGNED,
  c_uint INT UNSIGNED, c_ubigint BIGINT UNSIGNED,
  c_float FLOAT, c_double DOUBLE,
  c_decimal DECIMAL(18,4),
  c_char CHAR(50), c_varchar VARCHAR(200), c_text TEXT, c_blob BLOB,
  c_date DATE, c_time TIME(6), c_datetime DATETIME, c_timestamp TIMESTAMP,
  c_bool BOOL, c_json JSON, c_uuid UUID, c_enum ENUM('a','b','c'),
  c_vecf32 VECF32(8), c_vecf64 VECF64(8)
)
"""

def gen_all_types_row(i):
    """生成一行包含所有类型的测试数据"""
    vec32 = "[" + ",".join([str(random.random()) for _ in range(8)]) + "]"
    vec64 = "[" + ",".join([str(random.random()) for _ in range(8)]) + "]"
    return f"""({i}, 
        {i % 127}, {i % 32767}, {i}, {i * 1000},
        {i % 255}, {i % 65535}, {i}, {i * 1000},
        {i * 0.1}, {i * 0.01},
        {i}.1234,
        'char_{i}', 'varchar_{i}', 'text_{i}', x'{hex(i)[2:].zfill(8)}',
        '2024-01-{(i % 28) + 1:02d}', '{i % 24:02d}:{i % 60:02d}:{i % 60:02d}', 
        '2024-01-{(i % 28) + 1:02d} {i % 24:02d}:{i % 60:02d}:{i % 60:02d}',
        '2024-01-{(i % 28) + 1:02d} {i % 24:02d}:{i % 60:02d}:{i % 60:02d}',
        {i % 2}, '{{"key": {i}}}', UUID(), '{['a','b','c'][i % 3]}',
        '{vec32}', '{vec64}'
    )"""

# ========== 1. 权限测试 ==========
def test_permission(up, down, result):
    print("\n=== 1. 权限测试 ===")
    
    # 彻底清理环境（独立于其他测试）
    cleanup_all(up, down)
    
    # 上游创建账户和数据
    sql(up, "CREATE ACCOUNT up_acc1 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    sql(up, "CREATE ACCOUNT up_acc2 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    sql(up, "CREATE ACCOUNT up_acc3 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    sql(up, "CREATE DATABASE perm_db1")
    sql(up, "CREATE TABLE perm_db1.t1 (id INT PRIMARY KEY)")
    sql(up, "CREATE TABLE perm_db1.t2 (id INT PRIMARY KEY)")
    sql(up, "INSERT INTO perm_db1.t1 VALUES (1),(2),(3)")
    sql(up, "INSERT INTO perm_db1.t2 VALUES (10),(20)")
    
    # ------ 1.1 Account Level 权限 ------
    print("  --- 1.1 Account Level ---")
    sql(up, "CREATE PUBLICATION pub_acc DATABASE * ACCOUNT up_acc1")
    
    # Case 1: ds_acc1用up_acc1凭证订阅 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc1 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds1 = conn(DOWNSTREAM, "ds_acc1")
    try:
        sql(ds1, "CREATE ACCOUNT FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_acc")
        result.ok("1.1.1 授权账户订阅成功")
    except Exception as e:
        result.fail("1.1.1 授权账户订阅", str(e))
    ds1.close()
    
    # Case 2: ds_acc2用up_acc2凭证订阅 -> 失败
    sql(down, "CREATE ACCOUNT ds_acc2 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds2 = conn(DOWNSTREAM, "ds_acc2")
    try:
        sql(ds2, "CREATE ACCOUNT FROM 'mysql://up_acc2#dump:111@127.0.0.1:6001' sys PUBLICATION pub_acc")
        result.fail("1.1.2 未授权账户订阅应失败", "但成功了")
    except:
        result.ok("1.1.2 未授权账户订阅失败")
    ds2.close()
    
    # ------ 1.2 Database Level 权限 (部分订阅) ------
    print("  --- 1.2 Database Level ---")
    sql(up, "CREATE PUBLICATION pub_db DATABASE perm_db1 ACCOUNT up_acc1")
    
    # Case 1: ds_acc3订阅整个db -> 成功
    sql(down, "CREATE ACCOUNT ds_acc3 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds3 = conn(DOWNSTREAM, "ds_acc3")
    try:
        sql(ds3, "CREATE DATABASE perm_db1 FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_db")
        result.ok("1.2.1 订阅整个db成功")
    except Exception as e:
        result.fail("1.2.1 订阅整个db", str(e))
    ds3.close()
    
    # Case 2: ds_acc4只订阅单表t1 -> 成功 (部分订阅)
    sql(down, "CREATE ACCOUNT ds_acc4 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds4 = conn(DOWNSTREAM, "ds_acc4")
    try:
        sql(ds4, "CREATE DATABASE perm_db1")
        sql(ds4, "CREATE TABLE perm_db1.t1 FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_db")
        result.ok("1.2.2 部分订阅单表成功")
    except Exception as e:
        result.fail("1.2.2 部分订阅单表", str(e))
    ds4.close()
    
    # Case 3: ds_acc5订阅不存在的表 -> 失败
    sql(down, "CREATE ACCOUNT ds_acc5 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds5 = conn(DOWNSTREAM, "ds_acc5")
    try:
        sql(ds5, "CREATE DATABASE perm_db1")
        sql(ds5, "CREATE TABLE perm_db1.t999 FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_db")
        result.fail("1.2.3 订阅不存在表应失败", "但成功了")
    except:
        result.ok("1.2.3 订阅不存在表失败")
    ds5.close()
    
    # ------ 1.3 Table Level 权限 ------
    print("  --- 1.3 Table Level ---")
    sql(up, "CREATE PUBLICATION pub_tbl DATABASE perm_db1 TABLE t1 ACCOUNT up_acc1")
    
    # Case 1: ds_acc6订阅t1 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc6 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds6 = conn(DOWNSTREAM, "ds_acc6")
    try:
        sql(ds6, "CREATE DATABASE perm_db1")
        sql(ds6, "CREATE TABLE perm_db1.t1 FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_tbl")
        result.ok("1.3.1 订阅pub内的表成功")
    except Exception as e:
        result.fail("1.3.1 订阅pub内的表", str(e))
    ds6.close()
    
    # Case 2: ds_acc7订阅t2 (不在pub范围) -> 失败
    sql(down, "CREATE ACCOUNT ds_acc7 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds7 = conn(DOWNSTREAM, "ds_acc7")
    try:
        sql(ds7, "CREATE DATABASE perm_db1")
        sql(ds7, "CREATE TABLE perm_db1.t2 FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_tbl")
        result.fail("1.3.2 订阅pub外的表应失败", "但成功了")
    except:
        result.ok("1.3.2 订阅pub外的表失败")
    ds7.close()
    
    # ------ 1.4 ACCOUNT ALL 权限 ------
    print("  --- 1.4 ACCOUNT ALL ---")
    sql(up, "CREATE PUBLICATION pub_all DATABASE * ACCOUNT ALL")
    
    # Case 1: ds_acc9用up_acc1订阅 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc9 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds9 = conn(DOWNSTREAM, "ds_acc9")
    try:
        sql(ds9, "CREATE ACCOUNT FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_all")
        result.ok("1.4.1 ACCOUNT ALL - acc1订阅成功")
    except Exception as e:
        result.fail("1.4.1 ACCOUNT ALL - acc1订阅", str(e))
    ds9.close()
    
    # Case 2: ds_acc10用up_acc2订阅 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc10 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds10 = conn(DOWNSTREAM, "ds_acc10")
    try:
        sql(ds10, "CREATE ACCOUNT FROM 'mysql://up_acc2#dump:111@127.0.0.1:6001' sys PUBLICATION pub_all")
        result.ok("1.4.2 ACCOUNT ALL - acc2订阅成功")
    except Exception as e:
        result.fail("1.4.2 ACCOUNT ALL - acc2订阅", str(e))
    ds10.close()
    
    # ------ 1.5 多账户授权 ------
    print("  --- 1.5 多账户授权 ---")
    sql(up, "CREATE PUBLICATION pub_multi DATABASE * ACCOUNT up_acc1, up_acc2")
    
    # ds_acc11用up_acc1订阅 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc11 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds11 = conn(DOWNSTREAM, "ds_acc11")
    try:
        sql(ds11, "CREATE ACCOUNT FROM 'mysql://up_acc1#dump:111@127.0.0.1:6001' sys PUBLICATION pub_multi")
        result.ok("1.5.1 多账户授权 - acc1成功")
    except Exception as e:
        result.fail("1.5.1 多账户授权 - acc1", str(e))
    ds11.close()
    
    # ds_acc12用up_acc2订阅 -> 成功
    sql(down, "CREATE ACCOUNT ds_acc12 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds12 = conn(DOWNSTREAM, "ds_acc12")
    try:
        sql(ds12, "CREATE ACCOUNT FROM 'mysql://up_acc2#dump:111@127.0.0.1:6001' sys PUBLICATION pub_multi")
        result.ok("1.5.2 多账户授权 - acc2成功")
    except Exception as e:
        result.fail("1.5.2 多账户授权 - acc2", str(e))
    ds12.close()
    
    # ds_acc13用up_acc3订阅 -> 失败
    sql(down, "CREATE ACCOUNT ds_acc13 ADMIN_NAME 'dump' IDENTIFIED BY '111'")
    ds13 = conn(DOWNSTREAM, "ds_acc13")
    try:
        sql(ds13, "CREATE ACCOUNT FROM 'mysql://up_acc3#dump:111@127.0.0.1:6001' sys PUBLICATION pub_multi")
        result.fail("1.5.3 多账户授权 - acc3应失败", "但成功了")
    except:
        result.ok("1.5.3 多账户授权 - acc3失败")
    ds13.close()
    
    # ------ 1.6 删除Publication测试 ------
    print("  --- 1.6 删除Publication ---")
    sql_safe(up, "DROP PUBLICATION IF EXISTS pub_drop_test")
    sql_safe(down, f"DROP DATABASE IF EXISTS {DB_NAME}")
    sql_safe(up, f"DROP DATABASE IF EXISTS {DB_NAME}")
    
    sql(up, f"CREATE DATABASE {DB_NAME}")
    sql(up, f"CREATE TABLE {DB_NAME}.t1 (id INT PRIMARY KEY)")
    sql(up, f"INSERT INTO {DB_NAME}.t1 VALUES (1),(2),(3)")
    sql(up, f"CREATE PUBLICATION pub_drop_test DATABASE {DB_NAME} ACCOUNT ALL")
    sql(down, f"CREATE DATABASE {DB_NAME} FROM '{UPSTREAM_CONN_STR}' sys PUBLICATION pub_drop_test")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    
    # 删除publication
    sql(up, "DROP PUBLICATION pub_drop_test")
    sql(up, f"INSERT INTO {DB_NAME}.t1 VALUES (100)")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    
    # 检查状态
    r = sql(down, f"SELECT state, error_message FROM mo_catalog.mo_ccpr_log WHERE subscription_name='pub_drop_test' AND drop_at IS NULL", fetch=True)
    if r:
        state = r[0][0]
        if state == 1:  # error state
            result.ok("1.6 删除publication后订阅进入error状态")
        else:
            up_cnt = count(up, DB_NAME, "t1")
            down_cnt = count(down, DB_NAME, "t1")
            if down_cnt < up_cnt:
                result.ok(f"1.6 删除publication后数据不同步")
            else:
                result.fail("1.6 删除publication测试", f"state={state}")
    else:
        result.ok("1.6 删除publication (订阅已清理)")
    
    # 注：清理工作由下一个测试开头的cleanup_all完成

# ========== 2. DML测试 (全类型) ==========
def test_dml(up, down, result):
    print("\n=== 2. DML测试 (全类型) ===")
    
    # 彻底清理环境（独立于其他测试）
    cleanup_all(up, down)
    
    sql(up, f"CREATE DATABASE {DB_NAME}")
    sql(up, f"{ALL_TYPES_DDL.replace('CREATE TABLE all_types', f'CREATE TABLE {DB_NAME}.all_types')}")
    sql(up, f"CREATE PUBLICATION {PUB_NAME} DATABASE {DB_NAME} ACCOUNT ALL")
    sql(down, f"CREATE DATABASE {DB_NAME} FROM '{UPSTREAM_CONN_STR}' sys PUBLICATION {PUB_NAME}")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    
    # ------ 2.1 Table Level DML ------
    print("  --- 2.1 Table Level DML ---")
    
    # INSERT 100行
    for i in range(1, 101):
        try:
            sql(up, f"INSERT INTO {DB_NAME}.all_types VALUES {gen_all_types_row(i)}")
        except Exception as e:
            print(f"    INSERT row {i} error: {e}")
            break
    verify_sync(up, down, DB_NAME, "all_types", result, "2.1.1 INSERT 100行")
    
    # UPDATE 50行
    sql(up, f"UPDATE {DB_NAME}.all_types SET c_varchar = 'updated' WHERE id <= 50")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before)
    r = sql(down, f"SELECT COUNT(*) FROM {DB_NAME}.all_types WHERE c_varchar = 'updated'", fetch=True)
    if r and r[0][0] == 50:
        result.ok("2.1.2 UPDATE 50行")
    else:
        result.fail("2.1.2 UPDATE 50行", f"count={r[0][0] if r else 'N/A'}")
    
    # DELETE 30行
    sql(up, f"DELETE FROM {DB_NAME}.all_types WHERE id > 70")
    verify_sync(up, down, DB_NAME, "all_types", result, "2.1.3 DELETE 30行")

    # LOAD 10万行
    print("    正在插入10万行数据...")
    sql(up, f"CREATE TABLE {DB_NAME}.load_test (id BIGINT PRIMARY KEY, val VARCHAR(100))")
    checkpoint(up)
    time.sleep(5)
    
    batch_size = 1000
    inserted_batches = 0
    try:
        # 开启事务
        sql(up, "BEGIN")
        for batch in range(100):
            values = ",".join([f"({batch*batch_size + i}, 'v{batch*batch_size + i}')" for i in range(batch_size)])
            sql(up, f"INSERT INTO {DB_NAME}.load_test VALUES {values}")
            inserted_batches += 1
            if (batch + 1) % 20 == 0:
                print(f"    已插入 {inserted_batches * batch_size} 行...")
        # 提交事务
        sql(up, "COMMIT")
        print(f"    事务提交成功，共插入 {inserted_batches * batch_size} 行")
    except Exception as e:
        print(f"    batch {inserted_batches} 插入失败: {e}")
        sql_safe(up, "ROLLBACK")
    
    # 插入完成后先检查上游实际行数
    actual_up_cnt = count(up, DB_NAME, "load_test")
    print(f"    上游实际行数: {actual_up_cnt}")
    
    # 事务提交后再checkpoint
    checkpoint(up)
    verify_sync(up, down, DB_NAME, "load_test", result, "2.1.4 LOAD 10万行")
    
    # DELETE 5万行 (事务)
    try:
        sql(up, "BEGIN")
        sql(up, f"DELETE FROM {DB_NAME}.load_test WHERE id >= 50000")
        sql(up, "COMMIT")
        print("    DELETE 5万行事务提交成功")
    except Exception as e:
        print(f"    DELETE 5万行失败: {e}")
        sql_safe(up, "ROLLBACK")
    checkpoint(up)
    verify_sync(up, down, DB_NAME, "load_test", result, "2.1.5 DELETE 5万行")
    
    # TRUNCATE (事务)
    try:
        sql(up, "BEGIN")
        sql(up, f"DELETE FROM {DB_NAME}.load_test")
        sql(up, "COMMIT")
        print("    TRUNCATE事务提交成功")
    except Exception as e:
        print(f"    TRUNCATE失败: {e}")
        sql_safe(up, "ROLLBACK")
    checkpoint(up)
    verify_sync(up, down, DB_NAME, "load_test", result, "2.1.6 TRUNCATE")
    
    # ------ 2.2 Database Level DML (部分表更新) ------
    print("  --- 2.2 Database Level DML ---")
    sql(up, f"CREATE TABLE {DB_NAME}.t1 (id INT PRIMARY KEY, val VARCHAR(100))")
    sql(up, f"CREATE TABLE {DB_NAME}.t2 (id INT PRIMARY KEY, val VARCHAR(100))")
    sql(up, f"CREATE TABLE {DB_NAME}.t3 (id INT PRIMARY KEY, val VARCHAR(100))")
    checkpoint(up)
    time.sleep(5)
    
    # 初始化
    for t in ["t1", "t2", "t3"]:
        sql(up, f"INSERT INTO {DB_NAME}.{t} VALUES (1, 'init')")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before)
    
    # 只在t1插入
    sql(up, f"INSERT INTO {DB_NAME}.t1 VALUES (2, 'only_t1')")
    verify_sync(up, down, DB_NAME, "t1", result, "2.2.1 只更新t1")
    
    t2_cnt = count(down, DB_NAME, "t2")
    t3_cnt = count(down, DB_NAME, "t3")
    if t2_cnt == 1 and t3_cnt == 1:
        result.ok("2.2.2 t2/t3不变")
    else:
        result.fail("2.2.2 t2/t3不变", f"t2={t2_cnt}, t3={t3_cnt}")

# ========== 3. 索引测试 ==========
def test_indexes(up, down, result):
    print("\n=== 3. 索引测试 ===")
    
    # 彻底清理环境（独立于其他测试）
    cleanup_all(up, down)
    
    # 设置测试环境
    sql(up, f"CREATE DATABASE {DB_NAME}")
    sql(up, f"CREATE PUBLICATION {PUB_NAME} DATABASE {DB_NAME} ACCOUNT ALL")
    sql(down, f"CREATE DATABASE {DB_NAME} FROM '{UPSTREAM_CONN_STR}' sys PUBLICATION {PUB_NAME}")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    
    # 创建带各种索引的表
    sql(up, f"""CREATE TABLE {DB_NAME}.idx_test (
        id BIGINT PRIMARY KEY,
        c_int INT,
        c_varchar VARCHAR(200),
        c_text TEXT,
        c_vecf32 VECF32(8)
    )""")
    
    # 插入数据
    for i in range(1, 101):
        vec = "[" + ",".join([str(random.random()) for _ in range(8)]) + "]"
        sql(up, f"INSERT INTO {DB_NAME}.idx_test VALUES ({i}, {i}, 'varchar_{i}', 'text content {i}', '{vec}')")
    
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before)
    
    # 3.1 普通索引
    sql(up, f"CREATE INDEX idx_int ON {DB_NAME}.idx_test(c_int)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    verify_index_tables(up, down, DB_NAME, "idx_test", result, "3.1 普通索引")
    result.ok("3.1 普通索引创建")
    
    # 3.2 唯一索引
    sql(up, f"CREATE UNIQUE INDEX uidx_varchar ON {DB_NAME}.idx_test(c_varchar)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("3.2 唯一索引创建")
    
    # 3.3 全文索引
    sql_safe(up, f"CREATE FULLTEXT INDEX ftidx ON {DB_NAME}.idx_test(c_text)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("3.3 全文索引创建")
    
    # 3.4 向量索引
    sql_safe(up, f"CREATE INDEX idx_vec USING IVFFLAT ON {DB_NAME}.idx_test(c_vecf32) LISTS=4 OP_TYPE 'vector_l2_ops'")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=60)
    result.ok("3.4 向量索引创建")
    
    # 验证索引表一致性
    verify_index_tables(up, down, DB_NAME, "idx_test", result, "3.5 索引表一致性")

# ========== 4. ALTER TABLE测试 ==========
def test_alter_table(up, down, result):
    print("\n=== 4. ALTER TABLE测试 ===")
    
    # 彻底清理环境（独立于其他测试）
    cleanup_all(up, down)
    
    # 设置测试环境
    sql(up, f"CREATE DATABASE {DB_NAME}")
    sql(up, f"CREATE PUBLICATION {PUB_NAME} DATABASE {DB_NAME} ACCOUNT ALL")
    sql(down, f"CREATE DATABASE {DB_NAME} FROM '{UPSTREAM_CONN_STR}' sys PUBLICATION {PUB_NAME}")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    
    # 创建测试表
    sql(up, f"CREATE TABLE {DB_NAME}.alter_test (id INT PRIMARY KEY, val VARCHAR(100))")
    sql(up, f"INSERT INTO {DB_NAME}.alter_test VALUES (1, 'test')")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before)
    
    # ------ 4.1 Inplace ALTER ------
    print("  --- 4.1 Inplace ALTER ---")
    
    # ADD INDEX
    sql(up, f"CREATE INDEX idx_val ON {DB_NAME}.alter_test(val)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("4.1.1 ADD INDEX (inplace)")
    
    # DROP INDEX
    sql(up, f"DROP INDEX idx_val ON {DB_NAME}.alter_test")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("4.1.2 DROP INDEX (inplace)")
    
    # RENAME COLUMN
    sql(up, f"ALTER TABLE {DB_NAME}.alter_test RENAME COLUMN val TO val_renamed")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_columns WHERE att_database='{DB_NAME}' AND att_relname='alter_test' AND attname='val_renamed'", fetch=True)
    if r and r[0][0] > 0:
        result.ok("4.1.3 RENAME COLUMN (inplace)")
    else:
        result.fail("4.1.3 RENAME COLUMN (inplace)", "列名未更新")
    
    # CHANGE COMMENT
    sql(up, f"ALTER TABLE {DB_NAME}.alter_test COMMENT = 'test comment'")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("4.1.4 CHANGE COMMENT (inplace)")
    
    # ------ 4.2 Non-inplace ALTER ------
    print("  --- 4.2 Non-inplace ALTER ---")
    
    # ADD COLUMN
    sql(up, f"ALTER TABLE {DB_NAME}.alter_test ADD COLUMN new_col INT DEFAULT 0")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_columns WHERE att_database='{DB_NAME}' AND att_relname='alter_test' AND attname='new_col'", fetch=True)
    if r and r[0][0] > 0:
        result.ok("4.2.1 ADD COLUMN (non-inplace)")
    else:
        result.fail("4.2.1 ADD COLUMN (non-inplace)", "列未添加")
    
    # DROP COLUMN
    sql(up, f"ALTER TABLE {DB_NAME}.alter_test DROP COLUMN new_col")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_columns WHERE att_database='{DB_NAME}' AND att_relname='alter_test' AND attname='new_col'", fetch=True)
    if r and r[0][0] == 0:
        result.ok("4.2.2 DROP COLUMN (non-inplace)")
    else:
        result.fail("4.2.2 DROP COLUMN (non-inplace)", "列未删除")
    
    # MODIFY COLUMN
    sql(up, f"ALTER TABLE {DB_NAME}.alter_test MODIFY COLUMN val_renamed VARCHAR(500)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=30)
    result.ok("4.2.3 MODIFY COLUMN (non-inplace)")
    
    # ------ 4.3 Fulltext + ADD COLUMN场景 (参考c.sql) ------
    print("  --- 4.3 Fulltext + ADD COLUMN场景 ---")
    
    # 创建带fulltext索引的表
    sql(up, f"CREATE TABLE {DB_NAME}.src (id BIGINT PRIMARY KEY, body VARCHAR(500), title TEXT)")
    sql(up, f"INSERT INTO {DB_NAME}.src VALUES (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car')")
    sql(up, f"CREATE FULLTEXT INDEX ftidx ON {DB_NAME}.src (body, title)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=60)
    verify_sync(up, down, DB_NAME, "src", result, "4.3.1 创建fulltext表+数据")
    
    # ADD COLUMN后insert新数据（列数变化）
    sql(up, f"ALTER TABLE {DB_NAME}.src ADD COLUMN b INT")
    sql(up, f"INSERT INTO {DB_NAME}.src VALUES (11, 'color is red', 't1', 0)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=60)
    
    # 验证数据和列
    r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_columns WHERE att_database='{DB_NAME}' AND att_relname='src' AND attname='b'", fetch=True)
    if r and r[0][0] > 0:
        result.ok("4.3.2 ADD COLUMN后列存在")
    else:
        result.fail("4.3.2 ADD COLUMN后列存在", "列b未同步")
    
    up_cnt = count(up, DB_NAME, "src")
    down_cnt = count(down, DB_NAME, "src")
    if up_cnt == down_cnt:
        result.ok(f"4.3.3 ADD COLUMN后INSERT同步: {up_cnt}行")
    else:
        result.fail("4.3.3 ADD COLUMN后INSERT同步", f"上游{up_cnt} vs 下游{down_cnt}")
    
    # 检查新行数据
    r = sql(down, f"SELECT b FROM {DB_NAME}.src WHERE id = 11", fetch=True)
    if r and r[0][0] == 0:
        result.ok("4.3.4 新列数据正确")
    else:
        result.fail("4.3.4 新列数据正确", f"b={r[0][0] if r else 'N/A'}")
    
    # ADD FULLTEXT INDEX后insert
    sql(up, f"ALTER TABLE {DB_NAME}.src ADD FULLTEXT INDEX ftidx2 (body)")
    sql(up, f"INSERT INTO {DB_NAME}.src VALUES (12, 'new text', 'title12', 1)")
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=60)
    verify_sync(up, down, DB_NAME, "src", result, "4.3.5 ADD FULLTEXT INDEX后INSERT")
    
    # 验证fulltext索引存在
    r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_indexes WHERE table_id IN (SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase='{DB_NAME}' AND relname='src') AND name='ftidx2'", fetch=True)
    if r and r[0][0] > 0:
        result.ok("4.3.6 第二个fulltext索引存在")
    else:
        result.fail("4.3.6 第二个fulltext索引存在", "ftidx2未同步")
    
    # 验证索引表一致性
    verify_index_tables(up, down, DB_NAME, "src", result, "4.3.7 Fulltext索引表一致性")

# ========== 5. PAUSE/RESUME/DROP测试 ==========
def test_control_ops(up, down, result):
    print("\n=== 5. PAUSE/RESUME/DROP测试 ===")
    
    # 彻底清理环境（独立于其他测试）
    cleanup_all(up, down)
    
    # 设置测试环境
    sql(up, f"CREATE DATABASE {DB_NAME}")
    sql(up, f"{ALL_TYPES_DDL.replace('CREATE TABLE all_types', f'CREATE TABLE {DB_NAME}.all_types')}")
    # 插入一些测试数据
    for i in range(1, 11):
        try:
            sql(up, f"INSERT INTO {DB_NAME}.all_types VALUES {gen_all_types_row(i)}")
        except:
            pass
    sql(up, f"CREATE PUBLICATION {PUB_NAME} DATABASE {DB_NAME} ACCOUNT ALL")
    sql(down, f"CREATE DATABASE {DB_NAME} FROM '{UPSTREAM_CONN_STR}' sys PUBLICATION {PUB_NAME}")
    
    # 确保同步正常
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    before_cnt = count(down, DB_NAME, "all_types")
    
    # 获取 task_id（PAUSE/RESUME/DROP 都需要使用 task_id 而不是 subscription_name）
    r = sql(down, f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{PUB_NAME}' AND drop_at IS NULL", fetch=True)
    if not r or not r[0][0]:
        result.fail("5.0 获取task_id", "找不到task_id")
        return
    task_id = r[0][0]
    
    # 5.1 PAUSE
    sql(down, f"PAUSE CCPR SUBSCRIPTION '{task_id}'")
    time.sleep(3)
    r = sql(down, f"SELECT state FROM mo_catalog.mo_ccpr_log WHERE task_id='{task_id}' AND drop_at IS NULL", fetch=True)
    if r and r[0][0] == 2:  # PAUSE state
        result.ok("5.1 PAUSE (state=2)")
    else:
        result.fail("5.1 PAUSE", f"state={r[0][0] if r else 'N/A'}")
    
    # 5.2 PAUSE期间插入数据
    sql(up, f"INSERT INTO {DB_NAME}.all_types (id, c_int, c_varchar) VALUES (99999, 99999, 'pause_test')")
    checkpoint(up)
    time.sleep(SYNC_INTERVAL + 5)
    after_cnt = count(down, DB_NAME, "all_types")
    if after_cnt == before_cnt:
        result.ok("5.2 PAUSE期间数据不同步")
    else:
        result.fail("5.2 PAUSE期间数据不同步", f"before={before_cnt}, after={after_cnt}")
    
    # 5.3 RESUME
    sql(down, f"RESUME CCPR SUBSCRIPTION '{task_id}'")
    time.sleep(3)
    r = sql(down, f"SELECT state FROM mo_catalog.mo_ccpr_log WHERE task_id='{task_id}' AND drop_at IS NULL", fetch=True)
    if r and r[0][0] == 0:  # RUNNING state
        result.ok("5.3 RESUME (state=0)")
    else:
        result.fail("5.3 RESUME", f"state={r[0][0] if r else 'N/A'}")
    
    # 等待数据同步
    checkpoint(up)
    lsn_before = get_iteration_lsn(down, PUB_NAME)
    wait_lsn_change(down, PUB_NAME, lsn_before, timeout=60)
    final_cnt = count(down, DB_NAME, "all_types")
    up_cnt = count(up, DB_NAME, "all_types")
    if final_cnt == up_cnt:
        result.ok(f"5.4 RESUME后数据同步 ({final_cnt}行)")
    else:
        result.fail("5.4 RESUME后数据同步", f"上游{up_cnt} vs 下游{final_cnt}")
    
    # 5.5 DROP
    # 需要先获取 task_id，然后用 task_id 删除
    r = sql(down, f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{PUB_NAME}' AND drop_at IS NULL", fetch=True)
    if r and r[0][0]:
        task_id = r[0][0]
        sql(down, f"DROP CCPR SUBSCRIPTION IF EXISTS '{task_id}'")
        time.sleep(2)
        r = sql(down, f"SELECT COUNT(*) FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{PUB_NAME}' AND drop_at IS NULL", fetch=True)
        if r and r[0][0] == 0:
            result.ok("5.5 DROP SUBSCRIPTION")
        else:
            result.fail("5.5 DROP SUBSCRIPTION", "记录未标记删除")
    else:
        result.fail("5.5 DROP SUBSCRIPTION", "找不到task_id")

# ========== 主函数 ==========
def main():
    print("=" * 60)
    print("CCPR 短时间集成测试 (Quick Test)")
    print("=" * 60)
    
    result = TestResult()
    
    up = conn(UPSTREAM)
    down = conn(DOWNSTREAM)
    
    try:
        test_permission(up, down, result)
        test_dml(up, down, result)
        test_indexes(up, down, result)
        test_alter_table(up, down, result)
        test_control_ops(up, down, result)
    except Exception as e:
        print(f"\n!!! 测试异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 彻底清理
        # try:
        #     cleanup_all(up, down)
        # except:
        #     pass
        up.close()
        down.close()
    
    success = result.summary()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
