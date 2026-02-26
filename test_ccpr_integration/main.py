#!/usr/bin/env python3
"""
CCPR Integration Test - Main Runner

按照TEST_PLAN.md实现的CCPR集成测试:
- 短测试: 权限测试、DML测试、ALTER TABLE测试、控制操作测试
- 长测试: 同时跑account/db/table三种level，6个阶段流程
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from config import (
    TestConfig, ClusterConfig, SyncLevel, SubscriptionState,
    AccountConfig, load_config_from_env
)
from db_utils import (
    DatabaseConnection, execute_sql, execute_sql_safe, get_ccpr_status,
    trigger_checkpoint, get_table_count, verify_data_consistency,
    create_account, drop_account
)
from data_generator import (
    create_test_tables, random_string, escape_sql_string, random_text,
    BasicTable, FulltextTable, VectorTable, TableDefinition
)

logger = logging.getLogger(__name__)


# =============================================================================
# 测试账户配置
# =============================================================================

@dataclass
class CCPRTask:
    """CCPR任务配置"""
    level: SyncLevel
    name: str
    upstream_account: str
    downstream_account: str
    pub_name: str
    db_name: str
    table_name: Optional[str] = None  # 仅table level需要
    task_id: Optional[str] = None
    
    # 连接
    upstream_conn: Optional[DatabaseConnection] = None
    downstream_conn: Optional[DatabaseConnection] = None
    
    # 动态添加的列（用于ALTER测试）
    added_columns: List[str] = field(default_factory=list)
    added_indexes: List[str] = field(default_factory=list)


class LongTestPhase(Enum):
    """长测试阶段"""
    PHASE1_DML_ADD_COLUMN = 1      # 持续DML -> ADD COLUMN (non-inplace)
    PHASE2_DML_ADD_INDEX = 2       # 持续DML -> ADD INDEX (inplace)
    PHASE3_DML_ADD_FULLTEXT = 3    # 持续DML -> ADD FULLTEXT INDEX (inplace)
    PHASE4_DML_RENAME_COLUMN = 4   # 持续DML -> RENAME COLUMN (inplace)
    PHASE5_DML_DROP_COLUMN = 5     # 持续DML -> DROP COLUMN (non-inplace)
    PHASE6_DML_LOAD_COMMENT = 6    # 持续DML + 大批量LOAD -> CHANGE COMMENT (inplace)


# =============================================================================
# 测试基础设施
# =============================================================================

class CCPRLongTest:
    """
    CCPR长时间测试
    
    同时运行3种level的CCPR任务:
    - Account Level: 整个账户级别的订阅
    - Database Level: 数据库级别的订阅  
    - Table Level: 单表级别的订阅
    
    每种level在不同的上下游account下运行
    """
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.tasks: List[CCPRTask] = []
        self.sys_upstream_conn: Optional[DatabaseConnection] = None
        self.sys_downstream_conn: Optional[DatabaseConnection] = None
        self.stop_event = threading.Event()
        self.errors: List[str] = []
        self.dml_threads: List[threading.Thread] = []
        
    def setup(self) -> bool:
        """初始化测试环境"""
        logger.info("="*60)
        logger.info("Setting up CCPR Long Test")
        logger.info("="*60)
        
        try:
            # 系统级连接（用于创建account）
            self.sys_upstream_conn = DatabaseConnection(self.config.upstream)
            self.sys_upstream_conn.connect()
            self.sys_downstream_conn = DatabaseConnection(self.config.downstream)
            self.sys_downstream_conn.connect()
            
            # 只运行table level测试
            levels = [
                (SyncLevel.TABLE, "table"),
            ]
            
            for level, level_name in levels:
                task = self._setup_level(level, level_name)
                if task:
                    self.tasks.append(task)
                    logger.info(f"[{level_name.upper()}] Task setup complete: {task.pub_name}")
                else:
                    logger.error(f"[{level_name.upper()}] Task setup failed, continuing with other tasks")
            
            if not self.tasks:
                logger.error("No tasks were created successfully")
                return False
            
            logger.info(f"Setup complete. {len(self.tasks)} CCPR tasks created.")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            self.errors.append(str(e))
            return False
    
    def _setup_level(self, level: SyncLevel, level_name: str) -> Optional[CCPRTask]:
        """为指定level创建CCPR任务 - 每个任务使用不同的account"""
        suffix = random_string(6).lower()
        
        # 每个任务用不同的上下游account
        up_account = f"up_{level_name}_{suffix}"
        down_account = f"down_{level_name}_{suffix}"
        
        # 数据库、表、publication名
        db_name = f"ccpr_{level_name}_db_{suffix}"
        table_name = f"ccpr_{level_name}_tbl_{suffix}"
        pub_name = f"pub_{level_name}_{suffix}"
        
        try:
            # 1. 用sys在上游创建两个account：
            #    - up_account: 持有database/table
            #    - down_account: 用于publication授权（上游也要有这个account才能授权）
            logger.info(f"[{level_name}] Creating upstream accounts: {up_account}, {down_account}")
            create_account(self.sys_upstream_conn.get_connection(), up_account)
            create_account(self.sys_upstream_conn.get_connection(), down_account)
            
            # 2. 用sys在下游创建account（用于订阅）
            logger.info(f"[{level_name}] Creating downstream account: {down_account}")
            create_account(self.sys_downstream_conn.get_connection(), down_account)
            
            time.sleep(2)  # 等待account生效
            
            # 3. 连接上游account
            up_config = ClusterConfig(
                host=self.config.upstream.host,
                port=self.config.upstream.port,
                user="admin",
                password="111",
                account=up_account
            )
            up_conn = DatabaseConnection(up_config)
            up_conn.connect()
            
            # 4. 创建数据库和表
            logger.info(f"[{level_name}] Creating database: {db_name}")
            execute_sql(up_conn.get_connection(), f"CREATE DATABASE {db_name}")
            
            create_table_sql = f"""
                CREATE TABLE {db_name}.{table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    c_int INT DEFAULT 0,
                    c_bigint BIGINT,
                    c_float FLOAT,
                    c_double DOUBLE,
                    c_decimal DECIMAL(18,4),
                    c_varchar VARCHAR(200),
                    c_text TEXT,
                    c_date DATE,
                    c_datetime DATETIME,
                    c_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    c_bool BOOL DEFAULT FALSE,
                    c_json JSON,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            execute_sql(up_conn.get_connection(), create_table_sql)
            logger.info(f"[{level_name}] Created table: {db_name}.{table_name}")
            
            # 创建初始索引
            execute_sql(up_conn.get_connection(), 
                f"CREATE INDEX idx_status ON {db_name}.{table_name} (status)")
            
            # 插入初始数据
            for i in range(100):
                self._insert_row(up_conn.get_connection(), db_name, table_name)
            logger.info(f"[{level_name}] Inserted 100 initial rows")
            
            # 5. 从tenant账户创建publication，发布给下游account
            # Account level: DATABASE *
            # Database level: DATABASE db_name
            # Table level: DATABASE db_name TABLE table_name
            if level == SyncLevel.ACCOUNT:
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE * ACCOUNT {down_account}"
            elif level == SyncLevel.DATABASE:
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE {db_name} ACCOUNT {down_account}"
            else:  # TABLE
                pub_sql = f"CREATE PUBLICATION {pub_name} DATABASE {db_name} TABLE {table_name} ACCOUNT {down_account}"
            
            # 从tenant账户执行（不是sys）
            execute_sql(up_conn.get_connection(), pub_sql)
            logger.info(f"[{level_name}] Created publication: {pub_sql}")
            
            # 6. 连接下游account，创建订阅
            down_config = ClusterConfig(
                host=self.config.downstream.host,
                port=self.config.downstream.port,
                user="admin",
                password="111",
                account=down_account
            )
            down_conn = DatabaseConnection(down_config)
            down_conn.connect()
            
            # 关键：连接字符串需要使用 down_account 的凭证（上游也创建了该账户）
            # 这样下游才能以被授权的账户身份访问 publication
            sub_conn_str = f"mysql://{down_account}#admin:111@{self.config.upstream.host}:{self.config.upstream.port}"
            
            if level == SyncLevel.ACCOUNT:
                # ACCOUNT level: 订阅到当前session的account
                sub_sql = f"CREATE ACCOUNT FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            elif level == SyncLevel.DATABASE:
                # DATABASE level: 订阅整个数据库
                sub_sql = f"CREATE DATABASE {db_name} FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            else:  # TABLE level
                # TABLE level: 先建库，再订阅表
                execute_sql(down_conn.get_connection(), f"CREATE DATABASE {db_name}")
                sub_sql = f"CREATE TABLE {db_name}.{table_name} FROM '{sub_conn_str}' {up_account} PUBLICATION {pub_name}"
            
            execute_sql(down_conn.get_connection(), sub_sql)
            logger.info(f"[{level_name}] Created subscription: {sub_sql}")
            
            # 等待初始同步 (用sys租户触发checkpoint)
            trigger_checkpoint(self.sys_upstream_conn.get_connection())
            time.sleep(15)
            
            return CCPRTask(
                level=level,
                name=level_name,
                upstream_account=up_account,
                downstream_account=down_account,
                pub_name=pub_name,
                db_name=db_name,
                table_name=table_name,
                upstream_conn=up_conn,
                downstream_conn=down_conn,
            )
            
        except Exception as e:
            logger.error(f"[{level_name}] Setup failed: {e}")
            return None
    
    def _insert_row(self, conn, db_name: str, table_name: str, 
                    extra_columns: List[str] = None):
        """插入一行测试数据"""
        cols = ["c_int", "c_bigint", "c_float", "c_double", "c_decimal",
                "c_varchar", "c_text", "c_date", "c_datetime", "c_bool", 
                "c_json", "status"]
        
        json_val = json.dumps({"key": random_string(10)})
        values = [
            str(random.randint(0, 100000)),
            str(random.randint(0, 10000000000)),
            str(round(random.uniform(-1000, 1000), 4)),
            str(round(random.uniform(-1000000, 1000000), 8)),
            str(round(random.uniform(-10000, 10000), 4)),
            f"'{escape_sql_string(random_string(50))}'",
            f"'{escape_sql_string(random_text(10, 30))}'",
            f"'{datetime.now().strftime('%Y-%m-%d')}'",
            f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'",
            str(random.choice([0, 1])),
            f"'{json_val}'",
            f"'{random.choice(['active', 'inactive', 'pending'])}'",
        ]
        
        # 添加额外的列值
        if extra_columns:
            for col in extra_columns:
                cols.append(col)
                values.append(f"'{random_string(20)}'")
        
        sql = f"INSERT INTO {db_name}.{table_name} ({', '.join(cols)}) VALUES ({', '.join(values)})"
        execute_sql(conn, sql)
    
    def run(self, duration: int) -> bool:
        """运行长时间测试"""
        logger.info("="*60)
        logger.info(f"Starting Long Test for {duration} seconds ({duration/3600:.1f} hours)")
        logger.info("="*60)
        
        start_time = time.time()
        phase_duration = duration / 6  # 每个阶段的时间
        
        phases = [
            LongTestPhase.PHASE1_DML_ADD_COLUMN,
            LongTestPhase.PHASE2_DML_ADD_INDEX,
            LongTestPhase.PHASE3_DML_ADD_FULLTEXT,
            LongTestPhase.PHASE4_DML_RENAME_COLUMN,
            LongTestPhase.PHASE5_DML_DROP_COLUMN,
            LongTestPhase.PHASE6_DML_LOAD_COMMENT,
        ]
        
        try:
            for phase in phases:
                if time.time() - start_time >= duration:
                    break
                
                logger.info(f"\n{'='*60}")
                logger.info(f"Starting {phase.name}")
                logger.info(f"{'='*60}")
                
                # 1. 启动DML线程
                self._start_dml_threads()
                
                # 2. 运行DML直到阶段结束
                phase_end = start_time + (phase.value * phase_duration)
                while time.time() < phase_end and time.time() - start_time < duration:
                    time.sleep(10)
                    # 定期触发checkpoint (用sys租户)
                    trigger_checkpoint(self.sys_upstream_conn.get_connection())
                
                # 3. 停止DML
                self._stop_dml_threads()
                
                # 4. 检查点：PAUSE -> 一致性检查 -> RESUME
                logger.info(f"\n--- Checkpoint for {phase.name} ---")
                for task in self.tasks:
                    self._checkpoint(task)
                
                # 5. 执行ALTER操作
                logger.info(f"\n--- ALTER operations for {phase.name} ---")
                for task in self.tasks:
                    self._execute_alter(task, phase)
                
                # 等待ALTER同步 (用sys租户触发checkpoint)
                trigger_checkpoint(self.sys_upstream_conn.get_connection())
                time.sleep(self.config.sync_interval + 5)
            
            # 最终检查
            logger.info("\n" + "="*60)
            logger.info("Final Checkpoint")
            logger.info("="*60)
            for task in self.tasks:
                self._checkpoint(task)
            
            return len(self.errors) == 0
            
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
            self._stop_dml_threads()
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            self.errors.append(str(e))
            self._stop_dml_threads()
            return False
    
    def _start_dml_threads(self):
        """启动DML线程"""
        self.stop_event.clear()
        self.dml_threads = []
        
        for task in self.tasks:
            t = threading.Thread(target=self._dml_worker, args=(task,), daemon=True)
            t.start()
            self.dml_threads.append(t)
        
        logger.info(f"Started {len(self.dml_threads)} DML threads")
    
    def _stop_dml_threads(self):
        """停止DML线程"""
        self.stop_event.set()
        for t in self.dml_threads:
            t.join(timeout=10)
        self.dml_threads = []
        logger.info("Stopped DML threads")
    
    def _dml_worker(self, task: CCPRTask):
        """DML工作线程 - 持续执行各种DML操作"""
        # DML操作类型列表，轮流执行
        dml_operations = [
            "SMALL_INSERT",      # 小数据插入 (1-5行)
            "SMALL_UPDATE",      # 小数据更新 (1-5行)
            "SMALL_DELETE",      # 小数据删除 (1-5行)
            "BATCH_INSERT",      # 批量插入 (50-100行)
            "BATCH_UPDATE",      # 批量更新 (10-50行)
            "BATCH_DELETE",      # 批量删除 (10-50行)
            "SMALL_INSERT",      # 再来一轮小操作
            "SMALL_UPDATE",
            "SMALL_DELETE",
        ]
        
        # 连接上游account
        up_config = ClusterConfig(
            host=self.config.upstream.host,
            port=self.config.upstream.port,
            user="admin",
            password="111",
            account=task.upstream_account
        )
        worker_conn = DatabaseConnection(up_config)
        try:
            worker_conn.connect()
        except Exception as e:
            logger.error(f"[{task.name}] DML worker failed to connect: {e}")
            return
        
        op_index = 0
        try:
            while not self.stop_event.is_set():
                op = dml_operations[op_index % len(dml_operations)]
                op_index += 1
                conn = worker_conn.get_connection()
                db = task.db_name
                tbl = task.table_name
                
                try:
                    if op == "SMALL_INSERT":
                        # 小数据插入 1-5行
                        count = random.randint(1, 5)
                        for _ in range(count):
                            self._insert_row(conn, db, tbl, task.added_columns)
                        logger.debug(f"[{task.name}] SMALL_INSERT: {count} rows")
                        
                    elif op == "SMALL_UPDATE":
                        # 小数据更新 1-5行
                        count = random.randint(1, 5)
                        execute_sql(conn, f"""
                            UPDATE {db}.{tbl} 
                            SET c_int = {random.randint(0, 100000)},
                                c_varchar = '{escape_sql_string(random_string(50))}',
                                status = '{random.choice(['active', 'inactive', 'pending'])}'
                            ORDER BY RAND() LIMIT {count}
                        """)
                        logger.debug(f"[{task.name}] SMALL_UPDATE: {count} rows")
                        
                    elif op == "SMALL_DELETE":
                        # 小数据删除 1-5行
                        count = random.randint(1, 5)
                        execute_sql(conn, f"""
                            DELETE FROM {db}.{tbl} 
                            ORDER BY RAND() LIMIT {count}
                        """)
                        logger.debug(f"[{task.name}] SMALL_DELETE: {count} rows")
                        
                    elif op == "BATCH_INSERT":
                        # 批量插入 50-100行
                        count = random.randint(50, 100)
                        for _ in range(count):
                            self._insert_row(conn, db, tbl, task.added_columns)
                        logger.debug(f"[{task.name}] BATCH_INSERT: {count} rows")
                        
                    elif op == "BATCH_UPDATE":
                        # 批量更新 10-50行
                        count = random.randint(10, 50)
                        execute_sql(conn, f"""
                            UPDATE {db}.{tbl} 
                            SET c_int = c_int + 1,
                                c_varchar = '{escape_sql_string(random_string(50))}',
                                status = '{random.choice(['active', 'inactive', 'pending'])}'
                            ORDER BY RAND() LIMIT {count}
                        """)
                        logger.debug(f"[{task.name}] BATCH_UPDATE: {count} rows")
                        
                    elif op == "BATCH_DELETE":
                        # 批量删除 10-50行 (但保留至少100行)
                        current_count = get_table_count(conn, db, tbl)
                        if current_count > 150:
                            count = min(random.randint(10, 50), current_count - 100)
                            execute_sql(conn, f"""
                                DELETE FROM {db}.{tbl} 
                                ORDER BY RAND() LIMIT {count}
                            """)
                            logger.debug(f"[{task.name}] BATCH_DELETE: {count} rows")
                        else:
                            # 数据太少，改为插入
                            for _ in range(50):
                                self._insert_row(conn, db, tbl, task.added_columns)
                            logger.debug(f"[{task.name}] BATCH_DELETE->INSERT: 50 rows (table too small)")
                            
                except Exception as e:
                    logger.debug(f"[{task.name}] DML error ({op}): {e}")
                
                # 不等待，直接继续下一个操作
        finally:
            worker_conn.close()
    
    def _checkpoint(self, task: CCPRTask) -> bool:
        """
        检查点流程：
        1. 查询task_id
        2. PAUSE CCPR SUBSCRIPTION 'task_id'
        3. sleep 10s
        4. 从下游mo_ccpr_log读取状态
        5. 检查watermark和error_message
        6. 用snapshot对比数据一致性
        7. RESUME CCPR SUBSCRIPTION 'task_id'
        """
        logger.info(f"[{task.name}] Starting checkpoint...")
        
        try:
            # 1. 查询task_id (用sys租户查询系统表)
            result = execute_sql(self.sys_downstream_conn.get_connection(),
                f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{task.pub_name}' AND drop_at IS NULL",
                fetch=True)
            if not result or not result[0][0]:
                logger.warning(f"[{task.name}] No task_id found for {task.pub_name}")
                return False
            task_id = result[0][0]
            logger.info(f"[{task.name}] Found task_id: {task_id}")
            
            # 2. PAUSE (使用task_id带引号，用sys租户)
            execute_sql(self.sys_downstream_conn.get_connection(),
                f"PAUSE CCPR SUBSCRIPTION '{task_id}'")
            logger.info(f"[{task.name}] Paused subscription")
            
            # 2. Sleep
            time.sleep(10)
            
            # 3. 读取状态 (用sys租户查询系统表)
            statuses = get_ccpr_status(self.sys_downstream_conn.get_connection(),
                subscription_name=task.pub_name)
            
            if statuses:
                status = statuses[0]
                logger.info(f"[{task.name}] Status: state={status.state}, "
                          f"lsn={status.iteration_lsn}, error={status.error_message}")
                
                # 4. 检查error
                if status.error_message:
                    self.errors.append(f"[{task.name}] Error: {status.error_message}")
                    logger.error(f"[{task.name}] Subscription error: {status.error_message}")
                
                # 5. 数据一致性检查（使用snapshot）
                # 构造snapshot名称
                snapshot_name = f"ccpr_{status.task_id}_{status.iteration_lsn}"
                
                # 上游用snapshot读取
                try:
                    up_count = execute_sql(task.upstream_conn.get_connection(),
                        f"SELECT COUNT(*) FROM {task.db_name}.{task.table_name} "
                        f"{{SNAPSHOT = '{snapshot_name}'}}",
                        fetch=True)
                    up_count = up_count[0][0] if up_count else -1
                except Exception as e:
                    logger.warning(f"[{task.name}] Snapshot read failed, using direct read: {e}")
                    up_count = get_table_count(task.upstream_conn.get_connection(),
                        task.db_name, task.table_name)
                
                # 下游直接读取
                down_count = get_table_count(task.downstream_conn.get_connection(),
                    task.db_name, task.table_name)
                
                if up_count != down_count:
                    msg = f"[{task.name}] Count mismatch: upstream={up_count}, downstream={down_count}"
                    logger.error(msg)
                    # 数据不一致，直接退出
                    raise RuntimeError(msg)
                else:
                    logger.info(f"[{task.name}] Data consistent: {up_count} rows")
            
            # 7. RESUME (使用task_id带引号，用sys租户)
            execute_sql(self.sys_downstream_conn.get_connection(),
                f"RESUME CCPR SUBSCRIPTION '{task_id}'")
            logger.info(f"[{task.name}] Resumed subscription")
            
            return True
            
        except Exception as e:
            logger.error(f"[{task.name}] Checkpoint failed: {e}")
            self.errors.append(str(e))
            # 尝试resume (需要重新查询task_id，用sys租户)
            try:
                result = execute_sql(self.sys_downstream_conn.get_connection(),
                    f"SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name='{task.pub_name}' AND drop_at IS NULL",
                    fetch=True)
                if result and result[0][0]:
                    execute_sql(self.sys_downstream_conn.get_connection(),
                        f"RESUME CCPR SUBSCRIPTION '{result[0][0]}'")
            except:
                pass
            return False
    
    def _execute_alter(self, task: CCPRTask, phase: LongTestPhase):
        """执行ALTER操作"""
        conn = task.upstream_conn.get_connection()
        db = task.db_name
        tbl = task.table_name
        
        try:
            if phase == LongTestPhase.PHASE1_DML_ADD_COLUMN:
                # ADD COLUMN (non-inplace)
                col_name = f"extra_col_{random_string(4)}"
                execute_sql(conn, f"ALTER TABLE {db}.{tbl} ADD COLUMN {col_name} VARCHAR(100)")
                task.added_columns.append(col_name)
                logger.info(f"[{task.name}] Added column: {col_name}")
                
            elif phase == LongTestPhase.PHASE2_DML_ADD_INDEX:
                # ADD INDEX (inplace)
                idx_name = f"idx_{random_string(6)}"
                execute_sql(conn, f"CREATE INDEX {idx_name} ON {db}.{tbl} (c_int)")
                task.added_indexes.append(idx_name)
                logger.info(f"[{task.name}] Added index: {idx_name}")
                
            elif phase == LongTestPhase.PHASE3_DML_ADD_FULLTEXT:
                # ADD FULLTEXT INDEX (inplace)
                idx_name = f"ftidx_{random_string(6)}"
                execute_sql_safe(conn, 
                    f"CREATE FULLTEXT INDEX {idx_name} ON {db}.{tbl} (c_text)",
                    ignore_errors=True)
                task.added_indexes.append(idx_name)
                logger.info(f"[{task.name}] Added fulltext index: {idx_name}")
                
            elif phase == LongTestPhase.PHASE4_DML_RENAME_COLUMN:
                # RENAME COLUMN (inplace)
                if task.added_columns:
                    old_name = task.added_columns[0]
                    new_name = f"renamed_{random_string(4)}"
                    execute_sql(conn, 
                        f"ALTER TABLE {db}.{tbl} RENAME COLUMN {old_name} TO {new_name}")
                    task.added_columns[0] = new_name
                    logger.info(f"[{task.name}] Renamed column: {old_name} -> {new_name}")
                    
            elif phase == LongTestPhase.PHASE5_DML_DROP_COLUMN:
                # DROP COLUMN (non-inplace)
                if task.added_columns:
                    col_name = task.added_columns.pop()
                    execute_sql(conn, f"ALTER TABLE {db}.{tbl} DROP COLUMN {col_name}")
                    logger.info(f"[{task.name}] Dropped column: {col_name}")
                    
            elif phase == LongTestPhase.PHASE6_DML_LOAD_COMMENT:
                # 大批量LOAD + CHANGE COMMENT
                logger.info(f"[{task.name}] Bulk loading data...")
                for _ in range(100):
                    self._insert_row(conn, db, tbl, task.added_columns)
                
                comment = f"Long test completed at {datetime.now()}"
                execute_sql(conn, f"ALTER TABLE {db}.{tbl} COMMENT = '{comment}'")
                logger.info(f"[{task.name}] Changed comment")
                
        except Exception as e:
            logger.error(f"[{task.name}] ALTER failed in {phase.name}: {e}")
            self.errors.append(str(e))
    
    def cleanup(self):
        """清理测试资源"""
        logger.info("Cleaning up...")
        
        for task in self.tasks:
            try:
                # 关闭task连接
                if task.upstream_conn:
                    task.upstream_conn.close()
                if task.downstream_conn:
                    task.downstream_conn.close()
                
                # 用sys删除publication
                if self.sys_upstream_conn:
                    execute_sql_safe(self.sys_upstream_conn.get_connection(),
                        f"DROP PUBLICATION IF EXISTS {task.pub_name}", ignore_errors=True)
                
                # 删除上游account（会自动删除其下的database）
                if self.sys_upstream_conn and task.upstream_account != "sys":
                    drop_account(self.sys_upstream_conn.get_connection(), task.upstream_account)
                # 上游也创建了down_account用于授权，也要删除
                if self.sys_upstream_conn and task.downstream_account != "sys":
                    drop_account(self.sys_upstream_conn.get_connection(), task.downstream_account)
                
                # 删除下游account
                if self.sys_downstream_conn and task.downstream_account != "sys":
                    drop_account(self.sys_downstream_conn.get_connection(), task.downstream_account)
                    
            except Exception as e:
                logger.warning(f"Cleanup error for {task.name}: {e}")
        
        # 关闭sys连接
        if self.sys_upstream_conn:
            self.sys_upstream_conn.close()
        if self.sys_downstream_conn:
            self.sys_downstream_conn.close()
        
        logger.info("Cleanup complete")
    
    def get_report(self) -> Dict[str, Any]:
        """获取测试报告"""
        return {
            "test_type": "long_test",
            "tasks": [
                {
                    "level": task.level.value,
                    "name": task.name,
                    "upstream_account": task.upstream_account,
                    "downstream_account": task.downstream_account,
                    "db_name": task.db_name,
                    "table_name": task.table_name,
                }
                for task in self.tasks
            ],
            "errors": self.errors,
            "success": len(self.errors) == 0,
        }


# =============================================================================
# 主函数
# =============================================================================

def setup_logging(log_file: str, log_level: str):
    """配置日志"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pymysql').setLevel(logging.WARNING)


def run_long_test(config: TestConfig, duration: int, no_cleanup: bool = False) -> Dict[str, Any]:
    """运行长时间测试"""
    test = CCPRLongTest(config)
    
    try:
        if not test.setup():
            return {"success": False, "error": "Setup failed", "errors": test.errors}
        
        success = test.run(duration)
        report = test.get_report()
        report["duration_seconds"] = duration
        report["success"] = success and len(test.errors) == 0
        
        return report
    finally:
        if not no_cleanup:
            test.cleanup()
        else:
            logger.info("Skipping cleanup (--no-cleanup specified)")


def main():
    parser = argparse.ArgumentParser(
        description="CCPR Integration Test Runner (按TEST_PLAN.md实现)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 运行长时间测试 (8小时)
  python main.py --long-test
  
  # 运行长时间测试 (自定义时长, 如5分钟)
  python main.py --long-test --duration 300
  
  # 运行短测试
  python main.py --quick-test
  
  # 自定义集群端口
  python main.py --long-test --upstream-port 6001 --downstream-port 6002
        """
    )
    
    # 测试模式
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--long-test", action="store_true",
                           help="运行长时间测试 (默认8小时)")
    mode_group.add_argument("--quick-test", action="store_true",
                           help="运行短测试 (调用quick_test.py)")
    
    # 时长
    parser.add_argument("--duration", type=int, default=8*60*60,
                       help="测试时长(秒), 默认8小时=28800秒")
    
    # 集群配置
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, default=6001)
    parser.add_argument("--downstream-host", default="127.0.0.1")
    parser.add_argument("--downstream-port", type=int, default=6002)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="111")
    
    # 测试配置
    parser.add_argument("--sync-interval", type=int, default=10,
                       help="CCPR同步间隔(秒)")
    parser.add_argument("--insert-interval", type=float, default=2.0,
                       help="DML操作间隔(秒)")
    
    # 日志
    parser.add_argument("--log-file", default="ccpr_test.log")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--report-file", default="ccpr_report.json")
    
    # 清理控制
    parser.add_argument("--no-cleanup", action="store_true",
                       help="测试结束后不清理环境（保留账户和数据库）")
    
    args = parser.parse_args()
    
    # 构建配置
    config = TestConfig()
    config.upstream = ClusterConfig(
        host=args.upstream_host,
        port=args.upstream_port,
        user=args.user,
        password=args.password
    )
    config.downstream = ClusterConfig(
        host=args.downstream_host,
        port=args.downstream_port,
        user=args.user,
        password=args.password
    )
    config.sync_interval = args.sync_interval
    config.insert_interval = args.insert_interval
    
    # 配置日志
    setup_logging(args.log_file, args.log_level)
    
    logger.info("="*60)
    logger.info("CCPR Integration Test")
    logger.info("="*60)
    logger.info(f"Upstream: {config.upstream.host}:{config.upstream.port}")
    logger.info(f"Downstream: {config.downstream.host}:{config.downstream.port}")
    
    # 运行测试
    if args.quick_test:
        # 调用quick_test.py
        logger.info("Running quick test...")
        import quick_test
        quick_test.main()
        return
    
    # 默认运行长时间测试
    logger.info(f"Running long test for {args.duration} seconds ({args.duration/3600:.1f} hours)")
    if args.no_cleanup:
        logger.info("Cleanup disabled - environment will be preserved after test")
    results = run_long_test(config, args.duration, no_cleanup=args.no_cleanup)
    
    # 保存报告
    with open(args.report_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Report saved to {args.report_file}")
    
    # 打印摘要
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    logger.info(f"Success: {results.get('success', False)}")
    logger.info(f"Errors: {len(results.get('errors', []))}")
    
    if results.get('errors'):
        logger.error("Errors encountered:")
        for err in results['errors'][:10]:
            logger.error(f"  - {err}")
    
    sys.exit(0 if results.get('success') else 1)


if __name__ == "__main__":
    main()
